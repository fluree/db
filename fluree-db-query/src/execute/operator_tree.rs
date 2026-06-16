//! Operator tree building
//!
//! Builds the complete operator tree for a query including:
//! WHERE patterns → GROUP BY → Aggregates → HAVING → ORDER BY → PROJECT → DISTINCT → OFFSET → LIMIT

use crate::aggregate::AggregateOperator;
use crate::binary_scan::EmitMask;
use crate::count_rows::CountRowsOperator;
use crate::distinct::DistinctOperator;
use crate::error::{QueryError, Result};
use crate::eval::PreparedBoolExpression;
use crate::fast_count::{
    count_blank_node_subjects_operator, count_distinct_position_operator,
    count_literal_objects_operator, count_rows_lang_filter_operator,
    count_rows_numeric_compare_operator, count_rows_operator, count_triples_operator,
    sum_compare_as_count_operator, DistinctPosition, NumericCompareOp,
};
use crate::fast_exists_join_count_distinct_object::exists_join_count_distinct_object_operator;
use crate::fast_group_count_firsts::{
    GroupByObjectStarTopKOperator, PredicateGroupCountFirstsOperator,
    PredicateObjectCountFirstsOperator,
};
use crate::fast_label_regex_type::label_regex_type_operator;
use crate::fast_min_max_string::{predicate_min_max_string_operator, MinMaxMode};
use crate::fast_path_plus_count_all::{
    property_path_plus_count_all_operator, transitive_path_plus_count_all_operator,
};
use crate::fast_post_order_limit::post_order_desc_limit_operator;
use crate::fast_predicate_scalar_agg::{
    predicate_scalar_agg_operator, DateComponentFn, NumericUnaryFn, ScalarAggKind, SumExprI64,
};
use crate::fast_star_const_order_topk::star_const_ordered_limit_operator;
use crate::fast_string_prefix_count_all::{
    string_prefix_count_all_operator, string_prefix_sum_strstarts_operator,
};
use crate::fast_sum_strlen_group_concat::sum_strlen_group_concat_operator;
use crate::fast_union_star_count_all::{UnionCountMode, UnionStarCountAllOperator};
use crate::group_aggregate::{GroupAggregateOperator, StreamingAggSpec};
use crate::groupby::GroupByOperator;
use crate::having::HavingOperator;
use crate::ir::triple::{Ref, Term, TriplePattern};
use crate::ir::{
    AggregateFn, AggregateSpec, Aggregation, Expression, Grouping, InputSemantics, PathModifier,
    Pattern, Query, QueryOutput,
};
use crate::limit::LimitOperator;
use crate::offset::OffsetOperator;
use crate::operator::inline::InlineOperator;
use crate::operator::BoxedOperator;
use crate::project::ProjectOperator;
use crate::sort::SortDirection;
use crate::sort::SortOperator;
use crate::sort::SortSpec;
use crate::stats_query::stats_count_by_predicate_operator;
use crate::temporal_mode::PlanningContext;
use crate::var_registry::VarId;
use fluree_db_core::StatsView;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use super::dependency::{compute_variable_deps, VariableDeps};
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

/// Detected shape for the reverse-POST `ORDER BY DESC(?o) LIMIT k` fast path.
struct PostOrderDescLimitSpec {
    /// SELECT vars, in projection order (subset of `{subject_var, object_var}`).
    projected: Vec<VarId>,
    subject_var: VarId,
    object_var: VarId,
    /// Predicate whose object is the ORDER BY key.
    anchor_pred: Ref,
    /// Optional `?s rdf:type <Class>` constraint object.
    class_term: Option<Term>,
    distinct: bool,
    limit: usize,
    offset: usize,
}

/// Detect `SELECT ?s ?o WHERE { ?s <p> ?o [ ; ?s a <Class> ] } ORDER BY DESC(?o) LIMIT k`.
///
/// Shape-only (no stats): the object-datatype order-preservation proof and the
/// profitability budget are enforced at runtime by
/// [`post_order_desc_limit_operator`], which bails to the fallback tree on any
/// uncertainty. v1 is DESC-only (the ASC complement is served today by
/// [`detect_star_const_numeric_label_order_limit`] only for its narrow shape).
fn detect_post_order_desc_limit(
    query: &Query,
    stats: Option<&StatsView>,
) -> Option<PostOrderDescLimitSpec> {
    if query.grouping.is_some() || query.post_values.is_some() {
        return None;
    }
    let limit = query.limit?;
    if limit == 0 {
        return None;
    }
    let offset = query.offset.unwrap_or(0);
    if query.ordering.len() != 1 {
        return None;
    }
    let ob = &query.ordering[0];
    if ob.direction != SortDirection::Descending {
        return None;
    }
    let object_var = ob.var;

    // WHERE must be only triple patterns: one anchor `?s <p> ?o`, optionally one
    // `?s rdf:type <Class>`.
    let mut anchor: Option<(VarId, Ref)> = None;
    let mut class_term: Option<Term> = None;
    let mut subject_var: Option<VarId> = None;
    let mut triple_count = 0usize;
    for p in &query.patterns {
        let Pattern::Triple(tp) = p else {
            return None;
        };
        triple_count += 1;
        if triple_count > 2 {
            return None;
        }
        let Ref::Var(sv) = &tp.s else {
            return None;
        };
        match subject_var {
            None => subject_var = Some(*sv),
            Some(x) if x != *sv => return None,
            _ => {}
        }
        if tp.dtc.is_some() {
            return None;
        }
        if matches!(&tp.o, Term::Var(v) if *v == object_var) {
            if anchor.is_some() || !tp.p_bound() {
                return None;
            }
            anchor = Some((*sv, tp.p.clone()));
        } else if tp.p.is_rdf_type() {
            if class_term.is_some() {
                return None;
            }
            match &tp.o {
                Term::Sid(_) | Term::Iri(_) => class_term = Some(tp.o.clone()),
                _ => return None,
            }
        } else {
            return None;
        }
    }

    let (anchor_subject, anchor_pred) = anchor?;
    let subject_var = subject_var?;
    if anchor_subject != subject_var || subject_var == object_var {
        return None;
    }

    // Projection must be a non-empty subset of {subject_var, object_var}.
    let projected = query.output.projected_vars()?;
    if projected.is_empty()
        || projected
            .iter()
            .any(|v| *v != subject_var && *v != object_var)
    {
        return None;
    }

    let distinct = query.output.is_distinct();
    // DISTINCT with the order var projected away (`SELECT DISTINCT ?s ORDER BY
    // DESC(?o)`) has subtle projection/distinct semantics — defer to fallback.
    if distinct && !projected.contains(&object_var) {
        return None;
    }

    // Profitability gate for the `?s a <Class>` shape. The reverse-tail scan
    // walks the ordering predicate newest-first, testing each candidate's class,
    // until it has `offset + limit` survivors — so a *selective* class makes it
    // walk far more rows than the generic plan would by anchoring on the class
    // directly. When stats say so, defer to the generic plan. The bare shape
    // (no class) always wins, and missing stats fall back to the operator's
    // runtime scan budget. (Patterns beyond `?s <p> ?o [; ?s a <Class>]` were
    // already rejected above, so no other selective clause reaches here.)
    if let (Some(class), Some(stats)) = (&class_term, stats) {
        if !post_order_class_is_profitable(stats, &anchor_pred, class, offset + limit) {
            return None;
        }
    }

    Some(PostOrderDescLimitSpec {
        projected,
        subject_var,
        object_var,
        anchor_pred,
        class_term,
        distinct,
        limit,
        offset,
    })
}

/// Heuristic: is the reverse-POST tail scan cheaper than the generic plan's
/// option of anchoring on `<Class>`?
///
/// Estimates the rows the tail scan must inspect to collect `need` class
/// members — `need / survivor_fraction`, where `survivor_fraction` ≈
/// `class_count / ndv_subjects(anchor)` — and compares it to `class_count` (the
/// generic class-anchor scan). Returns `true` (profitable) when the tail scan is
/// no larger, or when stats are missing for either side (the operator's runtime
/// budget is then the backstop). A class with no recorded members is not
/// profitable (the tail scan would never fill `need`).
fn post_order_class_is_profitable(
    stats: &StatsView,
    anchor_pred: &Ref,
    class: &Term,
    need: usize,
) -> bool {
    let prop = match anchor_pred {
        Ref::Sid(s) => stats.get_property(s),
        Ref::Iri(i) => stats.get_property_by_iri(i),
        Ref::Var(_) => None,
    };
    let class_count = match class {
        Term::Sid(s) => stats.get_class_count(s),
        Term::Iri(i) => stats.get_class_count_by_iri(i),
        _ => None,
    };
    // Missing stats on either side → can't judge; let it run (runtime budget guards).
    let (Some(prop), Some(class_count)) = (prop, class_count) else {
        return true;
    };
    if class_count == 0 {
        return false; // no members ⇒ tail scan can never fill `need`
    }
    let ndv_subjects = prop.ndv_subjects.max(1) as f64;
    let survivor_fraction = (class_count as f64 / ndv_subjects).clamp(f64::MIN_POSITIVE, 1.0);
    let expected_inspect = need as f64 / survivor_fraction;
    // Tail scan inspects ~expected_inspect rows; the generic plan can anchor on
    // the class (~class_count rows). Prefer the tail scan only when it is the
    // smaller scan.
    expected_inspect <= class_count as f64
}

/// Detect a common benchmark shape:
/// - Same-subject star constraints with **constant IRI-ref objects**: `?s <p> <o>`
/// - One numeric predicate with `FILTER(?v > K)` used only as an existence constraint
/// - One label predicate whose object var is ORDER BY key
/// - SELECT DISTINCT of exactly `(?s, ?label)` plus `ORDER BY ?label LIMIT k`
fn detect_star_const_numeric_label_order_limit(query: &Query) -> Option<StarConstOrderTopKSpec> {
    if !query.output.is_distinct() || query.offset.is_some() || query.grouping.is_some() {
        return None;
    }
    let limit = query.limit?;
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
fn detect_label_regex_type(query: &Query) -> Option<LabelRegexTypeSpec> {
    if query.output.is_distinct()
        || query.limit.is_some()
        || query.offset.is_some()
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
fn implicit_single_aggregate(query: &Query) -> Option<&AggregateSpec> {
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
        || query.offset.is_some()
        || query.output.is_distinct()
        || query.limit == Some(0)
    {
        return None;
    }
    Some(aggregates.first())
}

/// Validate that a query has a single `COUNT(*)` aggregate with standard constraints.
///
/// Returns `Some(output_var)` if the query has:
/// - SELECT output (not CONSTRUCT/BOOLEAN/WILDCARD)
/// - Exactly one aggregate: `COUNT(*)` (`AggregateFn::CountAll`)
/// - No group_by, having, post-aggregation binds, order_by, offset, or DISTINCT
/// - LIMIT >= 1 (or no limit)
/// - SELECT vars == `[agg.output_var]`
pub(crate) fn detect_count_all_aggregate(query: &Query) -> Option<VarId> {
    let agg = implicit_single_aggregate(query)?;
    if !matches!(agg.function, AggregateFn::CountAll) {
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
fn detect_count_distinct_aggregate(query: &Query) -> Option<(VarId, VarId)> {
    let agg = implicit_single_aggregate(query)?;
    let AggregateFn::CountDistinct(in_var) = agg.function else {
        return None;
    };
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
fn detect_count_aggregate(query: &Query) -> Option<(Option<VarId>, VarId)> {
    let agg = implicit_single_aggregate(query)?;
    let input_var = match agg.function {
        AggregateFn::CountAll => None,
        AggregateFn::Count(v) => Some(v),
        _ => return None,
    };
    let select_vars = query.output.projected_vars()?;
    if select_vars.len() != 1 || select_vars[0] != agg.output_var {
        return None;
    }
    Some((input_var, agg.output_var))
}

fn detect_partitioned_group_by(query: &Query) -> bool {
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
    match &agg.function {
        AggregateFn::CountAll => {}
        AggregateFn::Count(v) if *v == s_var => {}
        _ => return None,
    }
    if !binds.is_empty() {
        return None;
    }
    // ORDER BY DESC(?count) and LIMIT k required so we can do top-k directly.
    let limit = query.limit?;
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
    if query.offset.is_some() {
        return None;
    }
    let limit = query.limit?;
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
        match agg.function {
            AggregateFn::CountAll => {
                if count_out.is_some() {
                    return None;
                }
                count_out = Some(agg.output_var);
            }
            AggregateFn::Count(v) if v == subj_var => {
                if count_out.is_some() {
                    return None;
                }
                count_out = Some(agg.output_var);
            }
            AggregateFn::Min(v) if v == subj_var => {
                if min_out.is_some() {
                    return None;
                }
                min_out = Some(agg.output_var);
            }
            AggregateFn::Max(v) if v == subj_var => {
                if max_out.is_some() {
                    return None;
                }
                max_out = Some(agg.output_var);
            }
            AggregateFn::Sample(v) if v == subj_var => {
                if sample_out.is_some() {
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

fn detect_sum_strlen_group_concat_subquery(query: &Query) -> Option<(Ref, Arc<str>, VarId)> {
    use crate::ir::{Expression, Function, Pattern};

    // Outer aggregate must be SUM(?v) (where ?v is the STRLEN bind var).
    let outer_agg = implicit_single_aggregate(query)?;
    let strlen_var = match outer_agg.function {
        AggregateFn::Sum(input, InputSemantics::List) => input,
        _ => return None,
    };

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
    // The fused operator sums STRLEN(GROUP_CONCAT(..)) over EVERY group directly
    // and ignores the inner subquery's slice / distinct / ordering modifiers.
    // Only fire for the exact unmodified inner shape; otherwise an inner
    // DISTINCT (collapsing duplicate concatenations) or LIMIT/OFFSET (summing a
    // subset of groups) would change the answer. Falling back to the generic
    // pipeline keeps those modifiers honored. ORDER BY alone is sum-invariant,
    // but is declined too so the matched shape stays exact.
    if sq.distinct
        || sq.limit.is_some()
        || sq.offset.is_some()
        || !sq.ordering.is_empty()
        || !sq.order_binds.is_empty()
    {
        return None;
    }
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
    // No post-aggregation binds and no HAVING — those would change the
    // shape that the outer SUM(STRLEN(?cat)) fast-path is keyed against.
    let Some(Grouping::Explicit {
        group_by: sq_group_by,
        aggregation:
            Some(Aggregation {
                aggregates: sq_aggregates,
                binds: sq_binds,
            }),
        having: None,
    }) = &sq.grouping
    else {
        return None;
    };
    if !sq_binds.is_empty() || sq_group_by.len() != 1 || sq_aggregates.len() != 1 {
        return None;
    }
    let inner_agg = sq_aggregates.first();
    let (sep, input_var) = match &inner_agg.function {
        AggregateFn::GroupConcat {
            separator,
            input,
            semantics: InputSemantics::List,
        } => (separator.as_str(), *input),
        _ => return None,
    };
    if inner_agg.output_var != *cat_var {
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
) -> Option<(Ref, VarId, crate::ir::triple::Term, VarId)> {
    // Expression ORDER BY needs the generic pipeline's order-bind stage; this
    // fast path sorts on `query.ordering` directly, so decline it.
    if !query.order_binds.is_empty() {
        return None;
    }
    let (input_var, out_var) = detect_count_aggregate(query)?;

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

fn detect_predicate_count_rows(query: &Query) -> Option<(Ref, VarId)> {
    let (input_var, out_var) = detect_count_aggregate(query)?;

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

/// Detect `COUNT(*)` / `COUNT(?s|?o)` of `?s rdf:type <Class> . ?s P ?o` — one leg
/// with a constant (class) object, the other a same-subject property with a variable
/// object. Returns `(type_pred, class_obj, property, out_var)`; the operator verifies
/// the constant-object leg is rdf:type and does the class-stat lookup. Exactly one
/// constant-object leg and one variable-object leg are required, so the variable-class
/// star (`?s rdf:type ?o1 . ?s P ?o2`) and pure-constant joins don't match.
/// `COUNT(DISTINCT …)` is rejected by `detect_count_aggregate`.
fn detect_class_property_count(query: &Query) -> Option<(Ref, Ref, Ref, VarId)> {
    let (input_var, out_var) = detect_count_aggregate(query)?;
    if query.patterns.len() != 2 {
        return None;
    }
    let (Pattern::Triple(t0), Pattern::Triple(t1)) = (&query.patterns[0], &query.patterns[1])
    else {
        return None;
    };
    for (cls_tp, prop_tp) in [(t0, t1), (t1, t0)] {
        // Class leg: ?s <type_pred> <ConstClass> (constant ref object).
        let Ref::Var(cs) = &cls_tp.s else { continue };
        if cls_tp.dtc.is_some() {
            continue;
        }
        let Some(type_pred) = extract_bound_predicate(&cls_tp.p) else {
            continue;
        };
        let class_obj = match &cls_tp.o {
            Term::Iri(i) => Ref::Iri(i.clone()),
            Term::Sid(s) => Ref::Sid(s.clone()),
            _ => continue, // not a constant class object
        };
        // Property leg: same subject, bound predicate, variable object.
        let Ref::Var(ps) = &prop_tp.s else { continue };
        if cs != ps || prop_tp.dtc.is_some() {
            continue;
        }
        let Some(property) = extract_bound_predicate(&prop_tp.p) else {
            continue;
        };
        let Term::Var(po) = &prop_tp.o else { continue };
        // COUNT(?v): v must be the subject or the property's object (both bound).
        if let Some(v) = input_var {
            if v != *cs && v != *po {
                continue;
            }
        }
        return Some((type_pred, class_obj, property, out_var));
    }
    None
}

fn detect_predicate_count_rows_lang_filter(query: &Query) -> Option<(Ref, String, VarId)> {
    let (input_var, out_var) = detect_count_aggregate(query)?;

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

fn detect_predicate_count_distinct_object(query: &Query) -> Option<(Ref, VarId)> {
    let (in_var, out_var) = detect_count_distinct_aggregate(query)?;

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

fn detect_predicate_minmax_string(query: &Query) -> Option<(Ref, MinMaxMode, VarId)> {
    // Must be a single implicit aggregate with no grouping/having/binds/etc.
    let agg = implicit_single_aggregate(query)?;
    // WHERE must be a single triple.
    if query.patterns.len() != 1 {
        return None;
    }
    let Pattern::Triple(tp) = &query.patterns[0] else {
        return None;
    };
    let (_s_var, pred, o_var) = validate_simple_triple(tp)?;

    // Aggregate must be MIN(?o) or MAX(?o).
    let mode = match agg.function {
        AggregateFn::Min(v) if v == o_var => MinMaxMode::Min,
        AggregateFn::Max(v) if v == o_var => MinMaxMode::Max,
        _ => return None,
    };

    // SELECT must be exactly the aggregate output var.
    let select_vars = query.output.projected_vars()?;
    if select_vars.len() != 1 || select_vars[0] != agg.output_var {
        return None;
    }

    Some((pred, mode, agg.output_var))
}

fn detect_predicate_avg_numeric(query: &Query) -> Option<(Ref, VarId)> {
    let agg = implicit_single_aggregate(query)?;
    if query.patterns.len() != 1 {
        return None;
    }
    let Pattern::Triple(tp) = &query.patterns[0] else {
        return None;
    };
    let (_s_var, pred, o_var) = validate_simple_triple(tp)?;
    let AggregateFn::Avg(input, InputSemantics::List) = agg.function else {
        return None;
    };
    if input != o_var {
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
) -> Option<(
    crate::ir::triple::TriplePattern,
    Vec<crate::ir::Expression>,
    VarId,
)> {
    // Must be single COUNT aggregate, no grouping/having/binds/etc.
    let agg = implicit_single_aggregate(query)?;
    if !matches!(agg.function, AggregateFn::Count(_) | AggregateFn::CountAll) {
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
    if matches!(agg.function, AggregateFn::Count(v) if v != s_var) {
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
) -> Option<(Ref, NumericCompareOp, fluree_db_core::FlakeValue, VarId)> {
    let agg = implicit_single_aggregate(query)?;
    if query.patterns.len() != 2 {
        return None;
    }
    if !matches!(agg.function, AggregateFn::Count(_) | AggregateFn::CountAll) {
        return None;
    }

    let (tp, filter) = match (&query.patterns[0], &query.patterns[1]) {
        (Pattern::Triple(tp), Pattern::Filter(expr)) => (tp, expr),
        (Pattern::Filter(expr), Pattern::Triple(tp)) => (tp, expr),
        _ => return None,
    };

    let (s_var, pred, o_var) = validate_simple_triple(tp)?;
    if matches!(agg.function, AggregateFn::Count(v) if v != s_var) {
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

fn detect_string_prefix_count_all(query: &Query) -> Option<(Ref, Arc<str>, VarId)> {
    let out_var = detect_count_all_aggregate(query)?;
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

fn detect_string_prefix_sum_strstarts(query: &Query) -> Option<(Ref, Arc<str>, VarId)> {
    use crate::ir::{Expression, FlakeValue, Function};

    let agg = implicit_single_aggregate(query)?;
    let AggregateFn::Sum(sum_input, InputSemantics::List) = agg.function else {
        return None;
    };
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
    if sum_input != bind_var {
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
fn detect_stats_count_by_predicate(query: &Query) -> Option<(VarId, VarId)> {
    // Expression ORDER BY needs the generic pipeline's order-bind stage; this
    // fast path sorts on `query.ordering` directly (a synthetic, unbound sort
    // var would be silently dropped), so decline it.
    if !query.order_binds.is_empty() {
        return None;
    }
    // OFFSET would be applied twice when the operator defers to its planned
    // fallback at open() time (the fallback tree already applies the full
    // modifier stack; the dispatch wraps modifiers around this operator
    // again, and OFFSET — unlike sort/project/distinct/limit — is not
    // idempotent). Decline and let the generic pipeline handle it.
    if query.offset.is_some() {
        return None;
    }
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
    let AggregateFn::Count(input_var) = agg.function else {
        return None;
    };
    // COUNT input must be a non-predicate variable (subject or object)
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

fn detect_fused_scan_sum_i64(query: &Query) -> Option<(Ref, SumExprI64, VarId)> {
    // Must be single aggregate, no grouping/having/binds/etc.
    let agg = implicit_single_aggregate(query)?;

    // SELECT must be exactly the aggregate output var.
    let select_vars = query.output.projected_vars()?;
    if select_vars.len() != 1 || select_vars[0] != agg.output_var {
        return None;
    }
    let AggregateFn::Sum(sum_input, InputSemantics::List) = agg.function else {
        return None;
    };

    match query.patterns.as_slice() {
        [Pattern::Triple(tp)] => {
            let pred = extract_bound_predicate(&tp.p)?;
            let Term::Var(o_var) = &tp.o else {
                return None;
            };
            if sum_input != *o_var {
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
            if sum_input != *var {
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

/// Detect `SELECT (SUM(?o <cmp> K) AS ?sum) WHERE { ?s <p> ?o }`, lowered as
/// `Triple + Bind(Gt/Ge/Lt/Le(?o, const)) + SUM(synthetic_var, List)`.
///
/// `SUM` of a boolean comparison is `COUNT` of the matching rows, which the
/// directory-skipping numeric-compare count answers far faster than the general
/// decode+eval+materialize aggregate pipeline. Restricted to `InputSemantics::List`
/// (non-DISTINCT) because `SUM(DISTINCT ?o>0)` sums the distinct set {0,1}, not a
/// row count. Empty-input (`SUM`=Unbound vs `COUNT`=0) is handled by the operator,
/// which defers to the fallback when the predicate feeds no rows.
fn detect_sum_numeric_compare_as_count(
    query: &Query,
) -> Option<(Ref, NumericCompareOp, fluree_db_core::FlakeValue, VarId)> {
    let agg = implicit_single_aggregate(query)?;
    let select_vars = query.output.projected_vars()?;
    if select_vars.len() != 1 || select_vars[0] != agg.output_var {
        return None;
    }
    let AggregateFn::Sum(sum_input, InputSemantics::List) = agg.function else {
        return None;
    };
    let [Pattern::Triple(tp), Pattern::Bind { var, expr }] = query.patterns.as_slice() else {
        return None;
    };
    if sum_input != *var {
        return None;
    }
    let pred = extract_bound_predicate(&tp.p)?;
    let Term::Var(o_var) = &tp.o else {
        return None;
    };
    let (cmp_var, op, threshold) = extract_simple_numeric_compare_threshold(expr)?;
    if cmp_var != *o_var {
        return None;
    }
    Some((pred, op, threshold, agg.output_var))
}

fn detect_exists_join_count_distinct_object(query: &Query) -> Option<(Ref, Ref, VarId)> {
    let (in_var, out_var) = detect_count_distinct_aggregate(query)?;

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

fn detect_count_blank_node_subjects(query: &Query) -> Option<VarId> {
    let (input_var, out_var) = detect_count_aggregate(query)?;

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

fn detect_count_literal_objects(query: &Query) -> Option<VarId> {
    let (input_var, out_var) = detect_count_aggregate(query)?;

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

/// Detect `SELECT (COUNT(DISTINCT ?v) AS ?c) WHERE { ?s ?p ?o }` and resolve
/// which triple position `?v` binds. All three positions must be variables (the
/// fast paths read whole-permutation metadata), matching the prior three
/// separate detectors exactly. Priority on positional ambiguity (e.g. `?x ?p ?x`)
/// is subjects → predicates → objects, preserving the old dispatch order.
fn detect_count_distinct_position(query: &Query) -> Option<(DistinctPosition, VarId)> {
    let (in_var, out_var) = detect_count_distinct_aggregate(query)?;

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

    let position = if in_var == *sv {
        DistinctPosition::Subjects
    } else if in_var == *pv {
        DistinctPosition::Predicates
    } else if in_var == *ov {
        DistinctPosition::Objects
    } else {
        return None;
    };

    Some((position, out_var))
}

fn detect_count_triples(query: &Query) -> Option<VarId> {
    let (input_var, out_var) = detect_count_aggregate(query)?;

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

fn detect_transitive_path_plus_count_all(query: &Query) -> Option<(Ref, Ref, VarId)> {
    let out_var = detect_count_all_aggregate(query)?;
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
) -> Option<(fluree_db_core::Sid, Ref, VarId)> {
    let out_var = detect_count_all_aggregate(query)?;
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
) -> Option<(Vec<Ref>, Vec<Ref>, UnionCountMode, VarId)> {
    use crate::ir::{Expression, Function};
    let out_var = detect_count_all_aggregate(query)?;

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

/// Global fast-path kill switch.
///
/// When set (programmatically via [`set_fast_paths_disabled`] or via the
/// `FLUREE_DISABLE_QUERY_FAST_PATHS` env var), the planner skips every
/// `detect_*` shape recognizer — the fused chain *and* the history-gated
/// non-fused paths — and always builds the generic operator pipeline.
///
/// This exists for the differential correctness harness
/// (`fluree-db-api/tests/it_differential_fastpath.rs`), which runs the same
/// query with fast paths on and off and asserts identical results, and as
/// an operational escape hatch when triaging a suspected fast-path bug.
/// It is NOT a tuning knob: runtime operator-internal optimizations
/// (cursor selection, batched joins) are unaffected.
static FAST_PATHS_DISABLED: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

/// Disable (or re-enable) planner fast paths process-wide. Test/triage use.
pub fn set_fast_paths_disabled(disabled: bool) {
    FAST_PATHS_DISABLED.store(disabled, std::sync::atomic::Ordering::Relaxed);
}

/// True when planner fast paths are disabled, either programmatically or
/// via `FLUREE_DISABLE_QUERY_FAST_PATHS` (read once per process).
pub fn fast_paths_disabled() -> bool {
    static ENV: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    FAST_PATHS_DISABLED.load(std::sync::atomic::Ordering::Relaxed)
        || *ENV.get_or_init(|| std::env::var_os("FLUREE_DISABLE_QUERY_FAST_PATHS").is_some())
}

/// Build the complete operator tree for a query
///
/// Constructs operators in the order:
/// WHERE patterns → GROUP BY → Aggregates → HAVING → ORDER BY → PROJECT → DISTINCT → OFFSET → LIMIT
pub fn build_operator_tree(
    query: &Query,
    stats: Option<Arc<StatsView>>,
    planning: &PlanningContext,
) -> Result<BoxedOperator> {
    // Fold `FILTER(?x = ?y)` equijoins into variable unification before planning,
    // so the rewrite feeds the stats-driven reorder, count planner, and index
    // fast paths (rather than running as a cross-product + filter). Only clone
    // the IR when there is actually something to fold. Recurses into subqueries.
    if stats.is_some() && crate::filter_fold::has_equijoin_filter(query) {
        let mut folded = query.clone();
        crate::filter_fold::fold_equijoin_filters(&mut folded, stats.as_deref());
        return build_operator_tree_inner(&folded, stats, true, planning);
    }
    // Rewrite `avg` over an anti-join complement into a difference of aggregates
    // (universe total minus the per-key WITH aggregate), eliminating the
    // feature x product cross-product. Clone only when a candidate is present.
    if crate::aggregate_complement_fold::has_aggregate_complement_candidate(query) {
        let mut rewritten = query.clone();
        crate::aggregate_complement_fold::fold_aggregate_complements(&mut rewritten);
        return build_operator_tree_inner(&rewritten, stats, true, planning);
    }
    build_operator_tree_inner(query, stats, true, planning)
}

fn build_operator_tree_inner(
    query: &Query,
    stats: Option<Arc<StatsView>>,
    enable_fused_fast_paths: bool,
    planning: &PlanningContext,
) -> Result<BoxedOperator> {
    // Global kill switch (differential harness / triage): force the generic
    // pipeline for the fused chain and the non-fused history-gated paths
    // below alike.
    let fast_paths_globally_disabled = fast_paths_disabled();
    let enable_fused_fast_paths = enable_fused_fast_paths && !fast_paths_globally_disabled;

    // Phase 5 of the planner-mode refactor: fast paths emit current-state
    // bindings (no `op` channel, no retract events) and don't consult the
    // history sidecar. In `History` mode they're semantically wrong, so the
    // planner declines to construct them at all — this collapses the
    // optimistic-then-fallback pattern in each operator's `open()` into a
    // single planner-time decision.
    let enable_fused_fast_paths = enable_fused_fast_paths && !planning.is_history();

    // Expression-based ORDER BY (`query.order_binds`) is materialized only by the
    // generic pipeline's dedicated post-grouping bind stage. No fast path runs
    // that stage, so any fast path that returns early would sort on a synthetic
    // ORDER BY variable that was never bound (silently dropping the sort). Force
    // the generic path whenever order binds are present. (The history-gated
    // count fast paths below are guarded inside their detectors for the same
    // reason.)
    let enable_fused_fast_paths = enable_fused_fast_paths && query.order_binds.is_empty();

    if enable_fused_fast_paths {
        tracing::debug!(
            patterns = ?query.patterns,
            grouping = ?query.grouping,
            "operator_tree: considering fused fast paths"
        );
    }

    // Fast-path: `SELECT (SUM(?o <cmp> K) AS ?sum) WHERE { ?s <p> ?o }`.
    //
    // SUM of a boolean comparison == COUNT of matching rows, answered by the
    // directory-skipping numeric-compare count instead of the general
    // decode+eval+materialize aggregate pipeline. Placed before the generic
    // SUM-i64 fast path (which declines the comparison shape).
    if enable_fused_fast_paths {
        if let Some((pred, compare, threshold, out_var)) =
            detect_sum_numeric_compare_as_count(query)
        {
            let fallback = build_operator_tree_inner(query, stats.clone(), false, planning)?;
            return Ok(Box::new(sum_compare_as_count_operator(
                pred,
                compare,
                threshold,
                out_var,
                Some(fallback),
            )));
        }
    }

    // Fast-path: `SELECT (SUM(DAY(?o)) AS ?sum) WHERE { ?s <p> ?o }` and friends.
    //
    // These are lowered as: Triple + Bind(expr) + SUM(synthetic_var).
    // This operator scans the predicate's POST range and aggregates directly from encoded values.
    if enable_fused_fast_paths {
        if let Some((pred, scalar, out_var)) = detect_fused_scan_sum_i64(query) {
            // Build fallback operator tree without this fast path to preserve correctness in
            // pre-index / history / policy contexts.
            let fallback = build_operator_tree_inner(query, stats.clone(), false, planning)?;
            return Ok(Box::new(predicate_scalar_agg_operator(
                ScalarAggKind::Sum(scalar),
                pred,
                out_var,
                Some(fallback),
            )));
        }
    }

    // Fast-path: `SELECT (AVG(?o) AS ?avg) WHERE { ?s <p> ?o }`
    // for homogeneous numeric predicates, scanning only POST `o_key` values.
    if enable_fused_fast_paths {
        if let Some((pred, out_var)) = detect_predicate_avg_numeric(query) {
            let fallback = build_operator_tree_inner(query, stats.clone(), false, planning)?;
            return Ok(Box::new(predicate_scalar_agg_operator(
                ScalarAggKind::AvgNumeric,
                pred,
                out_var,
                Some(fallback),
            )));
        }
    }

    // Fast-path: `SELECT (COUNT(DISTINCT ?o) AS ?c) WHERE { ?s <p> ?o }`
    // by scanning POST and counting distinct encoded object IDs.
    if enable_fused_fast_paths {
        if let Some((pred, out_var)) = detect_predicate_count_distinct_object(query) {
            let fallback = build_operator_tree_inner(query, stats.clone(), false, planning)?;
            return Ok(Box::new(predicate_scalar_agg_operator(
                ScalarAggKind::CountDistinctObject,
                pred,
                out_var,
                Some(fallback),
            )));
        }
    }

    // Fast-path: `SELECT (MIN(?o) AS ?min) WHERE { ?s <p> ?o }` and MAX(...)
    // when the object is string-dict-backed. This inspects only POST leaflet directory keys.
    if enable_fused_fast_paths {
        if let Some((pred, mode, out_var)) = detect_predicate_minmax_string(query) {
            let fallback = build_operator_tree_inner(query, stats.clone(), false, planning)?;
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
        if let Some((pred, prefix, out_var)) = detect_string_prefix_count_all(query) {
            let fallback = build_operator_tree_inner(query, stats.clone(), false, planning)?;
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
        if let Some((pred, prefix, out_var)) = detect_string_prefix_sum_strstarts(query) {
            let fallback = build_operator_tree_inner(query, stats.clone(), false, planning)?;
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
        if let Some((pred, lang_tag, out_var)) = detect_predicate_count_rows_lang_filter(query) {
            let fallback = build_operator_tree_inner(query, stats.clone(), false, planning)?;
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
            detect_predicate_count_rows_numeric_compare(query)
        {
            let fallback = build_operator_tree_inner(query, stats.clone(), false, planning)?;
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
        if let Some((tp, filters, out_var)) = detect_count_rows_with_encoded_filters(query) {
            let fallback = build_operator_tree_inner(query, stats.clone(), false, planning)?;
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
                tp.clone(),
                None,
                inline_ops.clone(),
                emit,
                None,
                planning.mode(),
            ));
            // Serial scan-count → general aggregate pipeline (the correct path for
            // overlay/time-travel/policy). This is the fast path's fallback.
            let scan_count: BoxedOperator =
                Box::new(CountRowsOperator::new(scan, out_var, Some(fallback)));
            // At HEAD, count the rows passing the encoded pre-filters in parallel
            // across leaf chunks (no per-row binding materialization); otherwise, or
            // if a filter can't be pushed to encoded columns, fall back to the scan.
            return Ok(Box::new(
                crate::fast_count::count_rows_encoded_filters_operator(
                    tp,
                    inline_ops,
                    out_var,
                    Some(scan_count),
                ),
            ));
        }
    }

    // Fast-path: `SELECT (COUNT(?x) AS ?c) WHERE { ?s <p> ?o }` (and COUNT(*))
    // answered from PSOT leaflet directory row counts (no scan / no decoding).
    if enable_fused_fast_paths {
        if let Some((pred, out_var)) = detect_predicate_count_rows(query) {
            let fallback = build_operator_tree_inner(query, stats.clone(), false, planning)?;
            return Ok(Box::new(count_rows_operator(pred, out_var, Some(fallback))));
        }
    }

    // Fast-path: `COUNT(*)` of `?s rdf:type <Class> . ?s P ?o` — the P-flake count on
    // instances of one bound class, from per-(class,property) stats. Before the count
    // planner, which would otherwise run it as a real class-instances ⋈ P join.
    if enable_fused_fast_paths {
        if let Some((type_pred, class_obj, property, out_var)) = detect_class_property_count(query)
        {
            let fallback = build_operator_tree_inner(query, stats.clone(), false, planning)?;
            return Ok(Box::new(crate::fast_count::class_property_count_operator(
                type_pred,
                class_obj,
                property,
                out_var,
                Some(fallback),
            )));
        }
    }

    // Count-only plan: generic join-aware count planner that handles star joins, chains,
    // and modifier combinations (OPTIONAL, MINUS, EXISTS, object-chain patterns).
    // Fires after trivial metadata-only counts but before the remaining specialized fast paths.
    if enable_fused_fast_paths {
        if let Some(plan) = crate::count_plan::try_build_count_plan(query) {
            let fallback = build_operator_tree_inner(query, stats.clone(), false, planning)?;
            return Ok(crate::count_plan_exec::count_plan_operator(
                plan,
                Some(fallback),
            ));
        }
    }

    // (The 2-pattern multicolumn join `?s p1 ?o . ?s p2 ?o` COUNT(*) is now handled
    // by the generic count planner above — see `count_plan::try_build_multicolumn_join`.)

    // Fast-path: `COUNT(DISTINCT ?o1)` with an existence-only same-subject join.
    if enable_fused_fast_paths {
        if let Some((count_pred, exists_pred, out_var)) =
            detect_exists_join_count_distinct_object(query)
        {
            let fallback = build_operator_tree_inner(query, stats.clone(), false, planning)?;
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
        if let Some(out_var) = detect_count_blank_node_subjects(query) {
            let fallback = build_operator_tree_inner(query, stats.clone(), false, planning)?;
            return Ok(Box::new(count_blank_node_subjects_operator(
                out_var,
                Some(fallback),
            )));
        }
    }

    // Fast-path: `SELECT (COUNT(?o) AS ?c) WHERE { ?s ?p ?o FILTER ISLITERAL(?o) }`
    // answered from PSOT leaflet metadata by counting non-node-ref `o_type` rows.
    if enable_fused_fast_paths {
        if let Some(out_var) = detect_count_literal_objects(query) {
            let fallback = build_operator_tree_inner(query, stats.clone(), false, planning)?;
            return Ok(Box::new(count_literal_objects_operator(
                out_var,
                Some(fallback),
            )));
        }
    }

    // Fast-path: `SELECT (COUNT(DISTINCT ?s|?p|?o) AS ?c) WHERE { ?s ?p ?o }`
    // answered metadata-only: subjects from SPOT `lead_group_count` + boundary
    // correction, predicates from PSOT `p_const` transitions, objects from OPST
    // `lead_group_count` + boundary correction. The object variant is mutually
    // exclusive with every detector between here and its old position, so folding
    // it into this block does not change which fast path fires.
    if enable_fused_fast_paths {
        if let Some((position, out_var)) = detect_count_distinct_position(query) {
            let fallback = build_operator_tree_inner(query, stats.clone(), false, planning)?;
            return Ok(Box::new(count_distinct_position_operator(
                position,
                out_var,
                Some(fallback),
            )));
        }
    }

    // Fast-path: `SELECT (COUNT(?s) AS ?c) WHERE { ?s ?p ?o }`
    // answered metadata-only by summing leaf row_count across a branch manifest.
    if enable_fused_fast_paths {
        if let Some(out_var) = detect_count_triples(query) {
            let fallback = build_operator_tree_inner(query, stats.clone(), false, planning)?;
            return Ok(Box::new(count_triples_operator(out_var, Some(fallback))));
        }
    }

    // `?a <p1> ?b . OPTIONAL { ?b <p2> ?c . ?c <p3> ?d }` COUNT(*) is now handled
    // by the generic count planner above (CountPlanRoot::OptionalChainHead).

    // Fast-path: `SELECT (COUNT(*) AS ?c) WHERE { <S> <p>+ ?o }`
    // Avoids repeated range scans by building adjacency once and traversing.
    if enable_fused_fast_paths {
        if let Some((pred_sid, subject, out_var)) =
            detect_property_path_plus_fixed_subject_count_all(query)
        {
            let fallback = build_operator_tree_inner(query, stats.clone(), false, planning)?;
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
        if let Some((union_preds, extra_preds, mode, out_var)) = detect_union_star_count_all(query)
        {
            let fallback = build_operator_tree_inner(query, stats.clone(), false, planning)?;
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
        if let Some((p1, p2, out_var)) = detect_transitive_path_plus_count_all(query) {
            let fallback = build_operator_tree_inner(query, stats.clone(), false, planning)?;
            return Ok(Box::new(transitive_path_plus_count_all_operator(
                p1,
                p2,
                out_var,
                Some(fallback),
            )));
        }
    }

    // Fast-path: constant-object star constraints + numeric existence filter + label ORDER BY + LIMIT.
    if enable_fused_fast_paths {
        if let Some(spec) = detect_star_const_numeric_label_order_limit(query) {
            let fallback = build_operator_tree_inner(query, stats.clone(), false, planning)?;
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

    // Fast-path: `ORDER BY DESC(?o) LIMIT k` over a single bound predicate's
    // object (optionally `?s a <Class>`), served by a bounded reverse scan of
    // the POST index tail instead of a full-drain top-k sort.
    if enable_fused_fast_paths {
        if let Some(spec) = detect_post_order_desc_limit(query, stats.as_deref()) {
            let fallback = build_operator_tree_inner(query, stats.clone(), false, planning)?;
            return Ok(post_order_desc_limit_operator(
                spec.projected,
                spec.subject_var,
                spec.object_var,
                spec.anchor_pred,
                spec.class_term,
                spec.distinct,
                spec.limit,
                spec.offset,
                Some(fallback),
            ));
        }
    }

    // Fast-path: label scan + regex filter + rdf:type membership check.
    if enable_fused_fast_paths {
        if let Some(spec) = detect_label_regex_type(query) {
            let fallback = build_operator_tree_inner(query, stats.clone(), false, planning)?;
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
        )) = detect_group_by_object_star_topk(query)
        {
            let fallback = build_operator_tree_inner(query, stats.clone(), false, planning)?;
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
    if !planning.is_history() && !fast_paths_globally_disabled {
        if let Some((pred, s_var, o_var, count_var, limit)) =
            detect_predicate_group_by_object_count_topk(query)
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
        if let Some((pred, sep, out_var)) = detect_sum_strlen_group_concat_subquery(query) {
            let fallback = build_operator_tree_inner(query, stats.clone(), false, planning)?;
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
    if !planning.is_history() && !fast_paths_globally_disabled {
        if let Some((pred, s_var, obj, count_var)) = detect_predicate_object_count(query) {
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
            if let Some(offset) = query.offset {
                if offset > 0 {
                    operator = Box::new(OffsetOperator::new(operator, offset));
                }
            }

            // LIMIT
            if let Some(limit) = query.limit {
                operator = Box::new(LimitOperator::new(operator, limit));
            }

            return Ok(operator);
        }
    }

    // Fast-path: per-predicate count answered from POST leaf-directory
    // metadata (exact; see stats_query.rs for why IndexStats numbers are
    // NOT used as answers). Part of the fused chain: gating on
    // `enable_fused_fast_paths` (a) skips it in History mode — directory
    // rows are current-state only — and (b) keeps the fallback recursion
    // below (which passes `false`) from re-entering this block. The
    // operator's open()-time gate (`fast_path_store`) defers to the
    // planned fallback under overlay/time-travel/policy/multi-ledger.
    if enable_fused_fast_paths {
        {
            if let Some((pred_var, count_var)) = detect_stats_count_by_predicate(query) {
                let fallback = build_operator_tree_inner(query, stats.clone(), false, planning)?;
                let mut operator: BoxedOperator = Box::new(stats_count_by_predicate_operator(
                    pred_var,
                    count_var,
                    Some(fallback),
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
                if let Some(offset) = query.offset {
                    if offset > 0 {
                        operator = Box::new(OffsetOperator::new(operator, offset));
                    }
                }

                // LIMIT
                if let Some(limit) = query.limit {
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

    // The WHERE planner takes the GROUP BY keys as a hint (it can stream-group
    // a sorted scan). The remainder of the grouping phase (aggregates, HAVING,
    // post-binds) is consumed by the shared `apply_solution_modifiers` tail.
    let group_by_vec: Vec<VarId> = query
        .grouping
        .iter()
        .flat_map(Grouping::group_by_vars)
        .collect();

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

    // The solution-modifier tail (grouping → HAVING → post-binds → order-binds
    // → sort/validate → PROJECT → DISTINCT → OFFSET → LIMIT) is shared with the
    // per-row correlated-subquery pipeline (`SubqueryOperator`) via
    // `apply_solution_modifiers`, so both inherit identical modifier semantics.
    // Only the WHERE build and outermost-only concerns (post-VALUES) stay here.
    let projected = query.output.projected_vars();
    apply_solution_modifiers(
        operator,
        query.grouping.as_ref(),
        &query.order_binds,
        &query.ordering,
        projected.as_deref(),
        query.output.is_distinct(),
        query.offset,
        query.limit,
        detect_partitioned_group_by(query),
        variable_deps.as_ref(),
    )
}

/// Apply the SPARQL solution-modifier tail to an already-built WHERE operator.
///
/// Shared by the top-level query pipeline (`build_operator_tree_inner`) and the
/// per-row correlated-subquery pipeline (`SubqueryOperator`). Runs, in order:
/// GROUP BY + aggregation, HAVING, post-aggregation binds, expression/aggregate
/// ORDER-BY binds, sort-var validation, ORDER BY (with safe top-k and the
/// project-distinct-before-sort optimization), PROJECT, DISTINCT, OFFSET, LIMIT.
///
/// `operator` is the WHERE output (seeded or not). `select_vars` is the plain
/// projected-variable list (`None` = no projection, e.g. wildcard). Outermost-
/// only concerns (output formatting, CONSTRUCT/ASK, post-VALUES) stay in the
/// caller. `variable_deps` drives projection trimming; pass `None` to skip it.
/// `partitioned` is the streaming-GroupAggregate partition hint (callers that
/// don't benefit pass `false`).
#[allow(clippy::too_many_arguments)]
pub(crate) fn apply_solution_modifiers(
    mut operator: BoxedOperator,
    grouping: Option<&Grouping>,
    order_binds: &[(VarId, Expression)],
    ordering: &[SortSpec],
    select_vars: Option<&[VarId]>,
    distinct: bool,
    offset: Option<usize>,
    limit: Option<usize>,
    partitioned: bool,
    variable_deps: Option<&VariableDeps>,
) -> Result<BoxedOperator> {
    // Flatten the grouping phase's data for consumption below. The variant
    // distinction has already done its structural work at the IR boundary; the
    // tail treats both variants uniformly. Cloning is cheap — both vectors are
    // short and this is one-shot setup, not per-row work.
    let group_by_vec: Vec<VarId> = grouping
        .into_iter()
        .flat_map(Grouping::group_by_vars)
        .collect();
    let aggregates_vec: Vec<AggregateSpec> = grouping
        .map(|g| g.aggregates().cloned().collect())
        .unwrap_or_default();
    let post_binds_vec: Vec<(VarId, Expression)> = grouping
        .map(|g| g.binds().cloned().collect())
        .unwrap_or_default();
    let having_expr: Option<&Expression> = grouping.and_then(Grouping::having);

    let needs_grouping = !group_by_vec.is_empty() || !aggregates_vec.is_empty();

    // SPARQL 1.1 §11.4 (GROUP BY) / §18.5: a GROUP BY variable — or, in an
    // ungrouped query, a SELECT variable — that the WHERE never binds is legal.
    // It is simply unbound: every solution shares the same unbound value, so
    // they collapse into one group on that key, and the variable is reported
    // unbound. Materialize such variables as Unbound columns (an identity
    // `BIND(?v AS ?v)` over the absent var evaluates to Unbound) so grouping and
    // projection treat them as unbound instead of failing the schema lookup.
    // Aggregate INPUT variables are intentionally NOT padded here — a missing
    // aggregate input stays an error.
    let mut where_schema_vec: Vec<VarId> = operator.schema().to_vec();
    {
        let mut want: Vec<VarId> = group_by_vec.clone();
        if !needs_grouping {
            if let Some(sv) = select_vars {
                want.extend_from_slice(sv);
            }
        }
        for v in want {
            if !where_schema_vec.contains(&v) {
                operator = Box::new(crate::bind::BindOperator::new(
                    operator,
                    v,
                    Expression::Var(v),
                    Vec::new(),
                ));
                where_schema_vec.push(v);
            }
        }
    }
    // Get the schema after WHERE (before grouping), including any unbound pads.
    let where_schema: Arc<[VarId]> = Arc::from(where_schema_vec.into_boxed_slice());

    // GROUP BY + Aggregates
    // We use streaming GroupAggregateOperator when all aggregates are streamable
    // (COUNT, SUM, AVG, MIN, MAX). This is O(groups) memory instead of O(rows).
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
        let group_by_set: HashSet<VarId> = group_by_vec.iter().copied().collect();
        let mut seen_output_vars: HashSet<VarId> = HashSet::new();

        for spec in &aggregates_vec {
            if let Some(input_var) = spec.function.input_var() {
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
                    .function
                    .input_var()
                    .and_then(|v| current_schema.iter().position(|&sv| sv == v));
                StreamingAggSpec {
                    function: spec.function.clone(),
                    input_col,
                    output_var: spec.output_var,
                }
            })
            .collect();

        // The streaming GroupAggregateOperator only outputs GROUP BY keys + aggregate outputs.
        // If the SELECT projects any *grouped* variables (non-key, non-aggregate),
        // we must use the traditional GroupByOperator path so those vars become
        // `Binding::Grouped(Vec<Binding>)` and remain selectable.
        let select_needs_grouped_vars = select_vars.is_some_and(|vars| {
            vars.iter().any(|v| {
                !group_by_vec.contains(v) && !aggregates_vec.iter().any(|a| a.output_var == *v)
            })
        });

        let use_streaming = !aggregates_vec.is_empty()
            && GroupAggregateOperator::all_streamable(&streaming_specs)
            && !select_needs_grouped_vars;

        if use_streaming {
            // Streaming path: O(groups) memory
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

    // Expression-based ORDER BY binds (e.g. `ORDER BY DESC(?a / ?b)`).
    //
    // Evaluated once per solution as a dedicated stage AFTER
    // grouping/aggregation/HAVING/post-binds and BEFORE the sort, so the sort
    // keys can reference GROUP BY keys, aggregate outputs, and SELECT post-binds.
    // For ungrouped queries this is simply a post-WHERE stage. This placement is
    // what makes expression ORDER BY work uniformly across no-grouping,
    // dedup-only GROUP BY (no aggregation stage), and aggregating queries.
    if !order_binds.is_empty() {
        // Under grouping, an order-key expression may only read GROUP BY keys,
        // aggregate outputs, and post-aggregation bind outputs. Referencing any
        // other variable means it is `Binding::Grouped` here — reject cleanly
        // rather than evaluating the bind over a grouped binding (which panics
        // in `eval`), matching how bare `ORDER BY ?groupedVar` is rejected.
        if needs_grouping {
            let mut allowed: HashSet<VarId> = group_by_vec.iter().copied().collect();
            for spec in &aggregates_vec {
                allowed.insert(spec.output_var);
            }
            for (var, _) in &post_binds_vec {
                allowed.insert(*var);
            }
            for (out_var, expr) in order_binds {
                for v in expr.referenced_vars() {
                    if !allowed.contains(&v) {
                        return Err(QueryError::InvalidQuery(format!(
                            "ORDER BY expression references variable {v:?}, which is not a GROUP BY key or aggregate result"
                        )));
                    }
                }
                // A later order bind may legitimately reference an earlier one.
                allowed.insert(*out_var);
            }
        }
        for (var, expr) in order_binds {
            operator = Box::new(crate::bind::BindOperator::new(
                operator,
                *var,
                expr.clone(),
                vec![],
            ));
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
    let can_project_distinct_before_sort = distinct
        && !ordering.is_empty()
        && select_vars
            .is_some_and(|vars| !vars.is_empty() && ordering.iter().all(|s| vars.contains(&s.var)));

    // Validate SELECT vars (when present) exist in the post-group schema.
    if let Some(vars) = select_vars {
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
    if !ordering.is_empty() {
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
            // Post-aggregation binds (SELECT expressions like `(CEIL(?avg) AS
            // ?c)`) are per-group scalars computed after aggregation — they are
            // valid sort keys.
            for (var, _) in &post_binds_vec {
                allowed.insert(*var);
            }
            // Desugared expression-ORDER-BY keys run as a post-grouping stage
            // and are validated above to read only allowed vars.
            for (var, _) in order_binds {
                allowed.insert(*var);
            }
            allowed_sort_vars = Some(allowed);
        }
        for spec in ordering {
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
        if let Some(vars) = select_vars {
            operator = Box::new(ProjectOperator::new(operator, vars.to_vec()));
        }
        // DISTINCT (pre-sort)
        operator = Box::new(DistinctOperator::new(operator));

        // ORDER BY (post-distinct, projected vars only)
        let k = match (limit, offset) {
            (Some(limit), Some(offset)) => limit.saturating_add(offset),
            (Some(limit), None) => limit,
            _ => 0,
        };
        let can_topk = limit.is_some();
        let mut sort_op = if can_topk {
            SortOperator::new_topk(operator, ordering.to_vec(), k)
        } else {
            SortOperator::new(operator, ordering.to_vec())
        };
        sort_op = sort_op.with_out_schema(
            variable_deps
                .as_ref()
                .map(|d| d.required_sort_vars.as_slice()),
        );
        operator = Box::new(sort_op);
    } else {
        // ORDER BY (before projection - may reference vars not in SELECT)
        if !ordering.is_empty() {
            // Safe top-k: ORDER BY + (OFFSET o) + LIMIT l can keep only (o + l) rows.
            //
            // This is safe when DISTINCT is not in play because slicing happens after sorting.
            let can_topk = limit.is_some() && !distinct;
            let k = match (limit, offset) {
                (Some(limit), Some(offset)) => limit.saturating_add(offset),
                (Some(limit), None) => limit,
                _ => 0,
            };
            let mut sort_op = if can_topk {
                SortOperator::new_topk(operator, ordering.to_vec(), k)
            } else {
                SortOperator::new(operator, ordering.to_vec())
            };
            sort_op = sort_op.with_out_schema(
                variable_deps
                    .as_ref()
                    .map(|d| d.required_sort_vars.as_slice()),
            );
            operator = Box::new(sort_op);
        }

        // PROJECT
        if let Some(vars) = select_vars {
            if !vars.is_empty() {
                operator = Box::new(ProjectOperator::new(operator, vars.to_vec()));
            }
        }

        // DISTINCT (after projection)
        if distinct {
            operator = Box::new(DistinctOperator::new(operator));
        }
    }

    // OFFSET
    if let Some(offset) = offset {
        if offset > 0 {
            operator = Box::new(OffsetOperator::new(operator, offset));
        }
    }

    // LIMIT
    if let Some(limit) = limit {
        operator = Box::new(LimitOperator::new(operator, limit));
    }

    Ok(operator)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::triple::{Ref, Term, TriplePattern};
    use crate::ir::Pattern;
    use crate::ir::ReasoningConfig;
    use crate::ir::{Query, QueryOutput};
    use crate::sort::SortSpec;
    use fluree_db_core::NsCode;
    use fluree_db_core::Sid;
    use fluree_graph_json_ld::ParsedContext;

    fn make_pattern(s_var: VarId, p_name: &str, o_var: VarId) -> TriplePattern {
        TriplePattern::new(
            Ref::Var(s_var),
            Ref::Sid(Sid::new(NsCode(100), p_name)),
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
            reasoning: ReasoningConfig::default(),
            grouping: None,
            ordering: Vec::new(),
            order_binds: Vec::new(),
            limit: None,
            offset: None,
            post_values: None,
        }
    }

    #[test]
    fn detect_stats_count_by_predicate_declines_expression_order_by() {
        // The count-by-predicate fast path sorts on `query.ordering` directly,
        // so it must decline when `order_binds` are present (the synthetic sort
        // var is only bound by the generic pipeline's order-bind stage).
        let s = VarId(0);
        let p = VarId(1);
        let o = VarId(2);
        let c = VarId(3);
        let order_key = VarId(4);

        let make = |order_binds: Vec<(VarId, Expression)>, ordering: Vec<SortSpec>| Query {
            context: ParsedContext::default(),
            orig_context: None,
            output: QueryOutput::select_all(vec![p, c]),
            patterns: vec![Pattern::Triple(TriplePattern::new(
                Ref::Var(s),
                Ref::Var(p),
                Term::Var(o),
            ))],
            reasoning: ReasoningConfig::default(),
            grouping: Grouping::assemble(
                vec![p],
                vec![AggregateSpec {
                    function: AggregateFn::Count(s),
                    output_var: c,
                }],
                vec![],
                None,
            ),
            ordering,
            order_binds,
            limit: None,
            offset: None,
            post_values: None,
        };

        // Bare-variable ORDER BY (no order binds): fast path applies.
        let plain = make(Vec::new(), vec![SortSpec::asc(c)]);
        assert_eq!(detect_stats_count_by_predicate(&plain), Some((p, c)));

        // Expression ORDER BY (order binds present): must decline.
        let with_expr = make(
            vec![(order_key, Expression::Const(crate::ir::FlakeValue::Long(0)))],
            vec![SortSpec::asc(order_key)],
        );
        assert!(detect_stats_count_by_predicate(&with_expr).is_none());
    }

    #[test]
    fn test_detect_star_const_numeric_label_order_limit() {
        let s = VarId(0);
        let label = VarId(1);
        let v = VarId(2);

        let p_label = Ref::Sid(Sid::new(NsCode(100), "label"));
        let p_num = Ref::Sid(Sid::new(NsCode(100), "num"));
        let p_c1 = Ref::Sid(Sid::new(NsCode(100), "c1"));
        let p_c2 = Ref::Sid(Sid::new(NsCode(100), "c2"));

        let patterns = vec![
            Pattern::Triple(TriplePattern::new(
                Ref::Var(s),
                p_label.clone(),
                Term::Var(label),
            )),
            Pattern::Triple(TriplePattern::new(
                Ref::Var(s),
                p_c1.clone(),
                Term::Sid(Sid::new(NsCode(100), "o1")),
            )),
            Pattern::Triple(TriplePattern::new(
                Ref::Var(s),
                p_c2.clone(),
                Term::Sid(Sid::new(NsCode(100), "o2")),
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
            reasoning: ReasoningConfig::default(),
            grouping: None,
            ordering: vec![SortSpec::asc(label)],
            order_binds: Vec::new(),
            limit: Some(10),
            offset: None,
            post_values: None,
        };

        let spec =
            detect_star_const_numeric_label_order_limit(&query).expect("should detect shape");
        assert_eq!(spec.subject_var, s);
        assert_eq!(spec.label_var, label);
        assert_eq!(spec.limit, 10);
        assert_eq!(spec.const_constraints.len(), 2);
        assert_eq!(spec.numeric_pred, p_num);
        assert_eq!(spec.label_pred, p_label);
    }

    #[test]
    fn test_build_operator_tree_allows_unbound_select_var() {
        // SPARQL 1.1 §18.5: selecting a variable not bound by the pattern is
        // legal — it is reported unbound, not an error. The tree pads it as an
        // Unbound column rather than failing the schema lookup.
        let query = Query {
            context: ParsedContext::default(),
            orig_context: None,
            output: QueryOutput::select_all(vec![VarId(99)]), // Variable not in pattern
            patterns: vec![Pattern::Triple(make_pattern(VarId(0), "name", VarId(1)))],
            reasoning: ReasoningConfig::default(),
            grouping: None,
            ordering: Vec::new(),
            order_binds: Vec::new(),
            limit: None,
            offset: None,
            post_values: None,
        };

        let result = build_operator_tree(
            &query,
            None,
            &crate::temporal_mode::PlanningContext::current(),
        );
        assert!(
            result.is_ok(),
            "selecting an unbound variable should succeed (reported unbound)"
        );
    }

    #[test]
    fn test_build_operator_tree_validates_sort_vars() {
        let query = Query {
            context: ParsedContext::default(),
            orig_context: None,
            output: QueryOutput::select_all(vec![VarId(0)]),
            patterns: vec![Pattern::Triple(make_pattern(VarId(0), "name", VarId(1)))],
            reasoning: ReasoningConfig::default(),
            grouping: None,
            ordering: vec![SortSpec::asc(VarId(99))], // Invalid var
            order_binds: Vec::new(),
            limit: None,
            offset: None,
            post_values: None,
        };

        let result = build_operator_tree(
            &query,
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
        let exists_pred = Ref::Sid(Sid::new(NsCode(100), "rdf:type"));
        let count_pred = Ref::Sid(Sid::new(NsCode(100), "sourceLink"));

        let make_grouping = || {
            Some(Grouping::Implicit {
                aggregation: Aggregation {
                    aggregates: fluree_db_core::NonEmpty::try_from_vec(vec![
                        crate::ir::AggregateSpec {
                            function: crate::ir::AggregateFn::CountDistinct(counted_o),
                            output_var: out,
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
            reasoning: ReasoningConfig::default(),
            grouping: make_grouping(),
            ordering: Vec::new(),
            order_binds: Vec::new(),
            limit: None,
            offset: None,
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
            reasoning: ReasoningConfig::default(),
            grouping: make_grouping(),
            ordering: Vec::new(),
            order_binds: Vec::new(),
            limit: None,
            offset: None,
            post_values: None,
        };
        assert_eq!(
            detect_exists_join_count_distinct_object(&counted_first),
            Some((count_pred.clone(), exists_pred.clone(), out))
        );
        assert_eq!(
            detect_exists_join_count_distinct_object(&reversed),
            Some((count_pred, exists_pred, out))
        );
    }

    #[test]
    fn test_detect_sum_numeric_compare_as_count() {
        let s = VarId(0);
        let o = VarId(1);
        let synth = VarId(2);
        let out = VarId(3);
        let pred = Ref::Sid(Sid::new(NsCode(100), "numberOfCreators"));

        // SELECT (SUM(?synth) AS ?out) WHERE { ?s <pred> ?o . BIND(?o > 0 AS ?synth) }
        let make_query = |function: AggregateFn| Query {
            context: ParsedContext::default(),
            orig_context: None,
            output: QueryOutput::select_all(vec![out]),
            patterns: vec![
                Pattern::Triple(TriplePattern::new(Ref::Var(s), pred.clone(), Term::Var(o))),
                Pattern::Bind {
                    var: synth,
                    expr: crate::ir::Expression::gt(
                        crate::ir::Expression::Var(o),
                        crate::ir::Expression::Const(crate::ir::FlakeValue::Long(0)),
                    ),
                },
            ],
            reasoning: ReasoningConfig::default(),
            grouping: Some(Grouping::Implicit {
                aggregation: Aggregation {
                    aggregates: fluree_db_core::NonEmpty::try_from_vec(vec![
                        crate::ir::AggregateSpec {
                            function,
                            output_var: out,
                        },
                    ])
                    .unwrap(),
                    binds: Vec::new(),
                },
                having: None,
            }),
            ordering: Vec::new(),
            order_binds: Vec::new(),
            limit: None,
            offset: None,
            post_values: None,
        };

        // SUM (List / non-DISTINCT) over the comparison synth var => detected as Gt/threshold 0.
        let q = make_query(AggregateFn::Sum(synth, InputSemantics::List));
        assert_eq!(
            detect_sum_numeric_compare_as_count(&q),
            Some((
                pred.clone(),
                NumericCompareOp::Gt,
                fluree_db_core::FlakeValue::Long(0),
                out
            ))
        );

        // SUM(DISTINCT ...) must be rejected: SUM(DISTINCT bool) is not a row count.
        let q_distinct = make_query(AggregateFn::Sum(synth, InputSemantics::Set));
        assert_eq!(detect_sum_numeric_compare_as_count(&q_distinct), None);

        // A non-SUM aggregate over the same shape must be rejected.
        let q_count = make_query(AggregateFn::Count(synth));
        assert_eq!(detect_sum_numeric_compare_as_count(&q_count), None);
    }

    #[test]
    fn test_detect_post_order_desc_limit() {
        let s = VarId(0);
        let o = VarId(1);
        let date_pred = Ref::Sid(Sid::new(NsCode(100), "dateModified"));
        let rdf_type = Ref::Iri(std::sync::Arc::from(fluree_vocab::rdf::TYPE));
        let class = Term::Sid(Sid::new(NsCode(13), "Conversation"));

        let anchor = Pattern::Triple(TriplePattern::new(Ref::Var(s), date_pred, Term::Var(o)));
        let type_tp = Pattern::Triple(TriplePattern::new(Ref::Var(s), rdf_type, class));

        let mk = |patterns: Vec<Pattern>,
                  output: QueryOutput,
                  ordering: Vec<SortSpec>,
                  limit: Option<usize>,
                  offset: Option<usize>| Query {
            context: ParsedContext::default(),
            orig_context: None,
            output,
            patterns,
            reasoning: ReasoningConfig::default(),
            grouping: None,
            ordering,
            order_binds: Vec::new(),
            limit,
            offset,
            post_values: None,
        };

        // Positive: `?s a <Class> ; <p> ?o` ORDER BY DESC(?o) LIMIT 5, SELECT ?s ?o.
        let q = mk(
            vec![type_tp.clone(), anchor.clone()],
            QueryOutput::select_all(vec![s, o]),
            vec![SortSpec::desc(o)],
            Some(5),
            None,
        );
        let spec = detect_post_order_desc_limit(&q, None).expect("class + anchor should detect");
        assert_eq!(spec.subject_var, s);
        assert_eq!(spec.object_var, o);
        assert!(spec.class_term.is_some());
        assert_eq!(spec.limit, 5);
        assert_eq!(spec.offset, 0);
        assert!(!spec.distinct);
        assert_eq!(spec.projected, vec![s, o]);

        // Positive: single anchor (no class), with OFFSET.
        let q = mk(
            vec![anchor.clone()],
            QueryOutput::select_all(vec![s, o]),
            vec![SortSpec::desc(o)],
            Some(3),
            Some(2),
        );
        let spec = detect_post_order_desc_limit(&q, None).expect("single triple should detect");
        assert!(spec.class_term.is_none());
        assert_eq!(spec.offset, 2);

        // Positive: DISTINCT with the order var projected.
        let q = mk(
            vec![anchor.clone()],
            QueryOutput::select_distinct(vec![s, o]),
            vec![SortSpec::desc(o)],
            Some(5),
            None,
        );
        assert!(
            detect_post_order_desc_limit(&q, None).is_some_and(|sp| sp.distinct),
            "DISTINCT with ?o projected should detect"
        );

        // Negative cases.
        let asc = mk(
            vec![anchor.clone()],
            QueryOutput::select_all(vec![s, o]),
            vec![SortSpec::asc(o)],
            Some(5),
            None,
        );
        assert!(
            detect_post_order_desc_limit(&asc, None).is_none(),
            "ASC must not match"
        );

        let no_limit = mk(
            vec![anchor.clone()],
            QueryOutput::select_all(vec![s, o]),
            vec![SortSpec::desc(o)],
            None,
            None,
        );
        assert!(
            detect_post_order_desc_limit(&no_limit, None).is_none(),
            "missing LIMIT must not match"
        );

        let order_by_subject = mk(
            vec![anchor.clone()],
            QueryOutput::select_all(vec![s, o]),
            vec![SortSpec::desc(s)],
            Some(5),
            None,
        );
        assert!(
            detect_post_order_desc_limit(&order_by_subject, None).is_none(),
            "ordering by a non-object var must not match"
        );

        let with_filter = mk(
            vec![
                anchor.clone(),
                Pattern::Filter(crate::ir::Expression::Var(o)),
            ],
            QueryOutput::select_all(vec![s, o]),
            vec![SortSpec::desc(o)],
            Some(5),
            None,
        );
        assert!(
            detect_post_order_desc_limit(&with_filter, None).is_none(),
            "a FILTER pattern must not match in v1"
        );

        let distinct_no_o = mk(
            vec![anchor],
            QueryOutput::select_distinct(vec![s]),
            vec![SortSpec::desc(o)],
            Some(5),
            None,
        );
        assert!(
            detect_post_order_desc_limit(&distinct_no_o, None).is_none(),
            "DISTINCT without ?o projected must not match"
        );
    }

    #[test]
    fn test_post_order_class_profitability_gate() {
        let pred = Ref::Sid(Sid::new(NsCode(100), "dateModified"));
        let class_sid = Sid::new(NsCode(13), "Conversation");
        let class = Term::Sid(class_sid.clone());

        let mk = |ndv_subjects: u64, class_count: u64| {
            let mut s = fluree_db_core::StatsView::default();
            s.properties.insert(
                Sid::new(NsCode(100), "dateModified"),
                fluree_db_core::PropertyStatData {
                    count: ndv_subjects,
                    ndv_values: ndv_subjects,
                    ndv_subjects,
                },
            );
            s.classes.insert(class_sid.clone(), class_count);
            s
        };

        // Common class (most dated subjects are Conversations) → tail scan wins.
        let common = mk(1_000, 950);
        assert!(post_order_class_is_profitable(&common, &pred, &class, 5));

        // Selective class (5 Conversations among 1M dated subjects) → the tail
        // scan would walk ~1M rows to find 5; the generic class anchor is cheaper.
        let selective = mk(1_000_000, 5);
        assert!(!post_order_class_is_profitable(
            &selective, &pred, &class, 5
        ));

        // No recorded members → never fills `need`.
        let empty = mk(1_000, 0);
        assert!(!post_order_class_is_profitable(&empty, &pred, &class, 5));

        // Missing stats → run it (the operator's runtime scan budget is the guard).
        let none = fluree_db_core::StatsView::default();
        assert!(post_order_class_is_profitable(&none, &pred, &class, 5));
    }
}
