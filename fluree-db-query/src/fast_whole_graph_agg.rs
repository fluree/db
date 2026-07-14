//! Fast-path: whole-graph scalar aggregates over a distinct-subject scan.
//!
//! Cypher `MATCH (n) RETURN count(n), count(n.age)` (and the min/max/avg/sum
//! family) lowers to:
//!
//! ```text
//! patterns: [ Subquery(SELECT DISTINCT ?n WHERE { ?n ?p ?o }),
//!             Optional([ ?n <P> ?prop ]) ]      -- one accessor, at most
//! grouping: Implicit, aggregates over ?n / ?prop
//! ```
//!
//! Executed generally, the subquery scans every flake in the graph and dedups
//! subjects through the DISTINCT operator before a single aggregate row comes
//! out — linear in graph size. Every aggregate the shape admits is answerable
//! from index directories and predicate-scoped scans instead:
//!
//! - `count(?n)` / `COUNT(*)`: each distinct subject contributes
//!   `max(1, #P(n))` rows through the left join, so the row count is
//!   `N + count(P) − subj(P)` (`N` alone with no accessor) — all three terms
//!   are directory-only reads.
//! - `count(?prop)` counts non-null rows = `count(P)`, the predicate's live
//!   row count. Exact for multi-valued `P` (each value is its own row).
//! - `count(DISTINCT ?n)` = `N`; `count(DISTINCT ?prop)` = the predicate's
//!   distinct-object lead-group count.
//! - `min/max(?prop)` over the left join ignore nulls and duplicates — the
//!   POST boundary-key reduction ([`crate::fast_min_max_string`]).
//! - `avg/sum(?prop)`: the rows carrying `?prop` are exactly one per `P`
//!   value, so the predicate-scoped POST fold
//!   ([`crate::fast_predicate_scalar_agg`]) matches the pipeline.
//!
//! With two or more accessor optionals the per-subject rows multiply
//! (`#P1(n) × #P2(n)`), which changes duplicate-sensitive aggregates — the
//! detector only admits the single-accessor shape.
//!
//! The class-anchored family generalizes the same folds to
//! `MATCH (n:C) RETURN …` (anchor = `?n rdf:type <C>`): the subject universe
//! becomes the class's instance count from the per-graph class stats, and
//! property-derived folds additionally require the **containment proof**
//! `class_property_flakes(C, P) == predicate_rows(P)` — equality means every
//! `P`-bearing subject is a `C`, so predicate-scoped folds equal the
//! class-restricted join. Both sides are current-state-exact at HEAD (class
//! stats per issue #1266; rows from PSOT directories).
//!
//! Beyond the single-row scalar shape, `GROUP BY ?prop` + `COUNT(*)` (the
//! Cypher histogram `RETURN n.age, COUNT(*)`) folds to a predicate-scoped
//! POST run-length group count (values are physically contiguous in POST)
//! plus one null-group row (`subjects − subj(P)` — the left join gives
//! property-less subjects a single unbound-key row).
//!
//! The directory-only [`compute_clean`] lane requires the strict metadata-lane
//! gate ([`fast_path_store`]: single-ledger, root/no policy, no overlay, at
//! HEAD). When uncommitted novelty is present the [`compute_overlay`] lane runs
//! instead: it keeps the O(directory) base counts and reconciles them against a
//! bounded O(novelty) pass — the subject universe via base lead-groups ± the
//! overlay's net subject delta, and each predicate-scoped fold via an
//! overlay-merging cursor (linear in the predicate's rows, not the graph's).
//! The overlay lane covers the whole-graph anchor's scalar folds; class anchors
//! and histograms under novelty still decline to the general pipeline.
//!
//! Both lanes decline when the graph carries predicates the variable-predicate
//! scan hides (`f:reifies*` anywhere, the `f:` namespace in the default graph)
//! — those facts are invisible to `?n ?p ?o` but not to the SPOT directories;
//! a class anchor reads only class stats, so it is immune.

use crate::binding::{Batch, Binding};
use crate::error::{QueryError, Result};
use crate::fast_count::{
    count_distinct_lead_groups, count_distinct_objects_for_predicate,
    count_distinct_subjects_for_predicate,
};
use crate::fast_min_max_string::{minmax_numeric_post, minmax_string_dict_post, MinMaxMode};
use crate::fast_path_common::{
    count_rows_for_predicate_psot, count_to_i64, fast_path_store, normalize_pred_sid,
    FastPathOperator,
};
use crate::fast_predicate_scalar_agg::{scan_predicate_scalar_agg, ScalarAggKind, SumExprI64};
use crate::ir::grouping::{AggregateFn, Aggregation, Grouping, InputSemantics};
use crate::ir::triple::{Ref, Term};
use crate::ir::Expression;
use crate::ir::{Pattern, Query};
use crate::operator::BoxedOperator;
use crate::var_registry::VarId;
use fluree_db_binary_index::format::run_record::RunSortOrder;
use fluree_db_binary_index::BinaryIndexStore;
use fluree_db_core::{FlakeValue, GraphId, Sid};
use std::sync::Arc;

/// One aggregate of the recognized shape, mapped to its fold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AggTask {
    /// `count(?n)` / `COUNT(*)` — total pipeline rows.
    CountRows,
    /// `count(DISTINCT ?n)` — distinct subjects.
    CountSubjects,
    /// `count(?prop)` — the accessor predicate's live row count.
    CountProp,
    /// `count(DISTINCT ?prop)` — the predicate's distinct objects.
    CountDistinctProp,
    MinProp,
    MaxProp,
    AvgProp,
    SumProp,
}

impl AggTask {
    fn needs_accessor(self) -> bool {
        !matches!(self, AggTask::CountRows | AggTask::CountSubjects)
    }
}

/// The subject universe the aggregates range over.
enum Anchor {
    /// Bare `MATCH (n)`: the DISTINCT-subject subquery over `?n ?p ?o`.
    AllSubjects,
    /// `MATCH (n:C)`: `?n rdf:type <C>` with a concrete class SID.
    Class(Sid),
}

/// What the fold produces.
enum FoldKind {
    /// One row of scalar aggregates (implicit grouping), one task per
    /// projected column in projection order.
    Scalars(Vec<AggTask>),
    /// `GROUP BY ?prop` + `COUNT(*)`: one row per distinct property value
    /// plus a null group. Column positions index into `schema`. An optional
    /// filter over the group key drops whole groups: every row in a group
    /// shares the key value, so per-group evaluation (including on the
    /// null group's `Unbound`) is exactly the pipeline's per-row filter.
    Histogram {
        prop_col: usize,
        count_col: usize,
        filter: Option<Expression>,
    },
}

/// Detected plan: anchor, accessor predicate (if any), fold kind, and the
/// output schema in projection order.
pub(crate) struct WholeGraphAggPlan {
    anchor: Anchor,
    accessor: Option<Ref>,
    kind: FoldKind,
    schema: Vec<VarId>,
}

/// Recognize the whole-graph / class-anchored aggregate shapes. See the
/// module docs for the exact patterns and per-aggregate soundness arguments.
pub(crate) fn detect_whole_graph_scalar_aggs(query: &Query) -> Option<WholeGraphAggPlan> {
    if !query.ordering.is_empty()
        || !query.order_binds.is_empty()
        || query.offset.is_some()
        || query.output.is_distinct()
        || query.limit == Some(0)
        || query.post_values.is_some()
    {
        return None;
    }

    // Anchor: DISTINCT-subject subquery, or a single class triple.
    let (first, rest) = query.patterns.split_first()?;
    let (anchor, subject_var) = match first {
        Pattern::Subquery(sq) => (Anchor::AllSubjects, distinct_subject_scan_var(sq)?),
        Pattern::Triple(tp) => {
            if !tp.p.is_rdf_type() || tp.dtc.is_some() {
                return None;
            }
            let sv = tp.s.as_var()?;
            let Term::Sid(class_sid) = &tp.o else {
                return None;
            };
            (Anchor::Class(class_sid.clone()), sv)
        }
        _ => return None,
    };

    // At most one single-triple property-accessor OPTIONAL, optionally
    // followed by an identity Bind aliasing the accessor var into the
    // projected column (`RETURN n.age, COUNT(*)` emits `?alias = ?prop`).
    let parse_accessor = |inner: &[Pattern]| -> Option<(Ref, VarId)> {
        let [Pattern::Triple(tp)] = inner else {
            return None;
        };
        if tp.s.as_var() != Some(subject_var) || !tp.p_bound() || tp.dtc.is_some() {
            return None;
        }
        let Term::Var(prop_var) = tp.o else {
            return None;
        };
        if prop_var == subject_var {
            return None;
        }
        Some((tp.p.clone(), prop_var))
    };
    let parse_alias = |acc: &(Ref, VarId), var: &VarId, expr: &Expression| -> Option<VarId> {
        if *expr != Expression::Var(acc.1) || *var == acc.1 || *var == subject_var {
            return None;
        }
        Some(*var)
    };
    let (accessor, prop_alias, key_filter) = match rest {
        [] => (None, None, None),
        [Pattern::Optional(inner)] => (Some(parse_accessor(inner)?), None, None),
        [Pattern::Optional(inner), Pattern::Bind { var, expr }] => {
            let acc = parse_accessor(inner)?;
            let alias = parse_alias(&acc, var, expr)?;
            (Some(acc), Some(alias), None)
        }
        [Pattern::Optional(inner), Pattern::Filter(f)] => {
            let acc = parse_accessor(inner)?;
            (Some(acc), None, Some(f.clone()))
        }
        [Pattern::Optional(inner), Pattern::Filter(f), Pattern::Bind { var, expr }] => {
            let acc = parse_accessor(inner)?;
            let alias = parse_alias(&acc, var, expr)?;
            (Some(acc), Some(alias), Some(f.clone()))
        }
        _ => return None,
    };
    let prop_var = accessor.as_ref().map(|(_, v)| *v);
    // A filter is admitted only when it reads nothing but the accessor value
    // (whole-group-uniform) and is synchronously evaluable.
    if let Some(f) = &key_filter {
        let refs = f.referenced_vars();
        if refs.is_empty()
            || refs.iter().any(|v| Some(*v) != prop_var)
            || crate::filter::contains_exists(f)
            || crate::eval::metadata_resolve::contains_metadata_read(f)
        {
            return None;
        }
    }
    let select_vars = query.output.projected_vars()?;

    let kind = match &query.grouping {
        // Scalars: single implicit group, aggregates only.
        Some(Grouping::Implicit {
            aggregation: Aggregation { aggregates, binds },
            having: None,
        }) => {
            if !binds.is_empty() || select_vars.len() != aggregates.len() {
                return None;
            }
            if key_filter.is_some() {
                // Scalar folds read predicate totals; a value filter would
                // change them. (Histograms filter per group instead.)
                return None;
            }
            let mut tasks = Vec::with_capacity(select_vars.len());
            for out in &select_vars {
                let spec = aggregates.iter().find(|a| a.output_var == *out)?;
                tasks.push(classify_aggregate(&spec.function, subject_var, prop_var)?);
            }
            FoldKind::Scalars(tasks)
        }
        // Histogram: GROUP BY the accessor value, one COUNT(*) / COUNT(?n).
        Some(Grouping::Explicit {
            group_by,
            aggregation: Some(Aggregation { aggregates, binds }),
            having: None,
        }) => {
            let prop_var = prop_var?;
            // The group key is the accessor var or its projection alias
            // (identity bind — identical value per row).
            let key = *group_by.first();
            if group_by.len() != 1 || (key != prop_var && Some(key) != prop_alias) {
                return None;
            }
            if aggregates.len() != 1 || !binds.is_empty() {
                return None;
            }
            let agg = aggregates.first();
            match &agg.function {
                AggregateFn::CountAll => {}
                AggregateFn::Count(v) if *v == subject_var => {}
                _ => return None,
            }
            if select_vars.len() != 2 {
                return None;
            }
            let prop_col = select_vars.iter().position(|v| *v == key)?;
            let count_col = select_vars.iter().position(|v| *v == agg.output_var)?;
            if prop_col == count_col {
                return None;
            }
            // The batch schema carries the group-key var; rewrite the filter
            // to reference it when the key is the projection alias (identity
            // bind — same value).
            let filter = key_filter.map(|mut f| {
                if key != prop_var {
                    f.substitute_var(prop_var, key);
                }
                f
            });
            FoldKind::Histogram {
                prop_col,
                count_col,
                filter,
            }
        }
        _ => return None,
    };

    Some(WholeGraphAggPlan {
        anchor,
        accessor: accessor.map(|(p, _)| p),
        kind,
        schema: select_vars,
    })
}

/// Match `Subquery(SELECT DISTINCT ?n WHERE { ?n ?p ?o })` (no modifiers) and
/// return `?n`. The body's `?p`/`?o` never escape (only `?n` is selected), so
/// correlation cannot arise regardless of the `uncorrelated` flag.
fn distinct_subject_scan_var(sq: &crate::ir::SubqueryPattern) -> Option<VarId> {
    if !sq.distinct
        || sq.limit.is_some()
        || sq.offset.is_some()
        || !sq.ordering.is_empty()
        || !sq.order_binds.is_empty()
        || sq.grouping.is_some()
    {
        return None;
    }
    let [subject_var] = sq.select.as_slice() else {
        return None;
    };
    let [Pattern::Triple(tp)] = sq.patterns.as_slice() else {
        return None;
    };
    if tp.dtc.is_some() {
        return None;
    }
    let (Ref::Var(sv), Ref::Var(pv), Term::Var(ov)) = (&tp.s, &tp.p, &tp.o) else {
        return None;
    };
    if sv != subject_var || pv == sv || ov == sv || pv == ov {
        return None;
    }
    Some(*subject_var)
}

fn classify_aggregate(
    function: &AggregateFn,
    subject_var: VarId,
    prop_var: Option<VarId>,
) -> Option<AggTask> {
    let is_prop = |v: VarId| prop_var == Some(v);
    match function {
        AggregateFn::CountAll => Some(AggTask::CountRows),
        AggregateFn::Count(v) if *v == subject_var => Some(AggTask::CountRows),
        AggregateFn::Count(v) if is_prop(*v) => Some(AggTask::CountProp),
        AggregateFn::CountDistinct(v) if *v == subject_var => Some(AggTask::CountSubjects),
        AggregateFn::CountDistinct(v) if is_prop(*v) => Some(AggTask::CountDistinctProp),
        AggregateFn::Min(v) if is_prop(*v) => Some(AggTask::MinProp),
        AggregateFn::Max(v) if is_prop(*v) => Some(AggTask::MaxProp),
        AggregateFn::Avg(v, InputSemantics::List) if is_prop(*v) => Some(AggTask::AvgProp),
        AggregateFn::Sum(v, InputSemantics::List) if is_prop(*v) => Some(AggTask::SumProp),
        _ => None,
    }
}

/// Histograms above this many distinct property values decline to the
/// general pipeline (bounds the single output batch).
const HISTOGRAM_MAX_GROUPS: usize = 65_536;

/// Create the fused operator; declines to `fallback` whenever any component
/// cannot be answered exactly.
pub(crate) fn whole_graph_scalar_aggs_operator(
    plan: WholeGraphAggPlan,
    fallback: Option<BoxedOperator>,
) -> FastPathOperator {
    let schema: Arc<[VarId]> = Arc::from(plan.schema.clone().into_boxed_slice());
    let closure_schema = schema.clone();
    FastPathOperator::with_schema(
        schema,
        move |ctx| {
            // Clean HEAD read: every fold comes straight from index directories.
            if let Some(store) = fast_path_store(ctx) {
                return compute_clean(ctx, store, &plan, &closure_schema);
            }
            // Novelty present: reconcile the directory counts against a bounded
            // overlay pass rather than declining to a whole-graph flake scan.
            if overlay_lane_eligible(ctx) {
                let store = ctx
                    .binary_store
                    .as_ref()
                    .expect("overlay lane eligibility guarantees a binary store");
                return compute_overlay(ctx, store, &plan, &closure_schema);
            }
            Ok(None)
        },
        fallback,
        "whole-graph scalar aggregates",
    )
}

/// Clean-HEAD fold: subject universe and every aggregate answered from index
/// directories / predicate-scoped metadata scans (no overlay merge).
fn compute_clean(
    ctx: &crate::context::ExecutionContext<'_>,
    store: &Arc<BinaryIndexStore>,
    plan: &WholeGraphAggPlan,
    schema: &Arc<[VarId]>,
) -> Result<Option<Batch>> {
    // Subject universe.
    let subjects = match &plan.anchor {
        Anchor::AllSubjects => {
            if graph_has_scan_hidden_predicates(ctx, store)? {
                return Ok(None);
            }
            // Distinct subjects from SPOT leaflet lead groups
            // (SPOT key layout: s_id(8) + …).
            count_distinct_lead_groups(store, ctx.binary_g_id, RunSortOrder::Spot, 8)?
        }
        Anchor::Class(class_sid) => {
            let Some(class) = class_stats(ctx, class_sid) else {
                return Ok(None);
            };
            class.count
        }
    };
    if subjects == 0 {
        // Empty universe: leave empty-input aggregate identities
        // (COUNT 0 / unbound MIN) to the general pipeline.
        return Ok(None);
    }

    let accessor = match &plan.accessor {
        Some(pred) => {
            let pred_sid = normalize_pred_sid(store, pred)?;
            let Some(p_id) = store.sid_to_p_id(&pred_sid) else {
                // Absent predicate: all-null accessor rows; the
                // fallback computes the null-heavy identities.
                return Ok(None);
            };
            let rows = count_rows_for_predicate_psot(store, ctx.binary_g_id, p_id)?;
            // Class anchor: property-derived folds are predicate-
            // scoped, so every P-bearing subject must be an instance
            // of the class. Equality of the per-(class, property)
            // flake count with the predicate's total row count proves
            // exactly that (both current-state-exact at HEAD).
            if let Anchor::Class(class_sid) = &plan.anchor {
                let Some(class) = class_stats(ctx, class_sid) else {
                    return Ok(None);
                };
                let class_prop_rows: u64 = class
                    .properties
                    .iter()
                    .find(|p| p.property_sid == pred_sid)
                    .map(|p| p.datatypes.iter().map(|&(_, c)| c).sum())
                    .unwrap_or(0);
                if class_prop_rows != rows {
                    return Ok(None);
                }
            }
            let Some(with_prop) =
                count_distinct_subjects_for_predicate(store, ctx.binary_g_id, p_id)?
            else {
                return Ok(None);
            };
            Some(AccessorCounts {
                pred: pred.clone(),
                p_id,
                rows,
                subjects_with: with_prop,
            })
        }
        None => None,
    };

    let batch = match &plan.kind {
        FoldKind::Scalars(tasks) => {
            let mut row = Vec::with_capacity(tasks.len());
            for task in tasks {
                let Some(binding) =
                    compute_task(*task, store, ctx.binary_g_id, subjects, accessor.as_ref())?
                else {
                    return Ok(None);
                };
                row.push(binding);
            }
            Batch::single_row(schema.clone(), row)
                .map_err(|e| QueryError::execution(format!("whole-graph agg batch: {e}")))?
        }
        FoldKind::Histogram {
            prop_col,
            count_col,
            filter,
        } => {
            let Some(a) = accessor.as_ref() else {
                return Ok(None);
            };
            let Some(b) = compute_histogram(
                ctx,
                store,
                a,
                subjects,
                schema,
                *prop_col,
                *count_col,
                filter.as_ref(),
            )?
            else {
                return Ok(None);
            };
            b
        }
    };
    Ok(Some(batch))
}

// ===========================================================================
// Overlay lane
// ===========================================================================
//
// The clean lane reads only index directories, which are exact at the persisted
// index `max_t`. Once uncommitted novelty is present, those directory counts go
// stale and the whole operator would otherwise decline to a full-graph flake
// scan (linear in graph size — catastrophic at scale for a single count). The
// overlay lane keeps the O(directory) base counts and reconciles them with a
// bounded O(novelty) pass: the subject universe via base lead-groups ± the
// overlay's net subject delta, and every predicate-scoped fold via an
// overlay-merging cursor (linear in the *predicate's* rows, not the graph's).

/// Whether the overlay-aware lane may run: a single-ledger, policy-cleared read
/// carrying novelty (the exact condition that makes the directory-only
/// [`compute_clean`] lane stale). Pure historical reads (`to_t < max_t` with no
/// novelty) are out of scope and fall through to the general pipeline.
fn overlay_lane_eligible(ctx: &crate::context::ExecutionContext<'_>) -> bool {
    ctx.binary_store.is_some()
        && !ctx.is_multi_ledger()
        && ctx.from_t.is_none()
        && ctx.allow_unfiltered()
        && crate::fast_path_common::overlay_has_novelty(ctx)
}

/// Overlay-aware fold: base directory counts reconciled with a bounded pass over
/// the novelty overlay. Only the scalar shape over the whole-graph anchor is
/// reconciled today; class anchors and histograms decline to the general
/// pipeline.
fn compute_overlay(
    ctx: &crate::context::ExecutionContext<'_>,
    store: &Arc<BinaryIndexStore>,
    plan: &WholeGraphAggPlan,
    schema: &Arc<[VarId]>,
) -> Result<Option<Batch>> {
    let FoldKind::Scalars(tasks) = &plan.kind else {
        return Ok(None); // histograms are not yet overlay-aware
    };
    // Class-anchored universes need overlay-aware class membership; only the
    // whole-graph anchor is reconciled today.
    let Anchor::AllSubjects = &plan.anchor else {
        return Ok(None);
    };

    let Some(subjects) = overlay_all_subjects_count(ctx, store)? else {
        return Ok(None);
    };
    if subjects == 0 {
        return Ok(None);
    }

    let accessor = match &plan.accessor {
        Some(pred) => match overlay_accessor_counts(ctx, store, pred)? {
            Some(a) => Some(a),
            None => return Ok(None),
        },
        None => None,
    };

    let mut row = Vec::with_capacity(tasks.len());
    for task in tasks {
        let Some(binding) = compute_task_overlay(*task, ctx, store, subjects, accessor.as_ref())?
        else {
            return Ok(None);
        };
        row.push(binding);
    }
    let batch = Batch::single_row(schema.clone(), row)
        .map_err(|e| QueryError::execution(format!("whole-graph agg overlay batch: {e}")))?;
    Ok(Some(batch))
}

/// Distinct pipeline-visible subjects at `to_t`: the base SPOT lead-group count
/// (exact at `max_t`) plus the net subject delta from the novelty overlay.
///
/// Declines (`Ok(None)`) when the fold cannot stay exact: scan-hidden predicates
/// in the base or the overlay (invisible to `?n ?p ?o`), an overlay flake that
/// fails ID translation, or an ambiguous retraction-only subject that may empty
/// a base subject (a possible node deletion — the general pipeline counts it
/// exactly).
fn overlay_all_subjects_count(
    ctx: &crate::context::ExecutionContext<'_>,
    store: &Arc<BinaryIndexStore>,
) -> Result<Option<u64>> {
    use fluree_db_binary_index::read::types::{resolve_overlay_ops, sort_overlay_ops, OverlayOp};
    use std::collections::HashMap;

    if graph_has_scan_hidden_predicates(ctx, store)? {
        return Ok(None);
    }
    let g_id = ctx.binary_g_id;
    let base = count_distinct_lead_groups(store, g_id, RunSortOrder::Spot, 8)?;

    // Translate every novelty flake (all predicates) to V3 id space.
    let dn = ctx.dict_novelty.clone().unwrap_or_else(|| {
        Arc::new(fluree_db_core::dict_novelty::DictNovelty::new_uninitialized())
    });
    let mut ephemeral_preds: HashMap<Sid, u32> = HashMap::new();
    let mut next_ep = store.predicate_count();
    let mut ops: Vec<OverlayOp> = Vec::new();
    let mut declined = false;
    ctx.overlay().for_each_overlay_flake(
        g_id,
        crate::binary_scan::sort_order_to_index_type(RunSortOrder::Spot),
        None,
        None,
        true,
        ctx.to_t,
        &mut |flake| {
            if declined {
                return;
            }
            // Mirror the pipeline's `?n ?p ?o` visibility: `f:reifies*` anywhere
            // and the `f:` namespace in the default graph are invisible to the
            // scan but present in SPOT — their subjects must not be counted.
            if fluree_db_core::is_reserved_reifies_predicate(&flake.p)
                || (g_id == 0 && flake.p.namespace_code == fluree_vocab::namespaces::FLUREE_DB)
            {
                declined = true;
                return;
            }
            match crate::binary_scan::translate_one_flake_v3_pub(
                flake,
                store,
                Some(&dn),
                ctx.runtime_small_dicts,
                &mut ephemeral_preds,
                &mut next_ep,
                g_id,
            ) {
                Ok(op) => ops.push(op),
                Err(_) => declined = true,
            }
        },
    );
    if declined {
        return Ok(None);
    }
    if ops.is_empty() {
        return Ok(Some(base));
    }

    sort_overlay_ops(&mut ops, RunSortOrder::Spot);
    resolve_overlay_ops(&mut ops);

    // Per touched subject: does a surviving assertion remain after resolution?
    let mut has_assert: HashMap<u64, bool> = HashMap::new();
    for op in &ops {
        let e = has_assert.entry(op.s_id).or_insert(false);
        *e |= op.op;
    }

    // Base-row existence for every touched subject (one galloping SPOT pass at
    // the base index t). Novelty-only subjects simply have no base entry.
    let mut touched: Vec<u64> = has_assert.keys().copied().collect();
    touched.sort_unstable();
    let base_rows = fluree_db_binary_index::batched_lookup_subject_properties(
        store,
        g_id,
        &touched,
        store.max_t(),
    )
    .map_err(|e| QueryError::Internal(format!("subject-existence lookup: {e}")))?;

    let mut delta: i64 = 0;
    for (s_id, assert) in &has_assert {
        let base_exists = base_rows.get(s_id).is_some_and(|v| !v.is_empty());
        if *assert {
            // A surviving assertion means the subject exists after the overlay:
            // +1 if it is new to the base, 0 if it was already present.
            delta += 1 - base_exists as i64;
        } else if base_exists {
            // Retraction-only over an existing subject: whether it survives
            // depends on which base rows were retracted (an `o_i`-precise
            // question). Decline this rare (deletion) shape to the exact
            // pipeline rather than risk a miscount.
            return Ok(None);
        }
        // else: retraction-only over a subject with no base rows ⇒ Δ 0.
    }

    Ok(Some((base as i64 + delta).max(0) as u64))
}

/// Overlay-merged accessor predicate counts: live row count and distinct
/// subjects carrying the predicate, from one PSOT overlay cursor pass (linear
/// in the predicate's rows). Declines for a predicate absent from the persisted
/// dictionary (possibly novelty-only).
struct OverlayAccessorCounts {
    pred: Ref,
    pred_sid: Sid,
    p_id: u32,
    rows: u64,
    subjects_with: u64,
}

fn overlay_accessor_counts(
    ctx: &crate::context::ExecutionContext<'_>,
    store: &Arc<BinaryIndexStore>,
    pred: &Ref,
) -> Result<Option<OverlayAccessorCounts>> {
    let pred_sid = normalize_pred_sid(store, pred)?;
    let Some(p_id) = store.sid_to_p_id(&pred_sid) else {
        return Ok(None);
    };
    let Some(mut cursor) = crate::fast_path_common::build_psot_cursor_for_predicate(
        ctx,
        store,
        ctx.binary_g_id,
        pred_sid.clone(),
        p_id,
        crate::fast_path_common::projection_sid_only(),
    )?
    else {
        return Ok(None);
    };

    // PSOT orders rows `(p_id, s_id, …)`, so a subject's rows are contiguous:
    // a running `prev` counts distinct subjects without a set.
    let mut rows: u64 = 0;
    let mut subjects_with: u64 = 0;
    let mut prev: Option<u64> = None;
    while let Some(batch) = cursor
        .next_batch()
        .map_err(|e| QueryError::Internal(format!("accessor PSOT cursor: {e}")))?
    {
        for row in 0..batch.row_count {
            let s_id = batch.s_id.get(row);
            rows += 1;
            if prev != Some(s_id) {
                subjects_with += 1;
                prev = Some(s_id);
            }
        }
    }
    Ok(Some(OverlayAccessorCounts {
        pred: pred.clone(),
        pred_sid,
        p_id,
        rows,
        subjects_with,
    }))
}

fn compute_task_overlay(
    task: AggTask,
    ctx: &crate::context::ExecutionContext<'_>,
    store: &Arc<BinaryIndexStore>,
    subjects: u64,
    accessor: Option<&OverlayAccessorCounts>,
) -> Result<Option<Binding>> {
    if task.needs_accessor() && accessor.is_none() {
        return Ok(None);
    }
    let int = |v: u64| -> Result<Option<Binding>> {
        Ok(Some(Binding::lit(
            FlakeValue::Long(count_to_i64(v, "whole-graph aggregate")?),
            Sid::xsd_integer(),
        )))
    };
    match task {
        AggTask::CountRows => match accessor {
            Some(a) => int(subjects
                .saturating_sub(a.subjects_with)
                .saturating_add(a.rows)),
            None => int(subjects),
        },
        AggTask::CountSubjects => int(subjects),
        AggTask::CountProp => int(accessor.unwrap().rows),
        AggTask::CountDistinctProp => {
            let a = accessor.unwrap();
            match crate::fast_predicate_scalar_agg::scan_predicate_scalar_agg_overlay(
                ctx,
                store,
                ctx.binary_g_id,
                &a.pred,
                ScalarAggKind::CountDistinctObject,
            )? {
                Some(out) => Ok(Some(out.into_binding())),
                None => Ok(None),
            }
        }
        AggTask::MinProp | AggTask::MaxProp => {
            let a = accessor.unwrap();
            let mode = if task == AggTask::MinProp {
                MinMaxMode::Min
            } else {
                MinMaxMode::Max
            };
            overlay_minmax_numeric(ctx, store, &a.pred_sid, a.p_id, mode)
        }
        AggTask::AvgProp | AggTask::SumProp => {
            let a = accessor.unwrap();
            let kind = if task == AggTask::AvgProp {
                ScalarAggKind::AvgNumeric
            } else {
                ScalarAggKind::Sum(SumExprI64::Identity)
            };
            match crate::fast_predicate_scalar_agg::scan_predicate_scalar_agg_overlay(
                ctx,
                store,
                ctx.binary_g_id,
                &a.pred,
                kind,
            )? {
                Some(out) => Ok(Some(out.into_binding())),
                None => Ok(None),
            }
        }
    }
}

/// Overlay-merged MIN/MAX over a homogeneous numeric predicate: scan the POST
/// overlay cursor and keep the extreme `o_key` (order-preserving within one
/// numeric `o_type`). Declines on any non-numeric or mixed `o_type` (mirroring
/// the clean numeric-first path) and on an empty input (the fallback yields the
/// unbound identity).
fn overlay_minmax_numeric(
    ctx: &crate::context::ExecutionContext<'_>,
    store: &Arc<BinaryIndexStore>,
    pred_sid: &Sid,
    p_id: u32,
    mode: MinMaxMode,
) -> Result<Option<Binding>> {
    let Some(mut cursor) = crate::fast_path_common::build_post_cursor_for_predicate(
        ctx,
        store,
        ctx.binary_g_id,
        pred_sid.clone(),
        p_id,
        crate::fast_path_common::projection_sid_otype_okey(),
    )?
    else {
        return Ok(None);
    };

    let mut best: Option<(u16, u64)> = None;
    while let Some(batch) = cursor
        .next_batch()
        .map_err(|e| QueryError::Internal(format!("minmax POST cursor: {e}")))?
    {
        for row in 0..batch.row_count {
            let o_type = batch.o_type.get_or(row, 0);
            if !fluree_db_core::o_type::OType::from_u16(o_type).is_numeric() {
                return Ok(None);
            }
            let o_key = batch.o_key.get(row);
            match best {
                None => best = Some((o_type, o_key)),
                Some((bt, bk)) => {
                    if bt != o_type {
                        return Ok(None);
                    }
                    let better = match mode {
                        MinMaxMode::Min => o_key < bk,
                        MinMaxMode::Max => o_key > bk,
                    };
                    if better {
                        best = Some((o_type, o_key));
                    }
                }
            }
        }
    }

    Ok(best
        .map(|(ot, ok)| crate::fast_min_max_string::numeric_binding_from_otype_okey(store, ot, ok)))
}

/// The per-graph class stats entry for `class_sid`, if present.
fn class_stats<'a>(
    ctx: &'a crate::context::ExecutionContext<'_>,
    class_sid: &Sid,
) -> Option<&'a fluree_db_core::index_stats::ClassStatEntry> {
    ctx.active_snapshot
        .stats
        .as_ref()?
        .graphs
        .as_ref()?
        .iter()
        .find(|g| g.g_id == ctx.binary_g_id)?
        .classes
        .as_ref()?
        .iter()
        .find(|c| &c.class_sid == class_sid)
}

/// `GROUP BY ?prop COUNT(*)`: per-value counts from a POST run-length pass
/// (values are contiguous in POST order) plus one null-group row for
/// subjects without the property. Declines on very wide histograms and on
/// stores the group-count walk cannot serve.
#[allow(clippy::too_many_arguments)]
fn compute_histogram(
    ctx: &crate::context::ExecutionContext<'_>,
    store: &Arc<BinaryIndexStore>,
    accessor: &AccessorCounts,
    subjects: u64,
    schema: &Arc<[VarId]>,
    prop_col: usize,
    count_col: usize,
    filter: Option<&Expression>,
) -> Result<Option<Batch>> {
    // Bound the output (one batch) and guarantee the top-K walk keeps every
    // group: the directory-level distinct-object count must fit the cap.
    match count_distinct_objects_for_predicate(store, ctx.binary_g_id, accessor.p_id)? {
        Some(distinct) if (distinct as usize) <= HISTOGRAM_MAX_GROUPS => {}
        _ => return Ok(None),
    }
    let Ok(groups) = crate::fast_group_count_firsts::group_count_v6(
        store,
        ctx.binary_g_id,
        &accessor.pred,
        HISTOGRAM_MAX_GROUPS,
        &ctx.cancellation,
    ) else {
        return Ok(None);
    };

    let view = fluree_db_binary_index::BinaryGraphView::with_novelty(
        Arc::clone(store),
        ctx.binary_g_id,
        ctx.dict_novelty.clone(),
    )
    .with_namespace_codes_fallback(ctx.namespace_codes_fallback.clone());

    let null_rows = subjects.saturating_sub(accessor.subjects_with);
    let mut col_prop: Vec<Binding> = Vec::with_capacity(groups.len() + 1);
    let mut col_count: Vec<Binding> = Vec::with_capacity(groups.len() + 1);
    for (o_type, o_key, count) in groups {
        if o_type == fluree_db_core::o_type::OType::IRI_REF.as_u16() {
            col_prop.push(Binding::encoded_sid(o_key));
        } else {
            let val = view
                .decode_value(o_type, o_key, accessor.p_id)
                .map_err(|e| QueryError::Internal(format!("histogram decode: {e}")))?;
            let dt = store
                .resolve_datatype_sid_for_value(o_type, &val)
                .unwrap_or_else(|| Sid::new(0, ""));
            let dtc = match store.resolve_lang_tag(o_type) {
                Some(tag) => fluree_db_core::DatatypeConstraint::LangTag(Arc::from(tag)),
                None => fluree_db_core::DatatypeConstraint::Explicit(dt),
            };
            col_prop.push(Binding::Lit {
                val,
                dtc,
                t: None,
                op: None,
                p_id: None,
            });
        }
        col_count.push(Binding::lit(FlakeValue::Long(count), Sid::xsd_integer()));
    }
    if null_rows > 0 {
        col_prop.push(Binding::Unbound);
        col_count.push(Binding::lit(
            FlakeValue::Long(count_to_i64(null_rows, "histogram null group")?),
            Sid::xsd_integer(),
        ));
    }

    let mut cols: Vec<Vec<Binding>> = vec![Vec::new(), Vec::new()];
    cols[prop_col] = col_prop;
    cols[count_col] = col_count;
    let batch = Batch::new(schema.clone(), cols)
        .map_err(|e| QueryError::execution(format!("histogram batch: {e}")))?;

    // A group-key filter drops whole groups; evaluating it per group row
    // (including the null group's Unbound key) is exactly the pipeline's
    // per-row semantics, because every row of a group carries the same key.
    let Some(filter) = filter else {
        return Ok(Some(batch));
    };
    let prepared = crate::eval::PreparedBoolExpression::new(filter.clone());
    match crate::filter::filter_batch(&batch, &prepared, schema, ctx)? {
        Some(filtered) => Ok(Some(filtered)),
        // Every group rejected: a legitimately empty result, not a decline.
        None => Ok(Some(crate::fast_path_common::empty_batch(schema.clone())?)),
    }
}

struct AccessorCounts {
    pred: Ref,
    p_id: u32,
    rows: u64,
    subjects_with: u64,
}

fn compute_task(
    task: AggTask,
    store: &Arc<BinaryIndexStore>,
    g_id: GraphId,
    subjects: u64,
    accessor: Option<&AccessorCounts>,
) -> Result<Option<Binding>> {
    if task.needs_accessor() && accessor.is_none() {
        return Ok(None);
    }
    let int = |v: u64| -> Result<Option<Binding>> {
        Ok(Some(Binding::lit(
            FlakeValue::Long(count_to_i64(v, "whole-graph aggregate")?),
            Sid::xsd_integer(),
        )))
    };
    match task {
        AggTask::CountRows => match accessor {
            // Left join: subjects without P keep 1 row, subjects with P get
            // one row per value.
            Some(a) => int(subjects
                .saturating_sub(a.subjects_with)
                .saturating_add(a.rows)),
            None => int(subjects),
        },
        AggTask::CountSubjects => int(subjects),
        AggTask::CountProp => int(accessor.unwrap().rows),
        AggTask::CountDistinctProp => {
            match count_distinct_objects_for_predicate(store, g_id, accessor.unwrap().p_id)? {
                Some(distinct) => int(distinct),
                None => Ok(None),
            }
        }
        AggTask::MinProp | AggTask::MaxProp => {
            let mode = if task == AggTask::MinProp {
                MinMaxMode::Min
            } else {
                MinMaxMode::Max
            };
            let p_id = accessor.unwrap().p_id;
            if let Some(b) = minmax_numeric_post(store, g_id, p_id, mode)? {
                return Ok(Some(b));
            }
            minmax_string_dict_post(store, g_id, p_id, mode)
        }
        AggTask::AvgProp | AggTask::SumProp => {
            let kind = if task == AggTask::AvgProp {
                ScalarAggKind::AvgNumeric
            } else {
                ScalarAggKind::Sum(SumExprI64::Identity)
            };
            match scan_predicate_scalar_agg(store, g_id, &accessor.unwrap().pred, kind)? {
                Some(output) => Ok(Some(output.into_binding())),
                None => Ok(None),
            }
        }
    }
}

/// Whether the queried graph carries rows under any predicate the
/// variable-predicate scan hides — `f:reifies*` in every graph, the broader
/// `f:` namespace in the default graph (mirrors
/// `BinaryScanOperator::is_internal_predicate`). Such facts are invisible to
/// the `?n ?p ?o` pipeline but present in the SPOT directories this fold
/// reads, so their presence makes the fold inexact.
///
/// Two deliberate choices:
/// - Candidates come from the store's predicate **dictionary**, not the
///   per-graph stats: the incremental index build can persist delta-only
///   per-graph property stats (base entries lost), so a stats-driven check
///   can silently pass on a graph that does carry hidden facts.
/// - Each hidden candidate is confirmed by its **row count in the queried
///   graph** (`count_rows_for_predicate_psot`, directory-only): the
///   dictionary is global across graphs, so commit-metadata `f:` predicates
///   from the txn-meta graph are always present in it — bare membership
///   would permanently decline every ledger.
pub(crate) fn graph_has_scan_hidden_predicates(
    ctx: &crate::context::ExecutionContext<'_>,
    store: &BinaryIndexStore,
) -> Result<bool> {
    for p_id in 0..store.predicate_count() {
        let Some(sid) = store.predicate_sid(p_id) else {
            // Unresolvable dictionary entry — err toward declining.
            return Ok(true);
        };
        let hidden = fluree_db_core::is_reserved_reifies_predicate(&sid)
            || (ctx.binary_g_id == 0 && sid.namespace_code == fluree_vocab::namespaces::FLUREE_DB);
        if hidden && count_rows_for_predicate_psot(store, ctx.binary_g_id, p_id)? > 0 {
            return Ok(true);
        }
    }
    Ok(false)
}
