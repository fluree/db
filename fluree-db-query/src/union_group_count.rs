//! The grouped count of one triple pattern over a default graph of several
//! graphs of one ledger.
//!
//! `SELECT ?k (COUNT(*) AS ?c) WHERE { ?s <p> ?o } GROUP BY ?k`, with `?k`
//! the subject or the object, over a default graph of two or more graphs.
//! Over one graph the scan arrives sorted by the key and the partitioned
//! GROUP BY counts its runs. Over several, `DatasetOperator` drains the
//! members one after another and deduplicates their rows, so one key arrives
//! once per member and every row is decoded and hashed.
//!
//! Here the members' index ranges for `<p>` are merged instead
//! ([`MergedRangeCursor`]): PSOT to group by the subject, POST by the object.
//! The merge holds each fact once (storage's fact identity: one fact per value and list
//! position), the rows the general plan's union deduplication keeps, and it
//! is sorted by the key, so a group is one run of equal encoded keys. Runs
//! are counted on integer keys alone. Each run's key is then decoded once,
//! the way a member scan of the union binds it, and the runs are folded into
//! groups by the key GROUP BY uses ([`binding_to_group_key_normalized`]), so
//! the groups are the general plan's by construction.
//!
//! The merge declines a member with novelty or read below the index, a
//! member with no branch for the order, and an arena-backed object (a big
//! number, a vector or a spatial value), whose encoded key is a handle per
//! graph and predicate: equal handles in two members need not be one value,
//! and handle order is not value order. Each decline stamps its reason at
//! [`PARTITIONED_GROUP_BY_UNION_SITE`] and leaves the rows to the general
//! plan.

use std::sync::Arc;

use fluree_db_binary_index::format::column_block::ColumnId;
use fluree_db_binary_index::format::run_record::RunSortOrder;
use fluree_db_binary_index::{BinaryFilter, BinaryGraphView, ColumnProjection, ColumnSet};
use rustc_hash::FxBuildHasher;

use crate::binary_scan::BinaryScanOperator;
use crate::binding::Binding;
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::fast_path_common::predicate_range_keys;
use crate::fast_path_outcome::{stamp_fast_path, FastPathFallback, FastPathOutcome};
use crate::group_aggregate::{binding_to_group_key_normalized, GroupKeyOwned};
use crate::index_scope::MemberSet;
use crate::ir::triple::TriplePattern;
use crate::ir::{AggregateFn, Pattern, Query};
use crate::merged_cursor::MergedRangeCursor;
use crate::object_binding::{equality_norm, materialized_object_binding, EqualityNorm};
use crate::operator::CountGroup;
use crate::var_registry::VarId;

/// `fast-path outcome` site of the grouped count over a default graph of
/// several graphs, or of none: the one-graph partitioned GROUP BY's site
/// with `_union`, so a test can tell which of the two answered.
pub const PARTITIONED_GROUP_BY_UNION_SITE: &str = "partitioned_group_by_union";

/// Which position of the pattern a grouped count groups by.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupedPosition {
    Subject,
    Object,
}

/// The position of `pattern` (`?s <p> ?o`, two distinct variables, a bound
/// predicate, no datatype constraint) a grouped count by `group_vars` groups
/// by, when every variable in `bound_vars` (those a `COUNT(?v)` reads) is one
/// the pattern binds in every row.
pub(crate) fn grouped_position(
    pattern: &TriplePattern,
    group_vars: &[VarId],
    bound_vars: &[VarId],
) -> Option<GroupedPosition> {
    let (s, _, o) = crate::execute::operator_tree::validate_simple_triple(pattern)?;
    let position = match group_vars {
        [v] if *v == s => GroupedPosition::Subject,
        [v] if *v == o => GroupedPosition::Object,
        _ => return None,
    };
    bound_vars
        .iter()
        .all(|v| *v == s || *v == o)
        .then_some(position)
}

/// Stamp why the union lane stands down for `query` where SPARQL reads its
/// one pattern through its view of RDF lists (`crate::list_view`): a grouped
/// count of a list-view triple `?s <p> ?o` by `?s` or `?o`, every aggregate a
/// row count. That triple's rows are its stored rows that are not list
/// members plus the list triples, which no index range holds, so the planner
/// wraps its scan and the lane is never asked. The kill switch, where it is
/// on, is the reason given. For a plan over a default graph of several graphs
/// or of none, where the lane would otherwise run.
pub(crate) fn stamp_list_view_decline(query: &Query) {
    let (Some(grouping), [Pattern::ListView(lv)], None) = (
        query.grouping.as_ref(),
        query.patterns.as_slice(),
        query.post_values.as_ref(),
    ) else {
        return;
    };
    if grouping.aggregates().next().is_none() {
        return;
    }
    let mut bound_vars = Vec::new();
    for spec in grouping.aggregates() {
        match spec.function {
            AggregateFn::CountAll => {}
            AggregateFn::Count(v) => bound_vars.push(v),
            _ => return,
        }
    }
    let group_vars: Vec<VarId> = grouping.group_by_vars().collect();
    if grouped_position(&lv.triple, &group_vars, &bound_vars).is_none() {
        return;
    }
    let reason = if crate::execute::fast_paths_disabled() {
        FastPathFallback::KillSwitch
    } else {
        FastPathFallback::ListView
    };
    stamp_fast_path(
        PARTITIONED_GROUP_BY_UNION_SITE,
        FastPathOutcome::Fallback(reason),
    );
}

/// A grouped count one scan builder's pattern answers: every row is one fact
/// of `pattern` (`?s <p> ?o`), grouped by `position`.
pub struct UnionGroupCount<'a> {
    pub(crate) pattern: &'a TriplePattern,
    pub(crate) position: GroupedPosition,
}

/// The rows of `spec`'s pattern over `set`'s members, counted per group, or
/// why the index cannot give them exactly.
pub(crate) fn count_groups_over_union(
    ctx: &ExecutionContext<'_>,
    set: &MemberSet<'_>,
    spec: &UnionGroupCount<'_>,
) -> Result<std::result::Result<Vec<CountGroup>, FastPathFallback>> {
    if !set.all_current() {
        return Ok(Err(FastPathFallback::Novelty));
    }
    let store = set.store();
    let (_, p_sid, _) =
        BinaryScanOperator::extract_bound_terms_snapshot(ctx.active_snapshot, spec.pattern);
    let Some(p_id) = p_sid.and_then(|sid| store.sid_to_p_id(&sid)) else {
        return Ok(Err(FastPathFallback::GateDeclined));
    };
    let (order, key_columns) = match spec.position {
        GroupedPosition::Subject => (RunSortOrder::Psot, [ColumnId::SId].as_slice()),
        GroupedPosition::Object => (
            RunSortOrder::Post,
            [ColumnId::OType, ColumnId::OKey].as_slice(),
        ),
    };
    let mut output = ColumnSet::EMPTY;
    for &column in key_columns {
        output.insert(column);
    }
    let projection = ColumnProjection {
        output,
        internal: ColumnSet::EMPTY,
    };
    // The range keys' graph is not compared; each member reads its own.
    let (min, max) = predicate_range_keys(p_id, 0);
    let mut merged =
        match MergedRangeCursor::new(set, order, &min, &max, BinaryFilter::default(), projection) {
            Ok(merged) => merged.with_tracker(&ctx.tracker),
            Err(decline) => return Ok(Err(decline.fallback())),
        };
    // A graph that holds rows of `<p>`, whose view decodes the keys.
    let decode_graph = merged.members_in_range().first().copied();

    // Runs of one encoded key: `(o_type, o_key)` for the object, `(0, s_id)`
    // for the subject. The merge is sorted by the key, so each key is one run.
    let mut runs: Vec<((u16, u64), u64)> = Vec::new();
    loop {
        ctx.checkpoint()?;
        let batch = match merged.next_batch()? {
            Ok(Some(batch)) => batch,
            Ok(None) => break,
            Err(decline) => return Ok(Err(decline.fallback())),
        };
        let key_at = |row: usize| match spec.position {
            GroupedPosition::Subject => (0, batch.s_id.get(row)),
            GroupedPosition::Object => (batch.o_type.get(row), batch.o_key.get(row)),
        };
        for row in 0..batch.row_count {
            let key = key_at(row);
            match runs.last_mut() {
                Some((last, rows)) if *last == key => *rows += 1,
                _ => runs.push((key, 1)),
            }
        }
    }

    // Decode each run's key as a member scan of the union binds it (eagerly:
    // a scope of several graphs has no graph view to decode it later), and
    // fold the runs into groups by GROUP BY's own key. An arena-backed object
    // was declined above, so the view of any member that holds rows decodes
    // the same term.
    let Some(g_id) = decode_graph.filter(|_| !runs.is_empty()) else {
        return Ok(Ok(Vec::new()));
    };
    let view = BinaryGraphView::with_novelty(Arc::clone(store), g_id, ctx.dict_novelty.clone())
        .with_namespace_codes_fallback(ctx.namespace_codes_fallback.clone())
        .with_tracker(ctx.tracker.clone());
    let norm = equality_norm(ctx);
    let (dicts, gv) = EqualityNorm::parts(&norm);
    let mut groups: hashbrown::HashMap<GroupKeyOwned, CountGroup, FxBuildHasher> =
        hashbrown::HashMap::with_capacity_and_hasher(runs.len(), FxBuildHasher);
    for ((o_type, key), rows) in runs {
        let binding = match spec.position {
            GroupedPosition::Subject => Binding::sid(
                view.resolve_subject_sid(key)
                    .map_err(|e| QueryError::from_io("resolve_subject_sid", e))?,
            ),
            GroupedPosition::Object => {
                let val = view
                    .decode_value(o_type, key, p_id)
                    .map_err(|e| QueryError::from_io("decode_value", e))?;
                materialized_object_binding(store, o_type, p_id, val, None, None)
            }
        };
        let group_key = binding_to_group_key_normalized(&binding, dicts, gv)?;
        let groups_before = groups.len();
        let group = groups.entry(group_key).or_insert_with(|| CountGroup {
            keys: vec![binding],
            count: 0,
        });
        group.count = group
            .count
            .checked_add(rows)
            .ok_or_else(|| QueryError::execution("grouped COUNT(*) overflow"))?;
        if groups.len() > groups_before {
            ctx.record_alloc(crate::context::GROUP_EST_BYTES);
        }
    }
    ctx.checkpoint()?;
    Ok(Ok(groups.into_values().collect()))
}
