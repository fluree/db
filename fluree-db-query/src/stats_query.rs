//! Fast-path operator for per-predicate count queries.
//!
//! Answers `SELECT ?p (COUNT(?s) AS ?c) WHERE { ?s ?p ?o } GROUP BY ?p`
//! with **exact** counts read from POST leaf-directory metadata —
//! `O(leaflets)` directory reads, decoding only the rare mixed-predicate
//! leaflet — instead of scanning all triples.
//!
//! ## Why not answer from `IndexStats` / `StatsView`?
//!
//! A previous version of this operator returned `StatsView` property counts
//! directly. The differential harness
//! (`fluree-db-api/tests/it_differential_fastpath.rs`, FD-3) showed those
//! numbers are planner *estimates*, not current-state fact counts: they
//! count duplicate re-asserts that set-semantics novelty application dedups,
//! track `rdf:type` asymmetrically between indexer-built and
//! novelty-accumulated stats, and apply novelty deltas inconsistently per
//! predicate. Selectivity estimation tolerates all of that; an exact `COUNT`
//! answer does not.
//!
//! V3 leaflets store current-state rows only (history lives in the sidecar),
//! so per-predicate row counts from leaf directories are exact — but only
//! for the persisted index, which is why this path is gated by
//! [`fast_path_store`] (binary store present, no overlay novelty, query at
//! the store's `max_t`, root policy, single ledger). Anything else returns
//! `Ok(None)` and the planned fallback (the generic scan + group + count
//! pipeline, which applies per-flake policy filtering and honors novelty)
//! runs instead.
//!
//! ## Over a default graph of several graphs
//!
//! A default graph of two or more graphs of one ledger is the set union of
//! their facts, a fact being storage's identity: one per value and list
//! position (AJ-66). A predicate's count there is its number of distinct
//! facts over the members. [`count_by_predicate_union_operator`] reads every
//! member's POST directory as above. A predicate one member holds takes that
//! member's count: one graph's rows are distinct facts. A predicate two or
//! more members hold is counted by merging its PSOT rows across them
//! ([`MergedRangeCursor`]), which drops each fact they share. A predicate
//! some member holds big numbers for is counted through a set instead, a big
//! number keyed by its value: its encoded key is a handle into an arena per
//! graph and predicate, so equal keys in two members need not be one value.
//! Vectors and spatial values decline. The planner picks this operator for a
//! scope of several graphs or of none; it stamps its own site
//! ([`STATS_COUNT_BY_PREDICATE_UNION_SITE`]) and runs only when the index
//! alone answers for every member.

use crate::binding::{Batch, Binding};
use crate::context::WellKnownDatatypes;
use crate::error::{QueryError, Result};
use crate::fast_path_common::{build_range_cursor, predicate_range_keys, FastPathOperator};
use crate::fast_path_outcome::{stamp_fast_path, FastPathFallback, FastPathOutcome};
use crate::index_scope::{IndexScope, MemberSet};
use crate::merged_cursor::{is_graph_scoped_arena, MergeDecline, MergedRangeCursor};
use crate::operator::BoxedOperator;
use crate::var_registry::VarId;
use fluree_db_binary_index::arena::numbig::NumBigRepr;
use fluree_db_binary_index::format::column_block::ColumnId;
use fluree_db_binary_index::format::run_record::RunSortOrder;
use fluree_db_binary_index::{BinaryFilter, BinaryIndexStore, ColumnProjection, ColumnSet};
use fluree_db_core::{FlakeValue, GraphId, OType};
use rustc_hash::FxHashSet;
use std::collections::BTreeMap;
use std::sync::Arc;

/// `fast-path outcome` site of the per-predicate count over a default graph
/// of several graphs, or of none: the one-graph site's name with `_union`, so
/// a test can tell which of the two answered.
pub const STATS_COUNT_BY_PREDICATE_UNION_SITE: &str = "stats_count_by_predicate_union";

/// Build the per-predicate count operator. Emits `(pred, count)` rows from
/// exact POST leaf-directory metadata when the fast-path gate holds; defers
/// to `fallback` (the generic GROUP BY pipeline) otherwise.
///
/// Because the exact count is only valid for the persisted index at HEAD
/// under a root policy, the gate ([`fast_path_store`]) also discharges the
/// old policy-leak concern: under any non-root view policy the gate declines
/// and the policy-enforced fallback runs, so restricted predicates never
/// leak their existence or cardinality.
pub fn stats_count_by_predicate_operator(
    pred_var: VarId,
    count_var: VarId,
    fallback: Option<BoxedOperator>,
) -> FastPathOperator {
    let schema: Arc<[VarId]> = Arc::from(vec![pred_var, count_var].into_boxed_slice());
    let batch_schema = schema.clone();
    FastPathOperator::with_schema(
        schema,
        move |ctx| {
            let Some(IndexScope::One(member)) = ctx.index_scope() else {
                return Ok(None);
            };
            if !member.index_is_current() {
                return Ok(None);
            }
            let store = member.store();
            let Some(counts) = exact_count_by_predicate_post(store, member.g_id())? else {
                return Ok(None);
            };
            predicate_count_batch(store, &batch_schema, counts.into_iter())
        },
        fallback,
        "COUNT by predicate (directory)",
    )
}

/// Build the per-predicate count operator for a default graph of several
/// graphs of one ledger, or of none (see the module docs). Defers to
/// `fallback` when the scope is neither, when some member's index does not
/// answer for it alone, or when a predicate's rows cannot be merged exactly;
/// each decline over several graphs stamps its reason at
/// [`STATS_COUNT_BY_PREDICATE_UNION_SITE`].
pub fn count_by_predicate_union_operator(
    pred_var: VarId,
    count_var: VarId,
    fallback: Option<BoxedOperator>,
) -> FastPathOperator {
    let schema: Arc<[VarId]> = Arc::from(vec![pred_var, count_var].into_boxed_slice());
    let batch_schema = schema.clone();
    FastPathOperator::with_schema(
        schema,
        move |ctx| match ctx.index_scope() {
            Some(IndexScope::Union(set)) if !set.all_current() => {
                Ok(union_declined(FastPathFallback::Novelty))
            }
            Some(IndexScope::Union(set)) => match count_by_predicate_union(&set)? {
                Ok(counts) => predicate_count_batch(set.store(), &batch_schema, counts.into_iter()),
                Err(reason) => Ok(union_declined(reason)),
            },
            // A default graph of none holds no predicate.
            Some(IndexScope::Empty) => Batch::empty(batch_schema.clone())
                .map(Some)
                .map_err(|e| QueryError::execution(format!("stats count batch: {e}"))),
            _ => Ok(None),
        },
        fallback,
        STATS_COUNT_BY_PREDICATE_UNION_SITE,
    )
}

/// Stamp why the count over several graphs declined; the operator then runs
/// its fallback.
fn union_declined(reason: FastPathFallback) -> Option<Batch> {
    stamp_fast_path(
        STATS_COUNT_BY_PREDICATE_UNION_SITE,
        FastPathOutcome::Fallback(reason),
    );
    None
}

/// The `(predicate, count)` rows, or `None` when a predicate id does not
/// resolve.
fn predicate_count_batch(
    store: &BinaryIndexStore,
    schema: &Arc<[VarId]>,
    counts: impl ExactSizeIterator<Item = (u32, i64)>,
) -> Result<Option<Batch>> {
    // SPARQL COUNT yields xsd:integer (§18.5.1.6); the materialized and
    // streaming aggregate paths already tag it so — this per-predicate
    // fast-path must match or a COUNT re-typed as xsd:long leaks (agg02).
    let dt = WellKnownDatatypes::new().xsd_integer;
    let mut pred_col: Vec<Binding> = Vec::with_capacity(counts.len());
    let mut count_col: Vec<Binding> = Vec::with_capacity(counts.len());
    for (p_id, count) in counts {
        // Directory p_ids are persisted-store IDs by construction; an
        // unresolvable one means the store and its directories
        // disagree — fall back rather than emit a partial result.
        let Some(iri) = store.resolve_predicate_iri(p_id) else {
            return Ok(None);
        };
        pred_col.push(Binding::sid(store.encode_iri(iri)));
        count_col.push(Binding::lit(FlakeValue::Long(count), dt.clone()));
    }
    let batch = Batch::new(schema.clone(), vec![pred_col, count_col])
        .map_err(|e| QueryError::execution(format!("stats count batch: {e}")))?;
    Ok(Some(batch))
}

/// Exact per-predicate counts over the union of `set`'s members, or why they
/// cannot be had from the index.
fn count_by_predicate_union(
    set: &MemberSet<'_>,
) -> Result<std::result::Result<Vec<(u32, i64)>, FastPathFallback>> {
    let store = set.store();
    let mut held: BTreeMap<u32, Holders> = BTreeMap::new();
    for member in set.members() {
        let Some(rows) = exact_count_by_predicate_post(store, member.g_id())? else {
            return Ok(Err(FastPathFallback::GateDeclined));
        };
        for (p_id, rows) in rows {
            let holders = held.entry(p_id).or_default();
            holders.rows += rows;
            holders.members += 1;
        }
        // Arenas are loaded whole at store open: this reads no storage.
        for (p_id, arena) in store.numbig_arenas(member.g_id()) {
            if let Some(holders) = held.get_mut(&p_id) {
                holders.numbig = true;
                holders.numbig_duplicates |= arena.has_duplicate_handles();
            }
        }
    }
    let mut counts = Vec::with_capacity(held.len());
    for (p_id, holders) in held {
        // One member's rows of one predicate are distinct facts, unless a
        // legacy arena gave one value two handles.
        let count = if holders.members == 1 && !holders.numbig_duplicates {
            Ok(Ok(holders.rows))
        } else if holders.numbig {
            distinct_facts_by_value(set, p_id)
        } else {
            distinct_facts(set, p_id).map(|merged| merged.map_err(|decline| decline.fallback()))
        };
        match count {
            Ok(Ok(count)) => counts.push((p_id, count)),
            Ok(Err(reason)) => return Ok(Err(reason)),
            Err(e) => {
                tracing::warn!(error = %e, "count by predicate over several graphs: read failed; declining to generic scan");
                return Ok(Err(FastPathFallback::GateDeclined));
            }
        }
    }
    Ok(Ok(counts))
}

/// One predicate across the members of a union: what decides how its count
/// is had.
#[derive(Debug, Default)]
struct Holders {
    /// Its rows, summed over the members.
    rows: i64,
    /// How many members hold it.
    members: usize,
    /// Some member holding it has a big-number arena for it.
    numbig: bool,
    /// Some such arena gives one value two handles (written before arenas
    /// normalized their values).
    numbig_duplicates: bool,
}

/// The number of distinct facts of predicate `p_id` over `set`'s members:
/// its PSOT rows merged, where the members' copies of one fact are adjacent.
fn distinct_facts(
    set: &MemberSet<'_>,
    p_id: u32,
) -> Result<std::result::Result<i64, MergeDecline>> {
    // The range keys' graph is not compared; each member reads its own.
    let (min, max) = predicate_range_keys(p_id, 0);
    let count_only = ColumnProjection {
        output: ColumnSet::EMPTY,
        internal: ColumnSet::EMPTY,
    };
    let mut merged = match MergedRangeCursor::new(
        set,
        RunSortOrder::Psot,
        &min,
        &max,
        BinaryFilter::default(),
        count_only,
    ) {
        Ok(merged) => merged,
        Err(decline) => return Ok(Err(decline)),
    };
    let mut count = 0i64;
    loop {
        match merged.next_batch()? {
            Ok(Some(batch)) => count += batch.row_count as i64,
            Ok(None) => return Ok(Ok(count)),
            Err(decline) => return Ok(Err(decline)),
        }
    }
}

/// The number of distinct facts of predicate `p_id` over `set`'s members,
/// for a predicate some member holds big numbers for. A big number's `o_key`
/// is a handle into its graph's arena for the predicate, so two members'
/// handles for one value differ, and one handle can name two values: each
/// member's rows go into one set of facts `(s, o_type, object, list
/// position)`, a big number's object its value's normalized repr (one value
/// iff equal reprs, across arenas; a decimal and an integer of equal digits
/// differ in it), any other object its `o_key`. Vectors and spatial values,
/// whose arenas have no such key, decline.
fn distinct_facts_by_value(
    set: &MemberSet<'_>,
    p_id: u32,
) -> Result<std::result::Result<i64, FastPathFallback>> {
    #[derive(PartialEq, Eq, Hash)]
    enum Object {
        Encoded(u64),
        Big(NumBigRepr),
    }
    let store = set.store();
    // The range keys' graph is not compared; each member reads its own.
    let (min, max) = predicate_range_keys(p_id, 0);
    let mut columns = ColumnSet::single(ColumnId::SId);
    columns.insert(ColumnId::OType);
    columns.insert(ColumnId::OKey);
    columns.insert(ColumnId::OI);
    let projection = ColumnProjection {
        output: columns,
        internal: ColumnSet::EMPTY,
    };
    let filter = BinaryFilter {
        p_id: Some(p_id),
        ..BinaryFilter::default()
    };
    let numbig = OType::NUM_BIG_OVERFLOW.as_u16();
    // A fact: subject, object type, object, list position.
    let mut facts: FxHashSet<(u64, u16, Object, u32)> = FxHashSet::default();
    for member in set.members() {
        let arena = store
            .numbig_arenas(member.g_id())
            .find_map(|(p, arena)| (p == p_id).then_some(arena));
        let Some(mut cursor) = build_range_cursor(
            store,
            member.g_id(),
            RunSortOrder::Psot,
            &min,
            &max,
            filter.clone(),
            projection,
        ) else {
            return Ok(Err(FastPathFallback::GateDeclined));
        };
        cursor.set_to_t(member.to_t());
        while let Some(batch) = cursor
            .next_batch()
            .map_err(|e| QueryError::from_io("count by predicate", e))?
        {
            for row in 0..batch.row_count {
                let o_type = batch.o_type.get(row);
                let o_key = batch.o_key.get(row);
                let object = if o_type == numbig {
                    let Some(value) = arena.and_then(|a| a.get_by_handle(o_key as u32)) else {
                        return Ok(Err(FastPathFallback::GateDeclined));
                    };
                    Object::Big(value.normalized_repr())
                } else if is_graph_scoped_arena(o_type) {
                    return Ok(Err(FastPathFallback::ArenaType));
                } else {
                    Object::Encoded(o_key)
                };
                let o_i = batch.o_i.get_or(row, u32::MAX);
                facts.insert((batch.s_id.get(row), o_type, object, o_i));
            }
        }
    }
    Ok(Ok(facts.len() as i64))
}

/// Exact per-predicate row counts from POST leaf directories.
///
/// Homogeneous leaflets (`p_const = Some(p)`) contribute `row_count` without
/// any payload read; mixed-predicate leaflets decode only the `PId` column.
/// Returns `Ok(None)` when the graph has no POST branch, or when a leaf read
/// fails — either way the caller declines to the generic scan (the fast-path
/// "degrade to the correct slower path" contract) rather than asserting
/// emptiness from absence or hard-failing on a transient IO fault. Declining is
/// lossless here: this function only accumulates a local map and has emitted
/// nothing, and a genuine IO fault will resurface in the generic scan.
fn exact_count_by_predicate_post(
    store: &BinaryIndexStore,
    g_id: GraphId,
) -> Result<Option<Vec<(u32, i64)>>> {
    let Some(branch) = store.branch_for_order(g_id, RunSortOrder::Post) else {
        return Ok(None);
    };

    let pid_projection = ColumnProjection {
        output: ColumnSet::EMPTY,
        internal: {
            let mut s = ColumnSet::EMPTY;
            s.insert(ColumnId::PId);
            s
        },
    };

    let mut counts: BTreeMap<u32, i64> = BTreeMap::new();
    for leaf_entry in &branch.leaves {
        let handle = match store.open_leaf_handle(
            &leaf_entry.leaf_cid,
            leaf_entry.sidecar_cid.as_ref(),
            false,
        ) {
            Ok(h) => h,
            Err(e) => {
                tracing::warn!(error = %e, "stats count-by-predicate: leaf open failed; declining to generic scan");
                return Ok(None);
            }
        };
        let dir = handle.dir();
        for (leaflet_idx, entry) in dir.entries.iter().enumerate() {
            if entry.row_count == 0 {
                continue;
            }
            if let Some(p) = entry.p_const {
                *counts.entry(p).or_insert(0) += i64::from(entry.row_count);
            } else {
                let batch = match handle.load_columns(
                    leaflet_idx,
                    &pid_projection,
                    RunSortOrder::Post,
                ) {
                    Ok(b) => b,
                    Err(e) => {
                        tracing::warn!(error = %e, "stats count-by-predicate: load columns failed; declining to generic scan");
                        return Ok(None);
                    }
                };
                for row in 0..batch.row_count {
                    *counts.entry(batch.p_id.get(row)).or_insert(0) += 1;
                }
            }
        }
    }

    Ok(Some(counts.into_iter().collect()))
}
