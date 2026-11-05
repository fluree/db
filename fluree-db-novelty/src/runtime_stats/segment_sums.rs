//! Planner stats summed from per-segment deltas.
//!
//! The planner's merge ([`super::assemble_planner_stats`]) is the blind ±1
//! delta log over novelty: a sum, so it does not depend on the order flakes
//! are visited in, beyond the zero clamp each count gets (here applied once,
//! to the sum, where the walk applies it per flake; the two differ only where
//! a running count would dip below zero). Novelty segments are immutable, so
//! each segment's sums are computed once and kept with the segment, freed
//! when it is (an index publish trims it, compaction merges it away). They
//! hold no copy of a value: a distinct-value count keys on a hash of the
//! value, and subjects share the segment's own `Sid`s. A commit adds a segment,
//! and the view the next transaction plans with sums K segment deltas instead
//! of walking every novelty flake: the walk made planning, and so every
//! transaction WHERE and every query after a commit, O(novelty).

use super::{
    build_property_deltas, counts_only_classes, finalize_properties, get_or_insert_graph_entry,
    get_or_insert_graph_property, graph_id_for_flake, graphs_without_classes,
    include_in_runtime_stats, increment_count, note_observed_tag, runtime_datatype_tag,
    StatsLookup,
};
use crate::{Novelty, Segment};
use fluree_db_core::{is_rdf_type, Flake, FlakeValue, GraphId, IndexStats, LedgerSnapshot, Sid};
use rustc_hash::{FxHashMap, FxHashSet};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};

/// A segment's planner sums, under the ndv scope they were computed for.
#[derive(Default)]
pub(crate) struct SegmentSumsSlot(Mutex<Option<(u64, Arc<SegmentSums>)>>);

/// One segment's planner sums.
#[derive(Default)]
pub(crate) struct SegmentSums {
    flakes: i64,
    graph_flakes: FxHashMap<GraphId, i64>,
    classes: FxHashMap<Sid, i64>,
    properties: FxHashMap<Sid, PropertySums>,
    graph_properties: FxHashMap<(GraphId, Sid), GraphPropertySums>,
    /// Value and subject counts for the predicates the index has no ndv for.
    ndv: FxHashMap<Sid, NdvSums>,
}

#[derive(Default)]
struct PropertySums {
    count: i64,
    datatypes: FxHashMap<u8, i64>,
    asserted: FxHashSet<u8>,
}

#[derive(Default)]
struct GraphPropertySums {
    count: i64,
    last_modified_t: i64,
    datatypes: FxHashMap<u8, i64>,
    observed: Vec<u8>,
}

/// Per value (keyed by [`value_key`]) and per subject, the net count.
#[derive(Default)]
struct NdvSums {
    values: FxHashMap<u64, i64>,
    subjects: FxHashMap<Sid, i64>,
}

/// [`NdvSums`] merged across segments.
type NdvRefs<'a> = (FxHashMap<u64, i64>, FxHashMap<&'a Sid, i64>);

/// A value's identity for distinct-value counting: a hash of the term, so
/// sums hold no copy of a string or vector. Distinct values sharing a hash
/// count once, which an estimate tolerates.
fn value_key(flake: &Flake) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    (&flake.o, &flake.dt).hash(&mut hasher);
    hasher.finish()
}

impl SegmentSums {
    /// What the walk in `assemble_fast_stats_inner` does per flake under the
    /// estimate merge with counts-only classes, summed instead of applied.
    fn of(
        flakes: &[Flake],
        snapshot: &LedgerSnapshot,
        indexed_ndv: &FxHashSet<(u16, &str)>,
    ) -> Self {
        let mut sums = Self::default();
        for flake in flakes {
            let delta = if flake.op { 1 } else { -1 };
            sums.flakes += delta;
            let g_id = graph_id_for_flake(snapshot, flake);
            *sums.graph_flakes.entry(g_id).or_insert(0) += delta;
            if !include_in_runtime_stats(flake, i64::MAX) {
                continue;
            }
            if is_rdf_type(&flake.p) {
                if let FlakeValue::Ref(class_sid) = &flake.o {
                    *sums.classes.entry(class_sid.clone()).or_insert(0) += delta;
                }
                continue;
            }
            let tag = runtime_datatype_tag(flake);
            if !indexed_ndv.contains(&(flake.p.namespace_code, &*flake.p.name)) {
                let ndv = sums.ndv.entry(flake.p.clone()).or_default();
                *ndv.values.entry(value_key(flake)).or_insert(0) += delta;
                *ndv.subjects.entry(flake.s.clone()).or_insert(0) += delta;
            }
            let property = sums.properties.entry(flake.p.clone()).or_default();
            property.count += delta;
            *property.datatypes.entry(tag).or_insert(0) += delta;
            if flake.op {
                property.asserted.insert(tag);
            }
            let graph_property = sums
                .graph_properties
                .entry((g_id, flake.p.clone()))
                .or_default();
            graph_property.count += delta;
            graph_property.last_modified_t = graph_property.last_modified_t.max(flake.t);
            *graph_property.datatypes.entry(tag).or_insert(0) += delta;
            if flake.op {
                note_observed_tag(&mut graph_property.observed, tag);
            }
        }
        sums
    }
}

/// `segment`'s sums under `ndv_scope`, computed on first use and kept in the
/// segment.
fn segment_sums(
    segment: &Segment,
    ndv_scope: u64,
    snapshot: &LedgerSnapshot,
    indexed_ndv: &FxHashSet<(u16, &str)>,
) -> Arc<SegmentSums> {
    let slot = &segment.planner_sums.0;
    if let Some((scope, sums)) = slot
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .as_ref()
    {
        if *scope == ndv_scope {
            return Arc::clone(sums);
        }
    }
    let sums = Arc::new(SegmentSums::of(&segment.flakes, snapshot, indexed_ndv));
    *slot
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some((ndv_scope, Arc::clone(&sums)));
    sums
}

/// The planner merge of `novelty` into `indexed`, from segment sums. `None`
/// when a segment holds flakes after `to_t`, which the sums cannot exclude:
/// the caller walks instead.
pub(super) fn planner_stats(
    indexed: &IndexStats,
    snapshot: &LedgerSnapshot,
    novelty: &Novelty,
    to_t: i64,
    lookup: Option<&dyn StatsLookup>,
) -> Option<IndexStats> {
    // The sums carry no `rdf:reifies` link deltas, which only the walk counts.
    if novelty.has_annotations() {
        return None;
    }
    let segments: Vec<&Segment> = novelty
        .graphs
        .lists()
        .flat_map(|segments| segments.iter().map(AsRef::as_ref))
        .collect();
    if segments.iter().any(|segment| segment.max_t > to_t) {
        return None;
    }

    let indexed_ndv: FxHashSet<(u16, &str)> = indexed
        .properties
        .as_ref()
        .map(|props| {
            props
                .iter()
                .filter(|p| p.ndv_values > 0 || p.ndv_subjects > 0)
                .map(|p| (p.sid.0, p.sid.1.as_str()))
                .collect()
        })
        .unwrap_or_default();
    let ndv_scope = {
        let mut scope: Vec<&(u16, &str)> = indexed_ndv.iter().collect();
        scope.sort_unstable();
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        scope.hash(&mut hasher);
        hasher.finish()
    };
    let sums: Vec<Arc<SegmentSums>> = segments
        .iter()
        .map(|segment| segment_sums(segment, ndv_scope, snapshot, &indexed_ndv))
        .collect();

    let mut flakes_delta = 0i64;
    let mut graph_flakes: FxHashMap<GraphId, i64> = FxHashMap::default();
    let mut class_count_deltas: HashMap<Sid, i64> = HashMap::new();
    let mut property_deltas = build_property_deltas(indexed);
    let mut graph_properties: FxHashMap<(GraphId, &Sid), GraphPropertySums> = FxHashMap::default();
    let mut ndv_acc: FxHashMap<(u16, &str), NdvRefs<'_>> = FxHashMap::default();
    for segment in &sums {
        flakes_delta += segment.flakes;
        for (&g_id, &delta) in &segment.graph_flakes {
            *graph_flakes.entry(g_id).or_insert(0) += delta;
        }
        for (class_sid, &delta) in &segment.classes {
            *class_count_deltas.entry(class_sid.clone()).or_insert(0) += delta;
        }
        for (p, sum) in &segment.properties {
            let property = property_deltas
                .entry((p.namespace_code, &*p.name))
                .or_default();
            property.count += sum.count;
            for (&tag, &delta) in &sum.datatypes {
                *property.datatype_deltas.entry(tag).or_insert(0) += delta;
            }
            property
                .asserted_datatypes
                .extend(sum.asserted.iter().copied());
        }
        for ((g_id, p), sum) in &segment.graph_properties {
            let merged = graph_properties.entry((*g_id, p)).or_default();
            merged.count += sum.count;
            merged.last_modified_t = merged.last_modified_t.max(sum.last_modified_t);
            for (&tag, &delta) in &sum.datatypes {
                *merged.datatypes.entry(tag).or_insert(0) += delta;
            }
            for &tag in &sum.observed {
                note_observed_tag(&mut merged.observed, tag);
            }
        }
        for (p, ndv) in &segment.ndv {
            if indexed_ndv.contains(&(p.namespace_code, &*p.name)) {
                continue;
            }
            let acc = ndv_acc.entry((p.namespace_code, &*p.name)).or_default();
            for (&value, &delta) in &ndv.values {
                *acc.0.entry(value).or_insert(0) += delta;
            }
            for (subject, &delta) in &ndv.subjects {
                *acc.1.entry(subject).or_insert(0) += delta;
            }
        }
    }

    let mut graphs = graphs_without_classes(indexed);
    let mut graph_index: FxHashMap<GraphId, usize> = graphs
        .iter()
        .enumerate()
        .map(|(idx, entry)| (entry.g_id, idx))
        .collect();
    let mut graph_ids: Vec<GraphId> = graph_flakes.keys().copied().collect();
    graph_ids.sort_unstable();
    for g_id in graph_ids {
        let entry = get_or_insert_graph_entry(&mut graphs, &mut graph_index, g_id);
        entry.flakes = ((entry.flakes as i64) + graph_flakes[&g_id]).max(0) as u64;
    }
    if let Some(lookup) = lookup {
        for ((g_id, p), sum) in &graph_properties {
            let Some(p_id) = lookup.runtime_predicate_id_for_sid(p) else {
                continue;
            };
            let graph_entry = get_or_insert_graph_entry(&mut graphs, &mut graph_index, *g_id);
            let prop_entry = get_or_insert_graph_property(graph_entry, p_id);
            prop_entry.count = ((prop_entry.count as i64) + sum.count).max(0) as u64;
            prop_entry.last_modified_t = prop_entry.last_modified_t.max(sum.last_modified_t);
            let mut tags: Vec<(&u8, &i64)> = sum.datatypes.iter().collect();
            tags.sort_unstable();
            for (&tag, &delta) in tags {
                increment_count(&mut prop_entry.datatypes, tag, delta);
            }
            for &tag in &sum.observed {
                note_observed_tag(&mut prop_entry.observed_datatypes, tag);
            }
        }
    }

    let novelty_ndv: FxHashMap<(u16, &str), (u64, u64)> = ndv_acc
        .into_iter()
        .map(|(sid, (values, subjects))| {
            (
                sid,
                (
                    values.values().filter(|&&c| c > 0).count() as u64,
                    subjects.values().filter(|&&c| c > 0).count() as u64,
                ),
            )
        })
        .collect();

    let mut stats = IndexStats {
        flakes: (indexed.flakes as i64 + flakes_delta).max(0) as u64,
        size: indexed.size + novelty.size as u64,
        properties: finalize_properties(indexed, property_deltas, &novelty_ndv),
        classes: None,
        graphs: None,
        historical_since_t: indexed.historical_since_t,
        links: super::finalize_links(indexed, snapshot, HashMap::new()),
    };
    if graphs.is_empty() {
        stats.graphs = indexed.graphs.clone();
    } else {
        graphs.sort_by_key(|entry| entry.g_id);
        for graph in &mut graphs {
            graph.properties.sort_by_key(|entry| entry.p_id);
        }
        stats.graphs = Some(graphs);
    }
    stats.classes = counts_only_classes(indexed, class_count_deltas);
    Some(stats)
}
