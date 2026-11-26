//! Index statistics hooks.
//!
//! Provides a hook interface for collecting statistics during index building.
//!
//! ## Submodules
//!
//! - [`hashing`] — Domain-separated hashing for HLL registers
//! - [`id_hook`] — ID-based per-(graph, property) HLL tracking
//! - [`schema_extractor`] — Schema hierarchy extraction from flakes
//! - [`class_property`] — Class-property statistics from novelty + PSOT
//! - [`class_stats`] — JSON/struct output for class stats from SPOT merge

pub mod class_property;
pub mod class_stats;
pub mod hashing;
pub mod id_hook;
pub mod schema_extractor;
pub(crate) mod store_io;
pub use store_io::{
    add_segments_to_hot, encode_stats_segments, finalized_graph_stats, inline_graph_stats,
    put_build_store, put_encoded_segments, served_graph_stats, EncodedSegment,
};

// Re-export everything at stats:: level for backward compat
pub use class_property::{
    batch_lookup_subject_classes, compute_class_property_stats_parallel, ClassPropertyExtractor,
    ClassPropertyStatsResult,
};
pub use class_stats::{
    build_class_stat_entries, build_class_stat_entries_with, build_class_stats_json,
    SpotClassStats, DT_REF_ID,
};
pub use hashing::{subject_hash, value_hash, value_hash_v2};
pub use id_hook::{
    stats_record_from_v2, GraphPropertyKey, IdPropertyHll, IdStatsHook, IdStatsResult, StatsRecord,
};
pub use schema_extractor::{SchemaEntry, SchemaExtractor};

use fluree_db_core::Flake;
use fluree_db_core::{GraphStatsEntry, PropertyStatEntry, StatsIriEncoder};

/// Hook for collecting index statistics during build
///
/// Implementors receive callbacks during index building and produce
/// artifacts to persist alongside the index.
pub trait IndexStatsHook {
    /// Called for each flake during tree building
    fn on_flake(&mut self, flake: &Flake);

    /// Called after build completes, returns artifacts to persist
    fn finalize(self: Box<Self>) -> StatsArtifacts;
}

/// Artifacts produced by stats collection
#[derive(Debug, Clone, Default)]
pub struct StatsArtifacts {
    /// Summary fields for DbRoot (counts, NDV estimates)
    pub summary: StatsSummary,
}

/// Summary statistics for the index
#[derive(Debug, Clone, Default)]
pub struct StatsSummary {
    /// Total number of flakes in the index
    pub flake_count: usize,
    /// Per-property statistics (sorted by SID for determinism)
    ///
    /// Note: the current binary index pipeline uses `IdStatsHook` and produces
    /// ID-keyed sketches persisted in the statistics store. This field is retained
    /// for legacy hook implementations and may be `None`.
    pub properties: Option<Vec<fluree_db_core::PropertyStatEntry>>,
}

/// Roll the per-graph property stats up into the ledger-wide, SID-keyed
/// `IndexStats.properties` view, resolving each `p_id` to its IRI through
/// `resolve_predicate_iri` and that to the SID the planner looks it up by.
pub fn aggregate_property_entries_from_graphs<F>(
    graphs: &[GraphStatsEntry],
    encoder: &StatsIriEncoder,
    mut resolve_predicate_iri: F,
) -> Vec<PropertyStatEntry>
where
    F: FnMut(u32) -> Option<String>,
{
    aggregate_property_entries_by_sid(graphs, |p_id| {
        encoder.encode_parts(&resolve_predicate_iri(p_id).unwrap_or_default())
    })
}

/// The class tables of the graphs `graphs` holds, taken from `per_graph`, by
/// graph, each sorted by class: the root's per-graph class section.
pub fn graph_class_tables(
    graphs: &[GraphStatsEntry],
    per_graph: &mut std::collections::HashMap<
        fluree_db_core::GraphId,
        Vec<fluree_db_core::ClassStatEntry>,
    >,
) -> Vec<(fluree_db_core::GraphId, Vec<fluree_db_core::ClassStatEntry>)> {
    graphs
        .iter()
        .filter_map(|g| {
            let mut classes = per_graph.remove(&g.g_id)?;
            classes.sort_by(|a, b| a.class_sid.cmp(&b.class_sid));
            Some((g.g_id, classes))
        })
        .collect()
}

/// The ledger-wide class table: the union of the per-graph ones.
pub fn union_graph_class_tables(
    tables: &[(fluree_db_core::GraphId, Vec<fluree_db_core::ClassStatEntry>)],
) -> Option<Vec<fluree_db_core::ClassStatEntry>> {
    let slices: Vec<&[fluree_db_core::ClassStatEntry]> =
        tables.iter().map(|(_, c)| c.as_slice()).collect();
    fluree_db_core::index_stats::union_class_stat_slices(&slices)
}

/// One (graph, property) entry as the ledger-wide aggregate counts it.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct AggregateInput {
    pub count: u64,
    pub ndv_values: u64,
    pub ndv_subjects: u64,
    pub last_modified_t: i64,
    pub datatypes: Vec<(u8, u64)>,
    pub historical_datatypes: Vec<u8>,
}

impl From<&fluree_db_binary_index::format::stats_store::EntrySummary> for AggregateInput {
    fn from(s: &fluree_db_binary_index::format::stats_store::EntrySummary) -> Self {
        Self {
            count: s.count,
            ndv_values: s.ndv_values,
            ndv_subjects: s.ndv_subjects,
            last_modified_t: s.last_modified_t,
            datatypes: s.datatypes.clone(),
            historical_datatypes: s.historical_datatypes.clone(),
        }
    }
}

/// Move the ledger-wide property aggregates from each touched entry's
/// `before` state (`None`: the entry is new) to its `after` state, without
/// reading any other entry: counts and datatype counts by difference, NDV and
/// `last_modified_t` by maximum, historical tags by union. Sound because an
/// entry's NDV and `last_modified_t` never fall and its historical tags never
/// shrink while its graph stands; a graph drop, which removes entries, needs
/// [`aggregate_property_entries_by_sid`] over every entry instead.
///
/// `properties` is keyed by SID (`resolve_predicate_sid`), as the root holds
/// it; an aggregate whose count and datatypes reach zero stays, as one rolled
/// up from a zero-count entry would.
pub fn apply_aggregate_deltas<'a, F>(
    properties: &mut Vec<PropertyStatEntry>,
    changes: impl IntoIterator<Item = (u32, Option<&'a AggregateInput>, &'a AggregateInput)>,
    mut resolve_predicate_sid: F,
) where
    F: FnMut(u32) -> (u16, String),
{
    let mut at: std::collections::HashMap<(u16, String), usize> = properties
        .iter()
        .enumerate()
        .map(|(i, p)| (p.sid.clone(), i))
        .collect();
    let mut changed = std::collections::HashSet::new();
    for (p_id, before, after) in changes {
        let sid = resolve_predicate_sid(p_id);
        let i = *at.entry(sid.clone()).or_insert_with(|| {
            properties.push(PropertyStatEntry {
                sid,
                count: 0,
                ndv_values: 0,
                ndv_subjects: 0,
                last_modified_t: 0,
                datatypes: Vec::new(),
                observed_datatypes: Vec::new(),
                historical_datatypes: Vec::new(),
            });
            properties.len() - 1
        });
        let e = &mut properties[i];
        let before_count = before.map_or(0, |b| b.count);
        e.count = (e.count + after.count).saturating_sub(before_count);
        let mut datatypes: std::collections::BTreeMap<u8, i128> = e
            .datatypes
            .iter()
            .map(|&(t, n)| (t, i128::from(n)))
            .collect();
        for &(t, n) in &after.datatypes {
            *datatypes.entry(t).or_insert(0) += i128::from(n);
        }
        for &(t, n) in before.map_or(&[][..], |b| &b.datatypes[..]) {
            *datatypes.entry(t).or_insert(0) -= i128::from(n);
        }
        e.datatypes = datatypes
            .into_iter()
            .filter(|&(_, n)| n > 0)
            .map(|(t, n)| (t, n as u64))
            .collect();
        e.ndv_values = e.ndv_values.max(after.ndv_values);
        e.ndv_subjects = e.ndv_subjects.max(after.ndv_subjects);
        e.last_modified_t = e.last_modified_t.max(after.last_modified_t);
        e.historical_datatypes =
            PropertyStatEntry::union_tags(&e.historical_datatypes, &after.historical_datatypes);
        changed.insert(i);
    }
    for i in changed {
        let e = &mut properties[i];
        e.observed_datatypes = PropertyStatEntry::tags_of(&e.datatypes);
    }
    // In the order a root holds them, new entries included.
    properties.sort_by(|a, b| a.sid.cmp(&b.sid));
}

/// The live `rdf:reifies` links per inner predicate, from the hook's
/// triple-term row counts, with each inner predicate's SID. Empty when the
/// ledger has no `rdf:reifies` predicate.
pub fn link_stat_entries<F>(
    term_rows: &[(u32, u32, u64)],
    reifies_p_id: Option<u32>,
    mut resolve_predicate_sid: F,
) -> Vec<fluree_db_core::LinkStatEntry>
where
    F: FnMut(u32) -> (u16, String),
{
    let Some(reifies) = reifies_p_id else {
        return Vec::new();
    };
    term_rows
        .iter()
        .filter(|&&(p_id, _, _)| p_id == reifies)
        .map(|&(_, inner, count)| fluree_db_core::LinkStatEntry {
            sid: resolve_predicate_sid(inner),
            count,
        })
        .collect()
}

/// The same roll-up for a caller that already holds each predicate's SID — the
/// import builds a `predicate_sids` table as it goes, so it never needs the IRI
/// round trip.
///
/// This is the only place the index-build side fills
/// [`PropertyStatEntry::observed_datatypes`]. That field is fail-closed: an
/// unfilled one reads as "unknown" and silently declines the equijoin-filter
/// fold rather than breaking a query, which is exactly the failure mode a
/// second copy of this loop would hide. One producer, one test.
pub fn aggregate_property_entries_by_sid<F>(
    graphs: &[GraphStatsEntry],
    mut resolve_predicate_sid: F,
) -> Vec<PropertyStatEntry>
where
    F: FnMut(u32) -> (u16, String),
{
    struct PropAgg {
        count: u64,
        ndv_values: u64,
        ndv_subjects: u64,
        last_modified_t: i64,
        datatypes: Vec<(u8, u64)>,
        historical_datatypes: Vec<u8>,
    }

    let mut agg: std::collections::HashMap<u32, PropAgg> = std::collections::HashMap::new();
    for g in graphs {
        for p in &g.properties {
            let e = agg.entry(p.p_id).or_insert(PropAgg {
                count: 0,
                ndv_values: 0,
                ndv_subjects: 0,
                last_modified_t: 0,
                datatypes: Vec::new(),
                historical_datatypes: Vec::new(),
            });
            e.count += p.count;
            e.ndv_values = e.ndv_values.max(p.ndv_values);
            e.ndv_subjects = e.ndv_subjects.max(p.ndv_subjects);
            e.last_modified_t = e.last_modified_t.max(p.last_modified_t);
            e.historical_datatypes
                .extend_from_slice(&p.historical_datatypes);
            for &(dt, cnt) in &p.datatypes {
                if let Some(existing) = e.datatypes.iter_mut().find(|(d, _)| *d == dt) {
                    existing.1 += cnt;
                } else {
                    e.datatypes.push((dt, cnt));
                }
            }
        }
    }

    let mut out: Vec<PropertyStatEntry> = agg
        .into_iter()
        .map(|(p_id, mut pa)| {
            let (ns, name) = resolve_predicate_sid(p_id);
            pa.historical_datatypes.sort_unstable();
            pa.historical_datatypes.dedup();
            PropertyStatEntry {
                sid: (ns, name),
                count: pa.count,
                ndv_values: pa.ndv_values,
                ndv_subjects: pa.ndv_subjects,
                last_modified_t: pa.last_modified_t,
                observed_datatypes: PropertyStatEntry::tags_of(&pa.datatypes),
                historical_datatypes: pa.historical_datatypes,
                datatypes: pa.datatypes,
            }
        })
        .collect();
    // In the order a root holds them.
    out.sort_by(|a, b| a.sid.cmp(&b.sid));
    out
}

/// No-op implementation for Phase A
///
/// Does nothing but count flakes. Placeholder for future HLL/sketch implementations.
#[derive(Debug, Default)]
pub struct NoOpStatsHook {
    flake_count: usize,
}

impl NoOpStatsHook {
    /// Create a new no-op stats hook
    pub fn new() -> Self {
        Self::default()
    }
}

impl IndexStatsHook for NoOpStatsHook {
    fn on_flake(&mut self, _flake: &Flake) {
        self.flake_count += 1;
    }

    fn finalize(self: Box<Self>) -> StatsArtifacts {
        StatsArtifacts {
            summary: StatsSummary {
                flake_count: self.flake_count,
                properties: None,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::{FlakeValue, Sid};

    fn make_test_flake(t: i64) -> Flake {
        Flake::new(
            Sid::new(1, "s"),
            Sid::new(2, "p"),
            FlakeValue::Long(42),
            Sid::new(3, "long"),
            t,
            true,
            None,
        )
    }

    #[test]
    fn test_no_op_stats_hook() {
        let mut hook = NoOpStatsHook::new();

        hook.on_flake(&make_test_flake(1));
        hook.on_flake(&make_test_flake(2));
        hook.on_flake(&make_test_flake(3));

        let artifacts = Box::new(hook).finalize();

        assert_eq!(artifacts.summary.flake_count, 3);
    }

    #[test]
    fn test_no_op_stats_hook_empty() {
        let hook = NoOpStatsHook::new();
        let artifacts = Box::new(hook).finalize();

        assert_eq!(artifacts.summary.flake_count, 0);
    }

    /// The index-build aggregate has to carry the datatype tags across the
    /// graph roll-up. An empty `observed_datatypes` here is not a failure the
    /// query path can see — it reads as "unknown" and quietly declines the
    /// equijoin-filter fold — so nothing downstream would go red if this
    /// producer stopped filling it. This is the pin.
    #[test]
    fn aggregate_carries_the_observed_datatype_tags_across_graphs() {
        use fluree_db_core::{GraphPropertyStatEntry, GraphStatsEntry};

        let graph = |g_id, datatypes: Vec<(u8, u64)>, historical: Vec<u8>| GraphStatsEntry {
            g_id,
            flakes: 1,
            size: 10,
            properties: vec![GraphPropertyStatEntry {
                p_id: 7,
                count: datatypes.iter().map(|&(_, c)| c).sum(),
                ndv_values: 1,
                ndv_subjects: 1,
                last_modified_t: 1,
                observed_datatypes: PropertyStatEntry::tags_of(&datatypes),
                historical_datatypes: historical,
                datatypes,
            }],
            classes: None,
        };
        // Tag 1 in one graph, tag 3 in another: the roll-up has to end up with
        // both, so the fold's "does this predicate carry literals?" question is
        // answered over the whole ledger and not one graph of it. The
        // historical sets carry a tag (9) that no current count mentions —
        // a literal deleted in some earlier window — and the roll-up must
        // keep it, or a time-travel read loses its coverage.
        let graphs = vec![
            graph(0, vec![(1, 2)], vec![1, 9]),
            graph(1, vec![(3, 5)], vec![3]),
        ];

        let entries = aggregate_property_entries_by_sid(&graphs, |p_id| (10, format!("p{p_id}")));

        assert_eq!(entries.len(), 1);
        let entry = &entries[0];
        assert_eq!(entry.sid, (10, "p7".to_string()));
        assert_eq!(
            entry.observed_datatypes,
            vec![1, 3],
            "the index-build aggregate dropped the datatype tags it rolled up"
        );
        assert_eq!(
            entry.observed_datatypes,
            PropertyStatEntry::tags_of(&entry.datatypes),
            "the tag set and the breakdown it summarizes disagree"
        );
        assert_eq!(
            entry.historical_datatypes,
            vec![1, 3, 9],
            "the roll-up lost a historical tag its current counts no longer mention"
        );
    }
}
