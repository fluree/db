//! Index statistics hooks.
//!
//! Provides a hook interface for collecting statistics during index building.
//!
//! ## Submodules
//!
//! - [`hashing`] — Domain-separated hashing for HLL registers
//! - [`sketch_cas`] — CAS-persisted HLL sketch blob serialization
//! - [`id_hook`] — ID-based per-(graph, property) HLL tracking
//! - [`schema_extractor`] — Schema hierarchy extraction from flakes
//! - [`class_property`] — Class-property statistics from novelty + PSOT
//! - [`class_stats`] — JSON/struct output for class stats from SPOT merge

pub mod class_property;
pub mod class_stats;
pub mod hashing;
pub mod id_hook;
pub mod schema_extractor;
pub mod sketch_cas;

// Re-export everything at stats:: level for backward compat
pub use class_property::{
    batch_lookup_subject_classes, compute_class_property_stats_parallel, ClassPropertyExtractor,
    ClassPropertyStatsResult,
};
pub use class_stats::{
    build_class_stat_entries, build_class_stats_json, SpotClassStats, DT_REF_ID,
};
pub use hashing::{subject_hash, value_hash, value_hash_v2};
pub use id_hook::{
    stats_record_from_v2, GraphPropertyKey, IdPropertyHll, IdStatsHook, IdStatsResult, StatsRecord,
};
pub use schema_extractor::{SchemaEntry, SchemaExtractor};
pub use sketch_cas::{load_sketch_blob, HllPropertyEntry, HllSketchBlob};

use fluree_db_core::Flake;
use fluree_db_core::{GraphStatsEntry, PrefixTrie, PropertyStatEntry};

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
    /// ID-keyed sketches persisted via `HllSketchBlob`. This field is retained
    /// for legacy hook implementations and may be `None`.
    pub properties: Option<Vec<fluree_db_core::PropertyStatEntry>>,
}

/// Roll the per-graph property stats up into the ledger-wide, SID-keyed
/// `IndexStats.properties` view, resolving each `p_id` to a SID through `trie`
/// and `resolve_predicate_iri`.
pub fn aggregate_property_entries_from_graphs<F>(
    graphs: &[GraphStatsEntry],
    trie: &PrefixTrie,
    mut resolve_predicate_iri: F,
) -> Vec<PropertyStatEntry>
where
    F: FnMut(u32) -> Option<String>,
{
    aggregate_property_entries_by_sid(graphs, |p_id| {
        let iri = resolve_predicate_iri(p_id).unwrap_or_default();
        match trie.longest_match(&iri) {
            Some((code, prefix_len)) => (code, iri[prefix_len..].to_string()),
            None => (0u16, iri),
        }
    })
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
            });
            e.count += p.count;
            e.ndv_values = e.ndv_values.max(p.ndv_values);
            e.ndv_subjects = e.ndv_subjects.max(p.ndv_subjects);
            e.last_modified_t = e.last_modified_t.max(p.last_modified_t);
            for &(dt, cnt) in &p.datatypes {
                if let Some(existing) = e.datatypes.iter_mut().find(|(d, _)| *d == dt) {
                    existing.1 += cnt;
                } else {
                    e.datatypes.push((dt, cnt));
                }
            }
        }
    }

    agg.into_iter()
        .map(|(p_id, pa)| {
            let (ns, name) = resolve_predicate_sid(p_id);
            PropertyStatEntry {
                sid: (ns, name),
                count: pa.count,
                ndv_values: pa.ndv_values,
                ndv_subjects: pa.ndv_subjects,
                last_modified_t: pa.last_modified_t,
                observed_datatypes: PropertyStatEntry::tags_of(&pa.datatypes),
                datatypes: pa.datatypes,
            }
        })
        .collect()
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

        let graph = |g_id, datatypes: Vec<(u8, u64)>| GraphStatsEntry {
            g_id,
            flakes: 1,
            size: 10,
            properties: vec![GraphPropertyStatEntry {
                p_id: 7,
                count: datatypes.iter().map(|&(_, c)| c).sum(),
                ndv_values: 1,
                ndv_subjects: 1,
                last_modified_t: 1,
                datatypes,
            }],
            classes: None,
        };
        // Tag 1 in one graph, tag 3 in another: the roll-up has to end up with
        // both, so the fold's "does this predicate carry literals?" question is
        // answered over the whole ledger and not one graph of it.
        let graphs = vec![graph(0, vec![(1, 2)]), graph(1, vec![(3, 5)])];

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
    }
}
