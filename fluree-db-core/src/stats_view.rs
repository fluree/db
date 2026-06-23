//! Pre-built statistics lookup for query optimization.
//!
//! `StatsView` provides O(1) lookups of property and class statistics,
//! built from `IndexStats` at query time.

use crate::annotation_index::AnnotationStats;
use crate::ids::{GraphId, RuntimePredicateId};
use crate::index_stats::IndexStats;
use crate::sid::Sid;
use crate::value_id::ValueTypeTag;
use std::collections::HashMap;
use std::sync::Arc;

/// Pre-built stats lookup for query optimization.
///
/// Built from `IndexStats` at query time, provides O(1) lookups for
/// property and class statistics used in selectivity estimation.
#[derive(Debug, Default, Clone)]
pub struct StatsView {
    /// Property SID -> (count, ndv_values, ndv_subjects)
    pub properties: HashMap<Sid, PropertyStatData>,
    /// Class SID -> instance count
    pub classes: HashMap<Sid, u64>,
    /// Property IRI -> (count, ndv_values, ndv_subjects)
    ///
    /// This is derived from `properties` using the db's namespace table.
    /// It exists to support planners that keep IRIs unencoded (e.g. cross-ledger-aware planning).
    pub properties_by_iri: HashMap<Arc<str>, PropertyStatData>,
    /// Class IRI -> instance count
    ///
    /// This is derived from `classes` using the db's namespace table.
    pub classes_by_iri: HashMap<Arc<str>, u64>,
    /// Graph-scoped property stats keyed by runtime predicate IDs.
    ///
    /// Populated from `IndexStats.graphs` when present. Provides per-graph
    /// property lookups with datatype breakdown. The aggregate Sid-keyed
    /// `properties` map remains the primary source for the query planner.
    pub graph_properties: HashMap<GraphId, HashMap<RuntimePredicateId, GraphPropertyStatData>>,
    /// Property SID -> whether every object of this property is a node/IRI ref
    /// (all datatype tags are [`ValueTypeTag::JSON_LD_ID`]). Derived from the
    /// current-state (novelty-merged) per-datatype breakdown. Used by the
    /// equijoin-filter fold to soundly rewrite `FILTER(?x = ?y)` into a join
    /// only when value-equality coincides with term-equality (true for nodes).
    pub property_ref_only: HashMap<Sid, bool>,
    /// Property IRI -> ref-only flag (see [`Self::property_ref_only`]).
    pub property_ref_only_by_iri: HashMap<Arc<str>, bool>,
    /// Predicate IRI -> (class IRI -> count of that predicate's flakes whose
    /// SUBJECT is an instance of the class), summed across graphs. Sourced from
    /// `IndexStats.classes[*].properties[*].datatypes`.
    ///
    /// Enables sound elision of a redundant `?s rdf:type <C>`: when `?s` is the
    /// subject of predicate P and the count here for `(P, C)` equals P's total
    /// flake count, every P-subject is provably a C, so the type filter removes
    /// nothing. The aggregate (cross-graph) equality implies per-graph coverage,
    /// so it is sound for any single-graph current-state read. Only consult via
    /// [`Self::predicate_subjects_all_in_class_by_iri`], which honors the gate.
    pub predicate_class_subject_counts_by_iri: HashMap<Arc<str>, HashMap<Arc<str>, u64>>,
    /// True only when the class/property counts reflect exact current state with
    /// no overlay gap — novelty empty and no policy visibility layer. Set by the
    /// query stats-cache builder; defaults `false` so any caller that does not
    /// explicitly vouch for current-state exactness never triggers elision.
    pub class_coverage_trustworthy: bool,
}

/// Per-property statistics within a graph, keyed by numeric IDs.
#[derive(Debug, Clone)]
pub struct GraphPropertyStatData {
    /// Total number of flakes with this property in this graph
    pub count: u64,
    /// Estimated number of distinct object values (from HLL)
    pub ndv_values: u64,
    /// Estimated number of distinct subjects using this property (from HLL)
    pub ndv_subjects: u64,
    /// Per-datatype flake counts
    pub datatypes: Vec<(ValueTypeTag, u64)>,
}

/// Statistics for a single property.
#[derive(Debug, Clone, Copy)]
pub struct PropertyStatData {
    /// Total number of flakes with this property
    pub count: u64,
    /// Estimated number of distinct object values (from HLL)
    pub ndv_values: u64,
    /// Estimated number of distinct subjects using this property (from HLL)
    pub ndv_subjects: u64,
}

impl StatsView {
    /// Approximate byte size for cache weighing.
    pub fn byte_size(&self) -> usize {
        use std::mem::size_of;

        let properties = self
            .properties
            .keys()
            .map(|sid| size_of::<u16>() + sid.name.len() + size_of::<PropertyStatData>())
            .sum::<usize>();
        let classes = self
            .classes
            .keys()
            .map(|sid| size_of::<u16>() + sid.name.len() + size_of::<u64>())
            .sum::<usize>();
        let properties_by_iri = self
            .properties_by_iri
            .keys()
            .map(|iri| iri.len() + size_of::<PropertyStatData>())
            .sum::<usize>();
        let classes_by_iri = self
            .classes_by_iri
            .keys()
            .map(|iri| iri.len() + size_of::<u64>())
            .sum::<usize>();
        let graph_properties = self
            .graph_properties
            .values()
            .map(|props| {
                props
                    .values()
                    .map(|data| {
                        size_of::<RuntimePredicateId>()
                            + size_of::<GraphPropertyStatData>()
                            + data.datatypes.len() * size_of::<(ValueTypeTag, u64)>()
                    })
                    .sum::<usize>()
            })
            .sum::<usize>();

        let predicate_class_subject_counts = self
            .predicate_class_subject_counts_by_iri
            .iter()
            .map(|(pred, by_class)| {
                pred.len()
                    + by_class
                        .keys()
                        .map(|cls| cls.len() + size_of::<u64>())
                        .sum::<usize>()
            })
            .sum::<usize>();

        size_of::<Self>()
            + properties
            + classes
            + properties_by_iri
            + classes_by_iri
            + graph_properties
            + predicate_class_subject_counts
    }

    /// Build from IndexStats.
    ///
    /// Note: `PropertyStatEntry.sid` is already `(i32, String)` matching `Sid::new` shape,
    /// so no namespace_codes lookup is needed.
    pub fn from_db_stats(stats: &IndexStats) -> Self {
        let mut view = StatsView::default();

        if let Some(ref props) = stats.properties {
            for entry in props {
                // entry.sid is (namespace_code, name) - directly usable
                let sid = Sid::new(entry.sid.0, &entry.sid.1);
                view.properties.insert(
                    sid.clone(),
                    PropertyStatData {
                        count: entry.count,
                        ndv_values: entry.ndv_values,
                        ndv_subjects: entry.ndv_subjects,
                    },
                );
                // Ref-only iff every observed object datatype is a node/IRI ref.
                // Empty datatypes (unknown) => not provably ref-only.
                let ref_only = !entry.datatypes.is_empty()
                    && entry
                        .datatypes
                        .iter()
                        .all(|&(dt, _)| dt == ValueTypeTag::JSON_LD_ID.as_u8());
                view.property_ref_only.insert(sid, ref_only);
            }
        }

        if let Some(ref classes) = stats.classes {
            for entry in classes {
                view.classes.insert(entry.class_sid.clone(), entry.count);
            }
        }

        if let Some(ref graphs) = stats.graphs {
            for g_entry in graphs {
                let mut prop_map = HashMap::new();
                for p_entry in &g_entry.properties {
                    prop_map.insert(
                        RuntimePredicateId::from_u32(p_entry.p_id),
                        GraphPropertyStatData {
                            count: p_entry.count,
                            ndv_values: p_entry.ndv_values,
                            ndv_subjects: p_entry.ndv_subjects,
                            datatypes: p_entry
                                .datatypes
                                .iter()
                                .map(|&(dt, c)| (ValueTypeTag::from_u8(dt), c))
                                .collect(),
                        },
                    );
                }
                view.graph_properties.insert(g_entry.g_id, prop_map);
            }
        }

        view
    }

    /// Build from IndexStats, also deriving IRI-keyed maps using a namespace table.
    ///
    /// This does **not** change how stats are persisted (still SID-keyed in `IndexStats`).
    /// It just builds additional lookup maps that allow planning code to consult stats
    /// when query terms are represented as IRIs rather than SIDs.
    pub fn from_db_stats_with_namespaces(
        stats: &IndexStats,
        namespace_codes: &HashMap<u16, String>,
    ) -> Self {
        let mut view = StatsView::from_db_stats(stats);

        // Derive IRI-keyed property stats.
        // If a SID's namespace code is missing, skip it.
        for (sid, data) in &view.properties {
            if let Some(prefix) = namespace_codes.get(&sid.namespace_code) {
                let iri: Arc<str> = Arc::from(format!("{}{}", prefix, sid.name));
                view.properties_by_iri.insert(iri, *data);
            }
        }

        // Derive IRI-keyed class stats.
        for (sid, count) in &view.classes {
            if let Some(prefix) = namespace_codes.get(&sid.namespace_code) {
                let iri: Arc<str> = Arc::from(format!("{}{}", prefix, sid.name));
                view.classes_by_iri.insert(iri, *count);
            }
        }

        // Derive IRI-keyed ref-only flags.
        for (sid, ref_only) in &view.property_ref_only {
            if let Some(prefix) = namespace_codes.get(&sid.namespace_code) {
                let iri: Arc<str> = Arc::from(format!("{}{}", prefix, sid.name));
                view.property_ref_only_by_iri.insert(iri, *ref_only);
            }
        }

        // Derive predicate -> (class -> subject-flake count) from the raw
        // per-class property usage. `from_db_stats` keeps only class instance
        // counts, so read the usage straight off `stats.classes` here, where the
        // namespace table is available to resolve both SIDs to IRIs.
        let iri_for = |sid: &Sid| -> Option<Arc<str>> {
            namespace_codes
                .get(&sid.namespace_code)
                .map(|prefix| Arc::<str>::from(format!("{}{}", prefix, sid.name)))
        };
        if let Some(ref classes) = stats.classes {
            for class in classes {
                let Some(class_iri) = iri_for(&class.class_sid) else {
                    continue;
                };
                for prop in &class.properties {
                    let Some(pred_iri) = iri_for(&prop.property_sid) else {
                        continue;
                    };
                    let count: u64 = prop.datatypes.iter().map(|&(_, c)| c).sum();
                    if count == 0 {
                        continue;
                    }
                    *view
                        .predicate_class_subject_counts_by_iri
                        .entry(pred_iri)
                        .or_default()
                        .entry(class_iri.clone())
                        .or_insert(0) += count;
                }
            }
        }

        view
    }

    /// Whether stats prove that **every** subject of `pred_iri` is an instance of
    /// `class_iri` at exact current state — i.e. the count of `pred_iri` flakes
    /// contributed by `class_iri` instances equals `pred_iri`'s total flake count
    /// (both non-zero). When true, a `?s rdf:type <class_iri>` filter on a subject
    /// already bound by `pred_iri` is provably redundant and safe to elide.
    ///
    /// Returns `false` unless [`Self::class_coverage_trustworthy`] is set, so a
    /// stale/overlay/policy-affected view never licenses elision.
    pub fn predicate_subjects_all_in_class_by_iri(&self, pred_iri: &str, class_iri: &str) -> bool {
        if !self.class_coverage_trustworthy {
            return false;
        }
        let Some(total) = self.get_property_by_iri(pred_iri).map(|p| p.count) else {
            return false;
        };
        if total == 0 {
            return false;
        }
        let covered = self
            .predicate_class_subject_counts_by_iri
            .get(pred_iri)
            .and_then(|by_class| by_class.get(class_iri))
            .copied()
            .unwrap_or(0);
        covered == total
    }

    /// Whether every object of this property (by SID) is a node/IRI ref —
    /// i.e. value-equality coincides with term-equality. `None` when the
    /// property is unknown to stats. See [`Self::property_ref_only`].
    pub fn is_property_ref_only(&self, sid: &Sid) -> Option<bool> {
        self.property_ref_only.get(sid).copied()
    }

    /// Whether every object of this property (by IRI) is a node/IRI ref.
    pub fn is_property_ref_only_by_iri(&self, iri: &str) -> Option<bool> {
        self.property_ref_only_by_iri.get(iri).copied()
    }

    /// Get property statistics by SID.
    pub fn get_property(&self, sid: &Sid) -> Option<&PropertyStatData> {
        self.properties.get(sid)
    }

    /// Get property statistics by IRI.
    pub fn get_property_by_iri(&self, iri: &str) -> Option<&PropertyStatData> {
        self.properties_by_iri.get(iri)
    }

    /// Get class instance count by SID.
    pub fn get_class_count(&self, sid: &Sid) -> Option<u64> {
        self.classes.get(sid).copied()
    }

    /// Get class instance count by IRI.
    pub fn get_class_count_by_iri(&self, iri: &str) -> Option<u64> {
        self.classes_by_iri.get(iri).copied()
    }

    /// Check if any property statistics are available.
    pub fn has_property_stats(&self) -> bool {
        !self.properties.is_empty()
    }

    /// Check if any class statistics are available.
    pub fn has_class_stats(&self) -> bool {
        !self.classes.is_empty()
    }

    /// Get property stats within a specific graph by numeric IDs.
    pub fn get_graph_property(
        &self,
        g_id: GraphId,
        p_id: RuntimePredicateId,
    ) -> Option<&GraphPropertyStatData> {
        self.graph_properties.get(&g_id)?.get(&p_id)
    }

    /// Get all property stats for a specific graph.
    pub fn get_graph_properties(
        &self,
        g_id: GraphId,
    ) -> Option<&HashMap<RuntimePredicateId, GraphPropertyStatData>> {
        self.graph_properties.get(&g_id)
    }

    /// Return the set of graph IDs that have stats.
    pub fn graph_ids(&self) -> impl Iterator<Item = GraphId> + '_ {
        self.graph_properties.keys().copied()
    }

    /// Check if any graph-scoped statistics are available.
    pub fn has_graph_stats(&self) -> bool {
        !self.graph_properties.is_empty()
    }

    /// Overlay arena-derived statistics for the seven `f:reifies*`
    /// system predicates onto this view, using the per-slot NDV
    /// counters tracked by the arena builder.
    ///
    /// When a snapshot's `annotation_index` is present, the arena's
    /// live counters are a more accurate source for `f:reifies*`
    /// predicate cardinality than the generic `IndexStats.properties`
    /// HLL, because:
    ///
    /// - `IndexStats.count` mixes asserts and retracts; the arena
    ///   counters are live-only.
    /// - On freshly-indexed ledgers the property stats may be stale
    ///   or absent, but the arena counters are always current.
    ///
    /// **Required slots** (`f:reifiesSubject`, `f:reifiesPredicate`,
    /// `f:reifiesObject`): every live `(edge, ann)` pair contributes
    /// exactly one row, so `count = live_attachment_pairs`. Under
    /// the v1 single-target-per-ann invariant
    /// `live_attachment_pairs == distinct_annotations`, but a legacy
    /// or replayed-from-corrupt-history ledger can have one ann SID
    /// attached to multiple edges, in which case the pair count is
    /// the right denominator. `ndv_subjects = distinct_annotations`
    /// (the row's subject is the ann SID; even with multi-target
    /// the distinct subject set is still the ann SIDs).
    /// `ndv_values` uses the per-slot NDV
    /// (`distinct_reified_subjects`, `_predicates`, `_objects`)
    /// when available. Older arena roots were written before
    /// per-slot NDVs were tracked and report `0` for those fields;
    /// in that case we fall back to `ndv_values = 1` (the safe
    /// upper bound — the planner sees every `BoundObject` probe as
    /// a scan, which is conservative but not wrong).
    ///
    /// **Optional slots** (`f:reifiesGraph`, `f:reifiesLang`,
    /// `f:reifiesListIndex`): synthesized **only when their per-slot
    /// row count is non-zero**. The row count (e.g.
    /// `reifies_graph_rows`) is the number of live `(edge, ann)`
    /// pairs whose edge carries that slot — usually strictly less
    /// than `live_attachment_pairs`, and equal to the per-slot
    /// distinct ann SID count under the v1 single-target
    /// invariant. The multi-target anomaly can push it above
    /// `distinct_annotations` (one ann SID with many graph edges).
    /// `ndv_subjects` uses the per-slot ann-SID count
    /// (`distinct_<slot>_anns`) when available, falling back to
    /// `min(rows, distinct_annotations)` for older arena roots that
    /// predate the per-slot counters. When the row count is zero
    /// (older arenas with default-zeroed fields, or workloads that
    /// never use that slot), we leave the entry to the regular
    /// `IndexStats.properties` HLL.
    ///
    /// **`f:reifiesDatatype` is intentionally not synthesized.** The
    /// arena reconstructs `EdgeKey.dt` from the flake-level dt of
    /// `f:reifiesObject` and cannot tell whether the on-wire bundle
    /// actually emitted a separate `f:reifiesDatatype` flake. The
    /// arena builder reports zero for the datatype row count;
    /// `merge_annotation_stats` ignores datatype entirely and lets
    /// the regular HLL handle it.
    ///
    /// **Why per-slot NDV matters.** Without it (the original M3.1
    /// shipped state), `BoundObject` selectivity for
    /// `?ann f:reifiesObject ex:acme` was `count / 1 =
    /// distinct_annotations` — the same as a scan, so the planner
    /// got nothing from arena stats beyond the row total. With it,
    /// the estimate becomes `distinct_annotations /
    /// distinct_reified_objects` which can drop selectivity by
    /// orders of magnitude when objects are diverse.
    ///
    /// Existing entries for the seven `f:reifies*` predicates are
    /// overwritten — the arena is authoritative for live attachment
    /// counts.
    pub fn merge_annotation_stats(
        &mut self,
        ann: &AnnotationStats,
        namespace_codes: &HashMap<u16, String>,
    ) {
        use fluree_vocab::db as p;
        use fluree_vocab::namespaces::FLUREE_DB;

        if ann.distinct_annotations == 0 {
            return;
        }

        // Required slots: `count` is the number of live `(edge, ann)`
        // pairs (one row per pair per required slot). Older arena
        // roots predate `live_attachment_pairs` and report it as 0;
        // the v1 stage-time invariant says one ann SID has one live
        // target, so falling back to `distinct_annotations` is safe
        // for those. A multi-target anomaly on a current-format
        // arena will report a `live_attachment_pairs` strictly
        // greater than `distinct_annotations`, and the planner sees
        // the larger row count.
        let row_count = if ann.live_attachment_pairs > 0 {
            ann.live_attachment_pairs
        } else {
            ann.distinct_annotations
        };
        let req = |ndv: u64| PropertyStatData {
            count: row_count,
            ndv_values: ndv.max(1),
            ndv_subjects: ann.distinct_annotations,
        };

        let prefix = namespace_codes.get(&FLUREE_DB);
        let mut insert = |name: &str, data: PropertyStatData| {
            self.properties.insert(Sid::new(FLUREE_DB, name), data);
            if let Some(prefix) = prefix {
                let iri: Arc<str> = Arc::from(format!("{prefix}{name}"));
                self.properties_by_iri.insert(iri, data);
            }
        };

        insert(p::REIFIES_SUBJECT, req(ann.distinct_reified_subjects));
        insert(p::REIFIES_PREDICATE, req(ann.distinct_reified_predicates));
        insert(p::REIFIES_OBJECT, req(ann.distinct_reified_objects));

        // Optional slots: synth only when the arena observed non-zero
        // rows. `count = rows`, `ndv_values = distinct values`,
        // `ndv_subjects = per-slot ann-SID count` when available.
        //
        // Older arena roots predate the per-slot ann-SID counters
        // and report `0` for them. In that case we fall back to
        // `min(rows, distinct_annotations).max(1)` — a heuristic
        // that's exact under the v1 single-target invariant (rows
        // == distinct slot anns) but is wrong in either direction
        // when one ann SID has many slot rows: a sparse slot where
        // one anomaly ann holds most rows ends up with
        // `ndv_subjects == rows` (the cap doesn't bind) and
        // `BoundSubject` undercounts. The principled fix when this
        // matters is to reindex with the per-slot counters
        // populated — current builders always emit them.
        let cap_subjects = ann.distinct_annotations;
        let mut opt = |name: &str, rows: u64, ndv: u64, slot_anns: u64| {
            if rows == 0 {
                return;
            }
            let subjects = if slot_anns > 0 {
                slot_anns
            } else {
                rows.min(cap_subjects).max(1)
            };
            insert(
                name,
                PropertyStatData {
                    count: rows,
                    ndv_values: ndv.max(1),
                    ndv_subjects: subjects.max(1),
                },
            );
        };
        opt(
            p::REIFIES_GRAPH,
            ann.reifies_graph_rows,
            ann.distinct_reified_graphs,
            ann.distinct_graph_anns,
        );
        // `f:reifiesDatatype` is intentionally skipped — arena
        // builder reports zeros (see `AnnotationStats::reifies_datatype_rows`).
        opt(
            p::REIFIES_LANG,
            ann.reifies_lang_rows,
            ann.distinct_reified_langs,
            ann.distinct_lang_anns,
        );
        opt(
            p::REIFIES_LIST_INDEX,
            ann.reifies_list_index_rows,
            ann.distinct_reified_list_indices,
            ann.distinct_list_index_anns,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index_stats::{ClassStatEntry, PropertyStatEntry};

    #[test]
    fn test_empty_stats() {
        let stats = IndexStats {
            flakes: 0,
            size: 0,
            properties: None,
            classes: None,
            graphs: None,
        };
        let view = StatsView::from_db_stats(&stats);
        assert!(!view.has_property_stats());
        assert!(!view.has_class_stats());
    }

    #[test]
    fn test_property_lookup() {
        let stats = IndexStats {
            flakes: 100,
            size: 1000,
            properties: Some(vec![PropertyStatEntry {
                sid: (1, "name".to_string()),
                count: 50,
                ndv_values: 40,
                ndv_subjects: 45,
                last_modified_t: 10,
                datatypes: vec![],
            }]),
            classes: None,
            graphs: None,
        };
        let view = StatsView::from_db_stats(&stats);
        assert!(view.has_property_stats());

        let sid = Sid::new(1, "name");
        let prop = view.get_property(&sid).unwrap();
        assert_eq!(prop.count, 50);
        assert_eq!(prop.ndv_values, 40);
        assert_eq!(prop.ndv_subjects, 45);
    }

    #[test]
    fn merge_annotation_stats_uses_per_slot_ndv_for_required() {
        use fluree_vocab::db as p;
        use fluree_vocab::namespaces::FLUREE_DB;

        // 800 annotations across 200 edges (one target per ann
        // under the v1 invariant, so live_attachment_pairs == 800):
        // 50 distinct subjects, 4 distinct predicates, 200 distinct
        // objects. The planner's BoundObject formula
        // `count / ndv_values` should give:
        //   reifiesSubject:   800 /  50 = 16 annotations per subject
        //   reifiesPredicate: 800 /   4 = 200 annotations per predicate
        //   reifiesObject:    800 / 200 = 4 annotations per object
        let mut view = StatsView::default();
        let ann = AnnotationStats {
            forward_rows: 1_000,
            reverse_rows: 1_000,
            distinct_edges: 200,
            distinct_annotations: 800,
            live_attachment_pairs: 800,
            distinct_reified_subjects: 50,
            distinct_reified_predicates: 4,
            distinct_reified_objects: 200,
            ..Default::default()
        };
        let mut ns = HashMap::new();
        ns.insert(FLUREE_DB, "https://ns.flur.ee/db#".to_string());

        view.merge_annotation_stats(&ann, &ns);

        let subj = view
            .get_property(&Sid::new(FLUREE_DB, p::REIFIES_SUBJECT))
            .expect("reifiesSubject synth missing");
        assert_eq!(subj.count, 800);
        assert_eq!(subj.ndv_subjects, 800);
        assert_eq!(
            subj.ndv_values, 50,
            "reifiesSubject ndv = distinct subjects"
        );

        let pred = view
            .get_property(&Sid::new(FLUREE_DB, p::REIFIES_PREDICATE))
            .expect("reifiesPredicate synth missing");
        assert_eq!(
            pred.ndv_values, 4,
            "reifiesPredicate ndv = distinct predicates"
        );

        let obj = view
            .get_property(&Sid::new(FLUREE_DB, p::REIFIES_OBJECT))
            .expect("reifiesObject synth missing");
        assert_eq!(obj.ndv_values, 200, "reifiesObject ndv = distinct objects");

        // Optional slots: zero rows in this fixture → not synthesized,
        // falls through to whatever IndexStats.properties holds.
        for name in [
            p::REIFIES_GRAPH,
            p::REIFIES_DATATYPE,
            p::REIFIES_LANG,
            p::REIFIES_LIST_INDEX,
        ] {
            assert!(
                view.get_property(&Sid::new(FLUREE_DB, name)).is_none(),
                "optional slot {name} must not be synth'd when row count is zero"
            );
        }
    }

    #[test]
    fn merge_annotation_stats_uses_pair_count_when_multi_target_anomaly() {
        // Anomalous shape: 100 distinct annotation SIDs, but one of
        // them is attached to 3 different edges (legacy / replayed-
        // from-corrupt-history — the v1 stage-time invariant should
        // prevent this on healthy ledgers). live_attachment_pairs
        // is 102, which is the correct row count for the required
        // slots. The planner should see count = 102, not 100, so
        // BoundObject estimates don't undercount.
        use fluree_vocab::db as p;
        use fluree_vocab::namespaces::FLUREE_DB;

        let mut view = StatsView::default();
        let ann = AnnotationStats {
            distinct_edges: 100,
            distinct_annotations: 100,
            live_attachment_pairs: 102,
            distinct_reified_subjects: 100,
            distinct_reified_predicates: 5,
            distinct_reified_objects: 100,
            ..Default::default()
        };
        view.merge_annotation_stats(&ann, &HashMap::new());

        let subj = view
            .get_property(&Sid::new(FLUREE_DB, p::REIFIES_SUBJECT))
            .expect("reifiesSubject synth missing");
        assert_eq!(
            subj.count, 102,
            "row count must follow live_attachment_pairs, not distinct_annotations"
        );
        assert_eq!(
            subj.ndv_subjects, 100,
            "ndv_subjects = distinct ann SIDs (subject of each row)"
        );
    }

    #[test]
    fn merge_annotation_stats_falls_back_to_distinct_annotations_for_old_arenas() {
        // Older arena roots predate `live_attachment_pairs` and
        // deserialize as 0. Under the v1 invariant the pair count
        // equals distinct_annotations, so the merge falls back to
        // distinct_annotations as the row count.
        use fluree_vocab::db as p;
        use fluree_vocab::namespaces::FLUREE_DB;

        let mut view = StatsView::default();
        let ann = AnnotationStats {
            distinct_annotations: 50,
            // live_attachment_pairs missing (== 0)
            distinct_reified_subjects: 10,
            ..Default::default()
        };
        view.merge_annotation_stats(&ann, &HashMap::new());
        let subj = view
            .get_property(&Sid::new(FLUREE_DB, p::REIFIES_SUBJECT))
            .expect("reifiesSubject synth missing");
        assert_eq!(
            subj.count, 50,
            "older arena: count falls back to distinct_annotations"
        );
    }

    #[test]
    fn merge_annotation_stats_uses_per_slot_ann_count_for_sparse_anomaly() {
        // Reviewer's case: 1000 annotations total, but only one of
        // them carries a graph slot — and that one is attached to
        // 21 distinct named-graph edges (the multi-target anomaly).
        // So `reifies_graph_rows = 21`, `distinct_graph_anns = 1`.
        // Without the per-slot ann counter, the cap fallback
        // `min(rows, distinct_annotations) = min(21, 1000) = 21`
        // would set ndv_subjects = 21, making BoundSubject estimate
        // `21 / 21 = 1` row when the true answer for the anomalous
        // ann is 21 rows. With the per-slot counter, ndv_subjects
        // = 1 → BoundSubject = 21 / 1 = 21. Exact.
        use fluree_vocab::db as p;
        use fluree_vocab::namespaces::FLUREE_DB;

        let mut view = StatsView::default();
        let ann = AnnotationStats {
            distinct_edges: 1020,
            distinct_annotations: 1000,
            live_attachment_pairs: 1020,
            distinct_reified_subjects: 1000,
            distinct_reified_predicates: 5,
            distinct_reified_objects: 1020,
            reifies_graph_rows: 21,
            distinct_reified_graphs: 1,
            distinct_graph_anns: 1,
            ..Default::default()
        };
        view.merge_annotation_stats(&ann, &HashMap::new());

        let graph = view
            .get_property(&Sid::new(FLUREE_DB, p::REIFIES_GRAPH))
            .expect("reifiesGraph synth missing");
        assert_eq!(graph.count, 21);
        assert_eq!(
            graph.ndv_subjects, 1,
            "ndv_subjects must use distinct_graph_anns, not the row count"
        );
    }

    #[test]
    fn merge_annotation_stats_caps_optional_slot_subjects_under_multi_target() {
        // Multi-target anomaly: 50 distinct annotation SIDs but
        // 70 live `(edge, ann)` pairs (one ann is attached to 21
        // distinct named-graph edges = 21 rows from a single
        // subject). All 70 pairs are in named graphs across 3
        // distinct graph SIDs, so reifies_graph_rows = 70.
        // Under the old `ndv_subjects = rows` rule, the planner
        // would compute `BoundSubject` selectivity as
        // `70 / 70 = 1` row per known annotation, when the actual
        // number of rows per known annotation can be 21. With the
        // cap, ndv_subjects = min(70, 50) = 50, giving
        // `70 / 50 ≈ 2` per known annotation — closer to the
        // truth, never an undercount.
        use fluree_vocab::db as p;
        use fluree_vocab::namespaces::FLUREE_DB;

        let mut view = StatsView::default();
        let ann = AnnotationStats {
            distinct_edges: 70,
            distinct_annotations: 50,
            live_attachment_pairs: 70,
            distinct_reified_subjects: 50,
            distinct_reified_predicates: 5,
            distinct_reified_objects: 70,
            reifies_graph_rows: 70,
            distinct_reified_graphs: 3,
            ..Default::default()
        };
        view.merge_annotation_stats(&ann, &HashMap::new());

        let graph = view
            .get_property(&Sid::new(FLUREE_DB, p::REIFIES_GRAPH))
            .expect("reifiesGraph synth missing");
        assert_eq!(graph.count, 70);
        assert_eq!(graph.ndv_values, 3);
        assert_eq!(
            graph.ndv_subjects, 50,
            "ndv_subjects must be capped by distinct_annotations under multi-target"
        );
    }

    #[test]
    fn merge_annotation_stats_falls_back_when_per_slot_ndv_absent() {
        // Older arena roots written before per-slot NDV tracking
        // landed deserialize with zeroed `distinct_reified_*`. The
        // merge must treat zero as "no information" and fall back to
        // the safe `ndv_values = 1` upper bound rather than producing
        // a degenerate `count / 0` estimate.
        use fluree_vocab::db as p;
        use fluree_vocab::namespaces::FLUREE_DB;

        let mut view = StatsView::default();
        let ann = AnnotationStats {
            distinct_edges: 200,
            distinct_annotations: 800,
            // distinct_reified_* all default to 0
            ..Default::default()
        };
        view.merge_annotation_stats(&ann, &HashMap::new());

        for name in [p::REIFIES_SUBJECT, p::REIFIES_PREDICATE, p::REIFIES_OBJECT] {
            let entry = view
                .get_property(&Sid::new(FLUREE_DB, name))
                .unwrap_or_else(|| panic!("{name} synth missing"));
            assert_eq!(entry.count, 800);
            assert_eq!(
                entry.ndv_values, 1,
                "{name} ndv_values must fall back to 1 when arena reports zero"
            );
        }
    }

    #[test]
    fn merge_annotation_stats_synthesizes_optional_slots_when_present() {
        // 100 annotations, 20 of them in named graphs across 3
        // distinct graph SIDs, 10 with langString objects across 2
        // distinct languages. The optional-slot synth uses (rows,
        // ndv) directly rather than `distinct_annotations`.
        use fluree_vocab::db as p;
        use fluree_vocab::namespaces::FLUREE_DB;

        let mut view = StatsView::default();
        let ann = AnnotationStats {
            distinct_edges: 100,
            distinct_annotations: 100,
            distinct_reified_subjects: 100,
            distinct_reified_predicates: 5,
            distinct_reified_objects: 100,
            reifies_graph_rows: 20,
            distinct_reified_graphs: 3,
            reifies_lang_rows: 10,
            distinct_reified_langs: 2,
            // datatype + listIndex omitted → stays 0
            ..Default::default()
        };
        view.merge_annotation_stats(&ann, &HashMap::new());

        let graph = view
            .get_property(&Sid::new(FLUREE_DB, p::REIFIES_GRAPH))
            .expect("reifiesGraph synth missing");
        assert_eq!(graph.count, 20, "reifiesGraph count = rows in live state");
        assert_eq!(graph.ndv_subjects, 20);
        assert_eq!(graph.ndv_values, 3);

        let lang = view
            .get_property(&Sid::new(FLUREE_DB, p::REIFIES_LANG))
            .expect("reifiesLang synth missing");
        assert_eq!(lang.count, 10);
        assert_eq!(lang.ndv_values, 2);

        // listIndex still zero rows → not synthesized.
        assert!(view
            .get_property(&Sid::new(FLUREE_DB, p::REIFIES_LIST_INDEX))
            .is_none());

        // f:reifiesDatatype is never synthesized from the arena even
        // when the caller hands non-zero counts — the arena builder
        // emits zero by contract because it cannot reliably observe
        // the on-wire flake count. Pin that the synth path skips it
        // unconditionally.
        let mut view2 = StatsView::default();
        let ann_with_dt = AnnotationStats {
            distinct_annotations: 100,
            reifies_datatype_rows: 100, // hypothetical — builder never sets this
            distinct_reified_datatypes: 4,
            ..Default::default()
        };
        view2.merge_annotation_stats(&ann_with_dt, &HashMap::new());
        assert!(
            view2
                .get_property(&Sid::new(FLUREE_DB, p::REIFIES_DATATYPE))
                .is_none(),
            "reifiesDatatype must never be synthesized from arena stats"
        );
    }

    #[test]
    fn merge_annotation_stats_zero_annotations_is_noop() {
        let mut view = StatsView::default();
        let ann = AnnotationStats::default();
        let ns = HashMap::new();
        view.merge_annotation_stats(&ann, &ns);
        assert!(view.properties.is_empty());
        assert!(view.properties_by_iri.is_empty());
    }

    #[test]
    fn test_class_lookup() {
        let class_sid = Sid::new(2, "Person");
        let stats = IndexStats {
            flakes: 100,
            size: 1000,
            properties: None,
            classes: Some(vec![ClassStatEntry {
                class_sid: class_sid.clone(),
                count: 25,
                properties: vec![],
            }]),
            graphs: None,
        };
        let view = StatsView::from_db_stats(&stats);
        assert!(view.has_class_stats());

        let count = view.get_class_count(&class_sid).unwrap();
        assert_eq!(count, 25);
    }
}
