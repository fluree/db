//! Pre-built statistics lookup for query optimization.
//!
//! `StatsView` provides O(1) lookups of property and class statistics,
//! built from `IndexStats` at query time.

use crate::db::LedgerSnapshot;
use crate::ids::{GraphId, RuntimePredicateId};
use crate::index_stats::{ClassStatEntry, IndexStats};
use crate::ns_encoding::{canonical_split, NsSplitMode};
use crate::sid::Sid;
use crate::value_id::ValueTypeTag;
use std::collections::HashMap;
use std::sync::Arc;

/// Resolves a query-side IRI to the `Sid` that `IndexStats` is keyed by.
///
/// Must stay equivalent to [`LedgerSnapshot::encode_iri`], including its
/// EMPTY-namespace fallback for bare names.
#[derive(Debug, Clone)]
pub struct StatsIriEncoder {
    /// IRI prefix -> namespace code, shared with the snapshot.
    reverse: Arc<HashMap<String, u16>>,
    split_mode: NsSplitMode,
}

impl StatsIriEncoder {
    /// Build an encoder from a snapshot's namespace table.
    pub fn from_snapshot(snapshot: &LedgerSnapshot) -> Self {
        Self {
            reverse: snapshot.shared_namespace_reverse(),
            split_mode: snapshot.ns_split_mode(),
        }
    }

    /// Encode an IRI to the SID the stats tables are keyed by.
    pub fn encode(&self, iri: &str) -> Sid {
        let (prefix, suffix) = canonical_split(iri, self.split_mode);
        match self.reverse.get(prefix) {
            Some(&code) => Sid::new(code, suffix),
            None => Sid::new(fluree_vocab::namespaces::EMPTY, iri),
        }
    }
}

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
    /// Graph-scoped property stats keyed by runtime predicate IDs.
    ///
    /// Populated from `IndexStats.graphs` when present. Provides per-graph
    /// property lookups with datatype breakdown. The aggregate Sid-keyed
    /// `properties` map remains the primary source for the query planner.
    pub graph_properties: HashMap<GraphId, HashMap<RuntimePredicateId, GraphPropertyStatData>>,
    /// Property SID -> whether every object of this property is a node/IRI ref
    /// (all datatype tags are [`ValueTypeTag::JSON_LD_ID`]). Derived from
    /// [`crate::index_stats::PropertyStatEntry::observed_datatypes`], the tag set that is monotone
    /// under retraction — not from the `datatypes` counts, which novelty merges
    /// as a blind ±1 delta log and can therefore under-report. Used by the
    /// equijoin-filter fold to rewrite `FILTER(?x = ?y)` into a join only when
    /// value-equality coincides with term-equality (true for nodes).
    ///
    /// A `true` here is a soundness licence, so it is only ever allowed to be
    /// conservative: a property that has carried a literal reads `false` until a
    /// reindex genuinely removes the tag from the base index.
    ///
    /// That holds for a current-state read. It does not hold by itself for time
    /// travel: the base index's tag set is current state *as of the publish*,
    /// so a predicate whose literals were legitimately deleted before that
    /// publish carries no literal tag, and a query at an earlier `t` — which
    /// can still see those literals — would read `true`. A builder serving a
    /// read below the published index `t` has to substitute the persisted
    /// `historical_datatypes` set (sound for every `t` at or above
    /// `IndexStats::historical_since_t`) for `observed_datatypes` first — or,
    /// below that boundary, clear it so the flag falls back to "unknown";
    /// `fluree-db-query`'s `cached_stats_view_for_db` is the one that does.
    pub property_ref_only: HashMap<Sid, bool>,
    /// Property IRI -> ref-only flag (see [`Self::property_ref_only`]).
    pub property_ref_only_by_iri: HashMap<Arc<str>, bool>,
    /// The `IndexStats` this view was derived from. Per-class property usage
    /// is read from here on demand rather than copied into the view, since it
    /// grows with the number of distinct classes.
    pub source: Option<Arc<IndexStats>>,
    /// IRI -> SID resolution for the by-IRI accessors. `None` for views built
    /// without a namespace table, where those accessors report "unknown".
    pub iri_encoder: Option<StatsIriEncoder>,
    /// True only when the class/property counts reflect exact current state with
    /// no overlay gap — novelty empty and no policy visibility layer. Set by the
    /// query stats-cache builder; defaults `false` so any caller that does not
    /// explicitly vouch for current-state exactness never triggers elision.
    pub class_coverage_trustworthy: bool,
    /// Inner predicate SID -> live `rdf:reifies` links whose triple term has
    /// it. `None` when the stats carry no link counts.
    pub links: Option<HashMap<Sid, u64>>,
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
    /// Per-datatype flake counts. Estimates: on the query path these are
    /// novelty-merged as a blind ±1 delta log, so a spurious retraction can
    /// drop a tag whose data still exists. Sum them; never read them as a set.
    pub datatypes: Vec<(ValueTypeTag, u64)>,
    /// The datatype tags this property carries in this graph — the set
    /// consumers must read instead of `datatypes` when the answer gates a
    /// rewrite (scan narrowing to an exact datatype). Sourced from
    /// [`crate::index_stats::GraphPropertyStatEntry::observed_datatypes`],
    /// which is monotone under retraction within the novelty window and
    /// substituted with the historical set (or cleared) for reads below the
    /// published index `t`. Empty means "unknown": fail closed.
    pub observed_datatypes: Vec<ValueTypeTag>,
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
                            + data.observed_datatypes.len() * size_of::<ValueTypeTag>()
                    })
                    .sum::<usize>()
            })
            .sum::<usize>();

        // `source` is not counted: the planner's builder keeps it only when it
        // is the snapshot's own stats, which evicting the view would not free.
        size_of::<Self>() + properties + classes + properties_by_iri + graph_properties
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
                // Read the *observed tag set*, never the `datatypes` counts: the
                // counts are novelty-merged as a blind ±1 delta log, so a
                // retraction of a fact that was never asserted can zero out a
                // literal tag and make a mixed property read as all-ref. The tag
                // set is monotone under retraction, so it cannot. Empty
                // (unknown) => not provably ref-only.
                let ref_only = !entry.observed_datatypes.is_empty()
                    && entry
                        .observed_datatypes
                        .iter()
                        .all(|&dt| dt == ValueTypeTag::JSON_LD_ID.as_u8());
                view.property_ref_only.insert(sid, ref_only);
            }
        }

        if let Some(ref classes) = stats.classes {
            for entry in classes {
                view.classes.insert(entry.class_sid.clone(), entry.count);
            }
        }

        view.links = stats.links.as_ref().map(|links| {
            links
                .iter()
                .map(|l| (Sid::new(l.sid.0, &l.sid.1), l.count))
                .collect()
        });

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
                            observed_datatypes: p_entry
                                .observed_datatypes
                                .iter()
                                .map(|&dt| ValueTypeTag::from_u8(dt))
                                .collect(),
                        },
                    );
                }
                view.graph_properties.insert(g_entry.g_id, prop_map);
            }
        }

        view
    }

    /// Build from `stats` (which may be a novelty-merged copy) using
    /// `snapshot`'s namespace table.
    ///
    /// Only predicate lookups are materialized under IRI keys, because those
    /// are bounded by the schema. Class lookups by IRI encode the IRI per call
    /// instead, because classes can number in the millions.
    pub fn from_db_stats_with_namespaces(
        stats: &Arc<IndexStats>,
        snapshot: &LedgerSnapshot,
    ) -> Self {
        let mut view = StatsView::from_db_stats(stats);
        let namespace_codes = snapshot.namespaces();

        // Derive IRI-keyed property stats.
        // If a SID's namespace code is missing, skip it.
        for (sid, data) in &view.properties {
            if let Some(prefix) = namespace_codes.get(&sid.namespace_code) {
                let iri: Arc<str> = Arc::from(format!("{}{}", prefix, sid.name));
                view.properties_by_iri.insert(iri, *data);
            }
        }

        // Derive IRI-keyed ref-only flags.
        for (sid, ref_only) in &view.property_ref_only {
            if let Some(prefix) = namespace_codes.get(&sid.namespace_code) {
                let iri: Arc<str> = Arc::from(format!("{}{}", prefix, sid.name));
                view.property_ref_only_by_iri.insert(iri, *ref_only);
            }
        }

        #[cfg(debug_assertions)]
        if let Some(classes) = stats.classes.as_deref() {
            debug_assert!(
                classes.windows(2).all(|w| w[0].class_sid < w[1].class_sid),
                "IndexStats.classes must be strictly sorted by class_sid"
            );
        }

        view.source = Some(Arc::clone(stats));
        view.iri_encoder = Some(StatsIriEncoder::from_snapshot(snapshot));
        view
    }

    /// Live `rdf:reifies` links whose triple term has inner predicate `p`;
    /// `None` when the stats carry no link counts.
    pub fn link_count(&self, p: &Sid) -> Option<u64> {
        self.links
            .as_ref()
            .map(|links| links.get(p).copied().unwrap_or(0))
    }

    /// The per-class stats entry for `class_sid`, by binary search over the
    /// class table, which every producer sorts by `class_sid`. A miss reads as
    /// "no proof" to every caller, so an unsorted table declines rewrites
    /// rather than licensing wrong ones.
    fn class_entry(&self, class_sid: &Sid) -> Option<&ClassStatEntry> {
        let classes = self.source.as_deref()?.classes.as_deref()?;
        let idx = classes
            .binary_search_by(|entry| entry.class_sid.cmp(class_sid))
            .ok()?;
        Some(&classes[idx])
    }

    /// Flakes of `property_sid` whose subject is an instance of `class_sid`,
    /// summed across graphs. Zero when either is unknown to stats.
    fn class_property_flakes(&self, class_sid: &Sid, property_sid: &Sid) -> u64 {
        self.class_entry(class_sid)
            .and_then(|entry| {
                entry
                    .properties
                    .iter()
                    .find(|usage| &usage.property_sid == property_sid)
            })
            .map_or(0, |usage| usage.datatypes.iter().map(|&(_, c)| c).sum())
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
        let Some(encoder) = self.iri_encoder.as_ref() else {
            return false;
        };
        let covered =
            self.class_property_flakes(&encoder.encode(class_iri), &encoder.encode(pred_iri));
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

    /// Get class instance count by IRI. `None` when the view has no namespace
    /// table to resolve the IRI with, or the class is unknown to stats.
    pub fn get_class_count_by_iri(&self, iri: &str) -> Option<u64> {
        let sid = self.iri_encoder.as_ref()?.encode(iri);
        self.get_class_count(&sid)
    }

    /// Check if any property statistics are available.
    pub fn has_property_stats(&self) -> bool {
        !self.properties.is_empty()
    }

    /// Sum of per-property flake counts — an upper-bound estimate of the
    /// ledger's total triple count. Used to bound plans that consider a
    /// one-pass full sweep (e.g. the annotation hash probe's base-edge
    /// collection) against per-row probing.
    pub fn total_property_flakes(&self) -> u64 {
        self.properties.values().map(|p| p.count).sum()
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index_stats::{ClassStatEntry, PropertyStatEntry};

    fn entry_with(datatypes: Vec<(u8, u64)>, observed_datatypes: Vec<u8>) -> IndexStats {
        IndexStats {
            flakes: 1,
            size: 10,
            properties: Some(vec![PropertyStatEntry {
                sid: (1, "p".to_string()),
                count: 1,
                ndv_values: 1,
                ndv_subjects: 1,
                last_modified_t: 1,
                datatypes,
                observed_datatypes,
                historical_datatypes: vec![],
            }]),
            classes: None,
            graphs: None,
            historical_since_t: None,
            links: None,
        }
    }

    /// The ref-only flag is a soundness licence for the equijoin-filter fold, so
    /// it reads the observed-tag set — monotone under retraction — and not the
    /// `datatypes` counts, which the novelty merge can drive to zero for a tag
    /// whose data is still there.
    #[test]
    fn ref_only_reads_the_observed_tag_set_not_the_counts() {
        let ref_tag = ValueTypeTag::JSON_LD_ID.as_u8();
        let int_tag = ValueTypeTag::INTEGER.as_u8();
        let p = Sid::new(1, "p");

        // Counts say all-ref; the tag set remembers a literal. Not ref-only.
        let stats = entry_with(vec![(ref_tag, 5)], vec![int_tag, ref_tag]);
        assert_eq!(
            StatsView::from_db_stats(&stats).is_property_ref_only(&p),
            Some(false)
        );

        // Both agree it is all-ref.
        let stats = entry_with(vec![(ref_tag, 5)], vec![ref_tag]);
        assert_eq!(
            StatsView::from_db_stats(&stats).is_property_ref_only(&p),
            Some(true)
        );

        // No observed tags at all is "unknown", which must fail closed even
        // when the counts would have said all-ref.
        let stats = entry_with(vec![(ref_tag, 5)], vec![]);
        assert_eq!(
            StatsView::from_db_stats(&stats).is_property_ref_only(&p),
            Some(false)
        );
    }

    #[test]
    fn test_empty_stats() {
        let stats = IndexStats {
            flakes: 0,
            size: 0,
            properties: None,
            classes: None,
            graphs: None,
            historical_since_t: None,
            links: None,
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
                observed_datatypes: vec![],
                historical_datatypes: vec![],
            }]),
            classes: None,
            graphs: None,
            historical_since_t: None,
            links: None,
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
            historical_since_t: None,
            links: None,
        };
        let view = StatsView::from_db_stats(&stats);
        assert!(view.has_class_stats());

        let count = view.get_class_count(&class_sid).unwrap();
        assert_eq!(count, 25);
    }

    const EX: &str = "http://example.org/";

    fn ex_snapshot() -> LedgerSnapshot {
        let mut snapshot = LedgerSnapshot::genesis("stats-view:main");
        snapshot
            .insert_namespace_code(100, EX.to_string())
            .expect("register ex namespace");
        snapshot
    }

    fn usage(property: &str, flakes: u64) -> crate::index_stats::ClassPropertyUsage {
        crate::index_stats::ClassPropertyUsage {
            property_sid: Sid::new(100, property),
            datatypes: vec![(ValueTypeTag::JSON_LD_ID.as_u8(), flakes)],
            langs: vec![],
            ref_classes: vec![],
        }
    }

    fn ex_property(name: &str, count: u64) -> PropertyStatEntry {
        PropertyStatEntry {
            sid: (100, name.to_string()),
            count,
            ndv_values: 0,
            ndv_subjects: 0,
            last_modified_t: 1,
            datatypes: vec![],
            observed_datatypes: vec![],
            historical_datatypes: vec![],
        }
    }

    /// Class lookups by IRI resolve through the snapshot's namespace table, the
    /// same way `LedgerSnapshot::encode_iri` does — including the bare-name
    /// EMPTY-namespace fallback — rather than through a prebuilt IRI map.
    #[test]
    fn class_count_by_iri_resolves_through_namespace_table() {
        let snapshot = ex_snapshot();
        let stats = Arc::new(IndexStats {
            classes: Some(vec![
                ClassStatEntry {
                    class_sid: Sid::new(fluree_vocab::namespaces::EMPTY, "Bare"),
                    count: 3,
                    properties: vec![],
                },
                ClassStatEntry {
                    class_sid: Sid::new(100, "Person"),
                    count: 25,
                    properties: vec![],
                },
            ]),
            ..Default::default()
        });
        let view = StatsView::from_db_stats_with_namespaces(&stats, &snapshot);

        assert_eq!(
            view.get_class_count_by_iri(&format!("{EX}Person")),
            Some(25)
        );
        assert_eq!(view.get_class_count_by_iri("Bare"), Some(3));
        assert_eq!(view.get_class_count_by_iri(&format!("{EX}Nobody")), None);
        assert_eq!(
            view.get_class_count_by_iri("http://unregistered.example/Person"),
            None,
            "an IRI in an unregistered namespace must not alias a registered class"
        );

        // A view built without a namespace table cannot resolve IRIs at all.
        assert_eq!(
            StatsView::from_db_stats(&stats).get_class_count_by_iri(&format!("{EX}Person")),
            None
        );
    }

    /// The coverage proof behind `rdf:type` elision reads the per-class
    /// property usage from the shared source stats. It must hold exactly when
    /// the class accounts for every flake of the predicate, and fail closed on
    /// anything it cannot prove.
    #[test]
    fn predicate_coverage_reads_class_usage_from_source() {
        let snapshot = ex_snapshot();
        let stats = Arc::new(IndexStats {
            properties: Some(vec![ex_property("knows", 10), ex_property("name", 10)]),
            classes: Some(vec![
                ClassStatEntry {
                    class_sid: Sid::new(100, "Org"),
                    count: 1,
                    properties: vec![usage("name", 4)],
                },
                ClassStatEntry {
                    class_sid: Sid::new(100, "Person"),
                    count: 10,
                    properties: vec![usage("knows", 10), usage("name", 6)],
                },
            ]),
            ..Default::default()
        });
        let mut view = StatsView::from_db_stats_with_namespaces(&stats, &snapshot);
        let knows = format!("{EX}knows");
        let name = format!("{EX}name");
        let person = format!("{EX}Person");
        let org = format!("{EX}Org");

        assert!(
            !view.predicate_subjects_all_in_class_by_iri(&knows, &person),
            "never licensed without the current-state vouch"
        );

        view.class_coverage_trustworthy = true;
        assert!(view.predicate_subjects_all_in_class_by_iri(&knows, &person));
        assert!(
            !view.predicate_subjects_all_in_class_by_iri(&name, &person),
            "Person contributes 6 of 10 name flakes"
        );
        assert!(!view.predicate_subjects_all_in_class_by_iri(&knows, &org));
        assert!(!view.predicate_subjects_all_in_class_by_iri(&knows, &format!("{EX}Nobody")));
        assert!(!view.predicate_subjects_all_in_class_by_iri(&format!("{EX}unknown"), &person));
    }
}
