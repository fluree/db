//! Pre-built statistics lookup for query optimization.
//!
//! `StatsView` provides O(1) lookups of property and class statistics,
//! built from `IndexStats` at query time.

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

        size_of::<Self>()
            + properties
            + classes
            + properties_by_iri
            + classes_by_iri
            + graph_properties
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
                    sid,
                    PropertyStatData {
                        count: entry.count,
                        ndv_values: entry.ndv_values,
                        ndv_subjects: entry.ndv_subjects,
                    },
                );
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

        view
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
