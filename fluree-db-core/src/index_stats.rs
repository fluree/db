//! Index statistics types.
//!
//! These types describe per-property, per-class, and per-graph statistics
//! collected during indexing. They are used by the query planner for
//! selectivity estimation and by the index root for metadata.

use crate::ids::GraphId;
use crate::sid::Sid;

// === Per-Property Statistics ===

/// Per-property statistics entry.
///
/// Contains HLL-derived NDV estimates for a single property.
/// Stored sorted by SID for determinism.
#[derive(Debug, Clone)]
pub struct PropertyStatEntry {
    /// Predicate SID as (namespace_code, name).
    pub sid: (u16, String),
    /// Total number of flakes with this property (including history).
    pub count: u64,
    /// Estimated number of distinct object values (via HLL).
    pub ndv_values: u64,
    /// Estimated number of distinct subjects using this property (via HLL).
    pub ndv_subjects: u64,
    /// Most recent transaction time that modified this property.
    pub last_modified_t: i64,
    /// Per-datatype flake counts for this property (ValueTypeTag.0, count).
    ///
    /// This is the **ledger-wide aggregate** view (across all graphs).
    /// Graph-scoped property stats (authoritative for range narrowing) live under
    /// `IndexStats.graphs[*].properties[*].datatypes`.
    pub datatypes: Vec<(u8, u64)>,
}

// === Index Statistics ===

/// Index statistics (fast estimates).
///
/// Maintained incrementally during indexing/refresh. Must not require walking
/// the full index tree.
///
/// - `flakes`: total flakes in the index (including history; after dedup)
/// - `size`: estimated total bytes of flakes (speed over accuracy)
/// - `properties`: per-property HLL statistics (optional)
/// - `classes`: per-class property usage statistics (optional)
/// - `graphs`: per-graph ID-based statistics (authoritative)
#[derive(Debug, Clone, Default)]
pub struct IndexStats {
    /// Total number of flakes in the index (including history; after dedup).
    pub flakes: u64,
    /// Estimated total bytes of flakes in the index (not storage bytes of index nodes).
    pub size: u64,
    /// DEPRECATED: Sid-keyed aggregate view, kept for backward compatibility with
    /// StatsView, current_stats(), and the query planner. Derived from `graphs`
    /// when both are present. Will be removed once all consumers migrate to
    /// ID-based lookups via `graphs`.
    pub properties: Option<Vec<PropertyStatEntry>>,
    /// Per-class property usage statistics (sorted by class SID for determinism).
    /// Tracks which properties are used by instances of each class.
    ///
    /// IMPORTANT: Detailed per-property stats (counts/NDV/datatypes) must live in
    /// `graphs[*].properties` (graph-scoped) and NOT under classes.
    pub classes: Option<Vec<ClassStatEntry>>,
    /// Per-graph statistics keyed by numeric IDs (authoritative, ID-based).
    /// Each entry contains per-property stats including datatype usage.
    /// Sorted by g_id for determinism.
    pub graphs: Option<Vec<GraphStatsEntry>>,
}

impl IndexStats {
    /// Set `self.size` to `total_commit_size` and proportionally distribute it
    /// across `self.graphs[*].size` based on each graph's flake count.
    ///
    /// The per-graph allocation is an estimate (not exact storage bytes), but
    /// it avoids reporting 0 and stays consistent across the root_assembly,
    /// incremental, and import code paths. The last graph absorbs the
    /// remainder so the sum equals `total_commit_size` exactly.
    ///
    /// No-op for the per-graph distribution if there are no graphs, no flakes,
    /// or `total_commit_size` is zero; `self.size` is still set in all cases.
    pub fn distribute_total_size_by_flakes(&mut self, total_commit_size: u64) {
        self.size = total_commit_size;
        let Some(graphs) = self.graphs.as_mut() else {
            return;
        };
        let total_flakes: u64 = graphs.iter().map(|g| g.flakes).sum();
        if total_flakes == 0 || total_commit_size == 0 {
            return;
        }
        let n = graphs.len();
        let mut assigned: u64 = 0;
        for (i, g) in graphs.iter_mut().enumerate() {
            if i + 1 == n {
                g.size = total_commit_size.saturating_sub(assigned);
            } else {
                let part = ((total_commit_size as u128) * (g.flakes as u128)
                    / (total_flakes as u128)) as u64;
                g.size = part;
                assigned = assigned.saturating_add(part);
            }
        }
    }
}

// === Class-Property Statistics ===

/// Statistics for a single class (rdf:type target).
///
/// Tracks property usage patterns for instances of this class.
/// Used for query optimization (selectivity estimation) and schema inference.
#[derive(Debug, Clone)]
pub struct ClassStatEntry {
    /// The class SID (target of rdf:type assertions).
    pub class_sid: Sid,
    /// Number of instances of this class.
    pub count: u64,
    /// Properties used by instances of this class (sorted by property SID).
    pub properties: Vec<ClassPropertyUsage>,
}

/// Property usage within a class.
///
/// Intentionally avoids duplicating full per-property stats (counts/NDV/datatypes),
/// which are tracked in graph-scoped property stats (`IndexStats.graphs`).
///
/// This structure is meant for:
/// - class-policy indexing (which properties appear on instances of a class)
/// - ontology / schema visualization (class→property edges, ref target classes)
/// - datatype distribution (which datatypes appear for this property on instances of this class)
/// - language tag distribution (which language tags appear for this property on instances of this class)
#[derive(Debug, Clone)]
pub struct ClassPropertyUsage {
    /// The property SID.
    pub property_sid: Sid,
    /// Per-datatype flake counts. `u8` = `ValueTypeTag::as_u8()`.
    /// `JSON_LD_ID` (16) for `@id` references. Sorted by tag.
    pub datatypes: Vec<(u8, u64)>,
    /// Per-language-tag flake counts. Strings stored directly.
    /// Sorted by lang string.
    pub langs: Vec<(String, u64)>,
    /// For reference-valued properties, counts by target class.
    ///
    /// Each entry indicates how many reference assertions of this property (from
    /// instances of the owning class) point to instances of the target class.
    ///
    /// Stored sorted by `class_sid` for determinism.
    pub ref_classes: Vec<ClassRefCount>,
}

/// Reference target class counts for a class-scoped property.
#[derive(Debug, Clone)]
pub struct ClassRefCount {
    /// Target class SID (rdf:type of the referenced object).
    pub class_sid: Sid,
    /// Count of reference assertions pointing to this class.
    pub count: u64,
}

// === Graph-Scoped Statistics (ID-Based) ===

/// Per-property stats within a graph, keyed by numeric IDs from GlobalDicts.
///
/// This is the authoritative ID-based stats format. The Sid-keyed
/// `PropertyStatEntry` is a deprecated interim adapter.
#[derive(Debug, Clone)]
pub struct GraphPropertyStatEntry {
    /// Predicate dictionary ID (from GlobalDicts.predicates).
    pub p_id: u32,
    /// Total number of asserted flakes with this property (after dedup; retractions decrement).
    pub count: u64,
    /// Estimated number of distinct object values (via HLL).
    pub ndv_values: u64,
    /// Estimated number of distinct subjects using this property (via HLL).
    pub ndv_subjects: u64,
    /// Most recent transaction time that modified this property.
    pub last_modified_t: i64,
    /// Per-datatype flake counts: (ValueTypeTag.0, count).
    pub datatypes: Vec<(u8, u64)>,
}

/// Stats for a single named graph within a ledger.
///
/// Each entry corresponds to one graph in the binary index, identified
/// by `g_id` from GlobalDicts.graphs (0 = default, 1 = txn-meta).
#[derive(Debug, Clone)]
pub struct GraphStatsEntry {
    /// Graph dictionary ID (0 = default graph).
    pub g_id: GraphId,
    /// Total number of flakes in this graph (after dedup).
    pub flakes: u64,
    /// Estimated byte size of flakes in this graph in the binary index.
    /// Set to 0 in the pre-index manifest (binary index not yet built).
    /// Populated by index build/refresh.
    pub size: u64,
    /// Per-property statistics within this graph (sorted by p_id for determinism).
    pub properties: Vec<GraphPropertyStatEntry>,
    /// Per-graph class statistics (sorted by class_sid for determinism).
    /// `None` when class tracking was not enabled or no classes exist in this graph.
    pub classes: Option<Vec<ClassStatEntry>>,
}

// === Helpers ===

/// Derive a ledger-wide class stats list from per-graph `GraphStatsEntry`s.
///
/// Unions class counts (summed), property lists (unioned), and ref-class
/// counts (summed) across all graphs. Returns `None` if no graphs contain
/// class stats.
///
/// Used by both the full-build and incremental paths for backward-compatible
/// root-level `IndexStats.classes`.
pub fn union_per_graph_classes(graphs: &[GraphStatsEntry]) -> Option<Vec<ClassStatEntry>> {
    let slices: Vec<&[ClassStatEntry]> =
        graphs.iter().filter_map(|g| g.classes.as_deref()).collect();
    union_class_stat_slices(&slices)
}

/// Union multiple class stats slices into a single ledger-wide list.
///
/// Each input slice represents one graph's class stats. The function merges
/// class counts (summed), property lists (unioned), and per-property ref-class
/// counts (summed) across all slices.
///
/// Canonical sort order: entries by `class_sid`, properties by `property_sid`,
/// ref_classes by `class_sid`.
pub fn union_class_stat_slices(slices: &[&[ClassStatEntry]]) -> Option<Vec<ClassStatEntry>> {
    use std::collections::HashMap;

    let mut merged: HashMap<Sid, ClassStatEntry> = HashMap::new();

    for &slice in slices {
        for entry in slice {
            let e = merged
                .entry(entry.class_sid.clone())
                .or_insert_with(|| ClassStatEntry {
                    class_sid: entry.class_sid.clone(),
                    count: 0,
                    properties: Vec::new(),
                });
            e.count += entry.count;

            for prop in &entry.properties {
                if let Some(existing) = e
                    .properties
                    .iter_mut()
                    .find(|p| p.property_sid == prop.property_sid)
                {
                    // Merge datatypes: sum counts per tag.
                    for &(tag, count) in &prop.datatypes {
                        if let Some(edt) = existing.datatypes.iter_mut().find(|d| d.0 == tag) {
                            edt.1 += count;
                        } else {
                            existing.datatypes.push((tag, count));
                        }
                    }
                    // Merge langs: sum counts per lang string.
                    for (lang, count) in &prop.langs {
                        if let Some(el) = existing.langs.iter_mut().find(|l| &l.0 == lang) {
                            el.1 += count;
                        } else {
                            existing.langs.push((lang.clone(), *count));
                        }
                    }
                    // Merge ref_classes: sum counts per target class.
                    for rc in &prop.ref_classes {
                        if let Some(erc) = existing
                            .ref_classes
                            .iter_mut()
                            .find(|r| r.class_sid == rc.class_sid)
                        {
                            erc.count += rc.count;
                        } else {
                            existing.ref_classes.push(rc.clone());
                        }
                    }
                } else {
                    e.properties.push(ClassPropertyUsage {
                        property_sid: prop.property_sid.clone(),
                        datatypes: prop.datatypes.clone(),
                        langs: prop.langs.clone(),
                        ref_classes: prop.ref_classes.clone(),
                    });
                }
            }
        }
    }

    // Canonical sort for determinism.
    for e in merged.values_mut() {
        e.properties
            .sort_by(|a, b| a.property_sid.cmp(&b.property_sid));
        for p in &mut e.properties {
            p.datatypes.sort_by_key(|d| d.0);
            p.langs.sort_by(|a, b| a.0.cmp(&b.0));
            p.ref_classes.sort_by(|a, b| a.class_sid.cmp(&b.class_sid));
        }
    }

    let mut entries: Vec<ClassStatEntry> = merged.into_values().collect();
    entries.sort_by(|a, b| a.class_sid.cmp(&b.class_sid));

    if entries.is_empty() {
        None
    } else {
        Some(entries)
    }
}
