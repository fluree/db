//! Class-property statistics from novelty flakes and PSOT index.
//!
//! Tracks property usage per class from novelty flakes, optionally enriched
//! with class memberships from the PSOT index for subjects whose `rdf:type`
//! was asserted in prior transactions.

use std::collections::{HashMap, HashSet};

use fluree_db_core::comparator::IndexType;
use fluree_db_core::db::LedgerSnapshot;
use fluree_db_core::range::{range, RangeMatch, RangeOptions, RangeTest};
use fluree_db_core::{
    is_rdf_type, ClassPropertyUsage, ClassStatEntry, Flake, FlakeValue, GraphId, Sid,
};
use fluree_vocab::namespaces::RDF;
use fluree_vocab::predicates::RDF_TYPE;

/// Tracks property usage per class from novelty flakes
///
/// This extracts class-property statistics matching legacy `compute-class-property-stats-from-novelty`:
/// - Tracks rdf:type flakes to build subject→class mapping
/// - For each property used by a subject, tracks usage per class:
///   - Datatypes used (e.g., xsd:string, xsd:integer)
///   - Referenced classes (for @id refs)
///   - Language tags (for rdf:langString)
///
/// # Usage
///
/// ```ignore
/// let mut extractor = ClassPropertyExtractor::new();
/// // Or with prior stats:
/// let mut extractor = ClassPropertyExtractor::from_prior(db.stats.as_ref());
///
/// // First pass: collect rdf:type flakes to build subject→class map
/// for flake in novelty.iter() {
///     extractor.collect_type_flake(flake);
/// }
///
/// // Second pass: process all flakes with subject→class context
/// for flake in novelty.iter() {
///     extractor.process_flake(flake);
/// }
///
/// let class_stats = extractor.finalize();
/// ```
#[derive(Debug, Default)]
pub struct ClassPropertyExtractor {
    /// Subject SID → set of class SIDs (from rdf:type assertions in novelty)
    subject_classes: HashMap<Sid, HashSet<Sid>>,
    /// Class SID → class data (instance count, property usage)
    class_data: HashMap<Sid, ClassData>,
}

/// Internal class data during extraction
#[derive(Debug, Default)]
struct ClassData {
    /// Number of instances of this class (delta from novelty)
    count_delta: i64,
    /// Property usage: property SID → property data
    properties: HashMap<Sid, PropertyData>,
}

/// Internal property usage data during extraction
#[derive(Debug, Default)]
struct PropertyData {
    /// Count of asserted flakes for this (class, property) pair (delta).
    ///
    /// We intentionally do NOT track datatype/ref/lang breakdowns here; those live
    /// in graph-scoped property stats (`IndexStats.graphs[*].properties`).
    count_delta: i64,
}

impl ClassPropertyExtractor {
    /// Create a new empty class-property extractor
    pub fn new() -> Self {
        Self::default()
    }

    /// Create an extractor initialized with prior class stats
    ///
    /// Used during refresh to incrementally update class-property stats.
    pub fn from_prior(prior_stats: Option<&fluree_db_core::IndexStats>) -> Self {
        if let Some(stats) = prior_stats {
            if let Some(ref classes) = stats.classes {
                let mut class_data = HashMap::new();
                for class_entry in classes {
                    let mut props = HashMap::new();
                    for prop_usage in &class_entry.properties {
                        // Prior stats only carry the property identity. Treat presence as 1 so we
                        // preserve the property list, but do not attempt to "undo" it on retractions.
                        let prop_data = PropertyData { count_delta: 1 };
                        props.insert(prop_usage.property_sid.clone(), prop_data);
                    }
                    let data = ClassData {
                        count_delta: class_entry.count as i64,
                        properties: props,
                    };
                    class_data.insert(class_entry.class_sid.clone(), data);
                }
                return Self {
                    subject_classes: HashMap::new(),
                    class_data,
                };
            }
        }
        Self::default()
    }

    /// Collect rdf:type flakes to build subject→class mapping
    ///
    /// Call this for all novelty flakes BEFORE calling `process_flake`.
    /// Only processes rdf:type flakes; other flakes are ignored.
    pub fn collect_type_flake(&mut self, flake: &Flake) {
        if !is_rdf_type(&flake.p) {
            return;
        }

        if let FlakeValue::Ref(class_sid) = &flake.o {
            let classes = self.subject_classes.entry(flake.s.clone()).or_default();

            if flake.op {
                // Assertion: subject is instance of class
                classes.insert(class_sid.clone());
                // Update class count
                let class_data = self.class_data.entry(class_sid.clone()).or_default();
                class_data.count_delta += 1;
            } else {
                // Retraction: subject is no longer instance of class
                classes.remove(class_sid);
                if let Some(class_data) = self.class_data.get_mut(class_sid) {
                    class_data.count_delta -= 1;
                }
            }
        }
    }

    /// Process a flake to update class-property stats
    ///
    /// Must be called AFTER `collect_type_flake` has processed all novelty flakes.
    /// Uses the subject→class mapping to attribute property usage to classes.
    ///
    /// Skips rdf:type flakes (already processed in collect_type_flake).
    pub fn process_flake(&mut self, flake: &Flake) {
        // Skip rdf:type flakes (handled separately)
        if is_rdf_type(&flake.p) {
            return;
        }

        // Get classes for this subject
        let classes = match self.subject_classes.get(&flake.s) {
            Some(c) if !c.is_empty() => c.clone(),
            _ => return, // No classes for this subject - skip
        };

        let delta = if flake.op { 1i64 } else { -1i64 };

        // For each class the subject belongs to, track property usage
        for class_sid in classes {
            let class_data = self.class_data.entry(class_sid).or_default();

            let prop_data = class_data.properties.entry(flake.p.clone()).or_default();

            // Track presence for this (class, property) pair.
            //
            // NOTE: If a property is fully retracted from a class, we do not have enough
            // baseline information to reliably remove it without an expensive index walk,
            // so this list is treated as a conservative superset.
            prop_data.count_delta += delta;
        }
    }

    /// Finalize and return class statistics
    ///
    /// Returns None if no class data was collected.
    pub fn finalize(self) -> Option<Vec<ClassStatEntry>> {
        if self.class_data.is_empty() {
            return None;
        }

        // Convert to sorted output (determinism)
        let mut entries: Vec<ClassStatEntry> = self
            .class_data
            .into_iter()
            .filter(|(_, data)| data.count_delta > 0 || !data.properties.is_empty())
            .map(|(class_sid, data)| {
                // Convert properties (sorted by property SID)
                let mut properties: Vec<ClassPropertyUsage> = data
                    .properties
                    .into_iter()
                    .filter(|(_, prop_data)| prop_data.count_delta > 0)
                    .map(|(property_sid, _prop_data)| ClassPropertyUsage {
                        property_sid,
                        datatypes: Vec::new(),
                        langs: Vec::new(),
                        ref_classes: Vec::new(),
                    })
                    .collect();

                // Sort properties by SID for determinism
                properties.sort_by(|a, b| a.property_sid.cmp(&b.property_sid));

                ClassStatEntry {
                    class_sid,
                    count: data.count_delta.max(0) as u64,
                    properties,
                }
            })
            .collect();

        // Sort by class SID for determinism
        entries.sort_by(|a, b| a.class_sid.cmp(&b.class_sid));

        if entries.is_empty() {
            None
        } else {
            Some(entries)
        }
    }

    /// Check if any class data has been collected
    pub fn is_empty(&self) -> bool {
        self.class_data.is_empty() && self.subject_classes.is_empty()
    }

    /// Merge subject→class mapping from external lookup
    ///
    /// Used to incorporate class mappings retrieved from the index
    /// (subjects whose rdf:type was asserted in prior transactions).
    pub fn merge_subject_classes(&mut self, index_classes: HashMap<Sid, HashSet<Sid>>) {
        for (subject, classes) in index_classes {
            // Only add if not already in novelty (novelty takes precedence)
            self.subject_classes.entry(subject).or_insert(classes);
        }
    }
}

/// Result of class-property stats computation
#[derive(Debug, Default)]
pub struct ClassPropertyStatsResult {
    /// Class statistics (sorted for determinism)
    pub classes: Option<Vec<ClassStatEntry>>,
}

/// Batch lookup subject classes from PSOT index
///
/// Queries the PSOT index for rdf:type predicate to find class memberships
/// for the given subjects. This is used when subjects have their rdf:type
/// asserted in prior transactions (not in current novelty).
///
/// Returns {subject_sid -> HashSet<class_sid>} mapping.
pub async fn batch_lookup_subject_classes(
    snapshot: &LedgerSnapshot,
    g_id: GraphId,
    subjects: &HashSet<Sid>,
) -> crate::error::Result<HashMap<Sid, HashSet<Sid>>> {
    if subjects.is_empty() {
        return Ok(HashMap::new());
    }

    // Query PSOT for rdf:type predicate.
    //
    // Prefer an index-native batched predicate+subject lookup when available to avoid
    // scanning the full rdf:type predicate partition.
    let rdf_type_sid = Sid::new(RDF, RDF_TYPE);

    if let Some(provider) = snapshot.range_provider.as_ref() {
        let subj_vec: Vec<Sid> = subjects.iter().cloned().collect();
        let opts = RangeOptions::new().with_to_t(snapshot.t);
        let no_overlay = fluree_db_core::overlay::NoOverlay;
        match provider.lookup_subject_predicate_refs_batched(
            g_id,
            IndexType::Psot,
            &rdf_type_sid,
            &subj_vec,
            &opts,
            &no_overlay,
        ) {
            Ok(map) => {
                let mut out: HashMap<Sid, HashSet<Sid>> = HashMap::with_capacity(map.len());
                for (s, classes) in map {
                    out.insert(s, classes.into_iter().collect());
                }
                return Ok(out);
            }
            Err(e) if e.kind() == std::io::ErrorKind::Unsupported => {
                // Fall through to full predicate scan.
            }
            Err(e) => {
                return Err(crate::error::IndexerError::InvalidConfig(format!(
                    "batched class lookup failed: {e}"
                )));
            }
        }
    }

    // Fallback: full predicate scan (correct but potentially expensive).
    let match_val = RangeMatch::predicate(rdf_type_sid);
    let opts = RangeOptions::default();
    let flakes = range(
        snapshot,
        g_id,
        IndexType::Psot,
        RangeTest::Eq,
        match_val,
        opts,
    )
    .await
    .map_err(|e| {
        crate::error::IndexerError::InvalidConfig(format!("PSOT range query failed: {e}"))
    })?;

    let mut result: HashMap<Sid, HashSet<Sid>> = HashMap::new();
    for flake in flakes {
        if subjects.contains(&flake.s) && flake.op {
            if let FlakeValue::Ref(class_sid) = &flake.o {
                result
                    .entry(flake.s.clone())
                    .or_default()
                    .insert(class_sid.clone());
            }
        }
    }

    Ok(result)
}

/// Compute class-property statistics in parallel with index refresh
///
/// This function:
/// 1. Collects unique subjects from novelty
/// 2. Identifies subjects needing index lookup (no rdf:type in novelty)
/// 3. Queries PSOT index for missing class memberships
/// 4. Processes all novelty flakes to build class-property stats
///
/// Returns class statistics ready for inclusion in db-root.
pub async fn compute_class_property_stats_parallel(
    snapshot: &LedgerSnapshot,
    g_id: GraphId,
    prior_stats: Option<&fluree_db_core::IndexStats>,
    novelty_flakes: &[Flake],
) -> crate::error::Result<ClassPropertyStatsResult> {
    if novelty_flakes.is_empty() {
        // No novelty - preserve prior classes
        return Ok(ClassPropertyStatsResult {
            classes: prior_stats.and_then(|s| s.classes.clone()),
        });
    }

    // Phase 1: Initialize extractor with prior stats and collect novelty type flakes.
    //
    // IMPORTANT:
    // We must NOT treat "rdf:type present in novelty" as replacing prior class membership.
    // Instead, we:
    // - fetch the base classes for the subject from the persisted PSOT index (assertions only)
    // - apply novelty rdf:type asserts/retracts as a delta on top of that base set
    //
    // This matches legacy `batched-get-subject-classes` which applies `apply-type-novelty`
    // to the base class set returned from PSOT.
    let mut extractor = ClassPropertyExtractor::from_prior(prior_stats);

    // Collect rdf:type flakes first (builds subject→class mapping from novelty)
    for flake in novelty_flakes {
        extractor.collect_type_flake(flake);
    }

    // Phase 2: Collect subjects we may need class membership for (subjects present in novelty).
    let novelty_subjects: HashSet<Sid> = novelty_flakes.iter().map(|f| f.s.clone()).collect();

    let all_subjects_to_lookup: HashSet<Sid> = novelty_subjects;

    // Phase 3: Build novelty rdf:type deltas by subject.
    // We apply these to the base class set from PSOT (assertions only).
    let mut type_novelty_by_subject: HashMap<Sid, Vec<(Sid, bool)>> = HashMap::new();
    for flake in novelty_flakes {
        if is_rdf_type(&flake.p) {
            if let FlakeValue::Ref(class_sid) = &flake.o {
                type_novelty_by_subject
                    .entry(flake.s.clone())
                    .or_default()
                    .push((class_sid.clone(), flake.op));
            }
        }
    }

    // Phase 4: Batch lookup base classes from PSOT index (async), then apply novelty deltas.
    //
    // NOTE: Base lookup uses only assertions from the persisted index.
    // It does NOT collapse historical retractions inside the index itself.
    let mut subject_classes: HashMap<Sid, HashSet<Sid>> = if all_subjects_to_lookup.is_empty() {
        HashMap::new()
    } else {
        batch_lookup_subject_classes(snapshot, g_id, &all_subjects_to_lookup).await?
    };

    // Apply rdf:type novelty deltas to base membership (assert adds, retract removes).
    for (subject, deltas) in type_novelty_by_subject {
        let classes = subject_classes.entry(subject).or_default();
        for (class_sid, op) in deltas {
            if op {
                classes.insert(class_sid);
            } else {
                classes.remove(&class_sid);
            }
        }
    }

    // Install the final subject→classes map into the extractor (used by process_flake).
    extractor.subject_classes = subject_classes;

    // Phase 5: Process all novelty flakes with complete subject→class mapping
    for flake in novelty_flakes {
        extractor.process_flake(flake);
    }

    // Finalize and return
    let classes = extractor.finalize();

    Ok(ClassPropertyStatsResult { classes })
}

// class_property_stats_tests removed: depended on deleted builder module.
// Needs rewrite for binary pipeline.
