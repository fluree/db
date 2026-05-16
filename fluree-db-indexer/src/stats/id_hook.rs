//! ID-based per-(graph, property) HLL tracking for index statistics.
//!
//! [`IdStatsHook`] maintains per-(graph, property) HLL sketches and datatype
//! usage. All keys are numeric IDs — no `Sid` anywhere. Used by the import
//! and incremental indexing pipelines where GlobalDicts are available.

use std::collections::{HashMap, HashSet};

use crate::hll::HllSketch256;
use fluree_db_core::value_id::ValueTypeTag;
use fluree_db_core::{GraphId, GraphPropertyStatEntry, GraphStatsEntry};

use super::hashing::subject_hash;

/// Key for graph-scoped property stats (numeric IDs only)
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct GraphPropertyKey {
    pub g_id: GraphId,
    pub p_id: u32,
}

/// Per-(graph, property) HLL state with datatype tracking.
///
/// Uses signed deltas internally; clamped to 0 at finalize.
#[derive(Debug)]
pub struct IdPropertyHll {
    /// Flake count delta (signed: retractions decrement)
    pub count: i64,
    /// HLL sketch for distinct object values
    pub values_hll: HllSketch256,
    /// HLL sketch for distinct subjects
    pub subjects_hll: HllSketch256,
    /// Most recent transaction time
    pub last_modified_t: i64,
    /// Per-datatype flake count deltas: ValueTypeTag(u8) -> signed count
    pub datatypes: HashMap<u8, i64>,
}

impl IdPropertyHll {
    pub(crate) fn new() -> Self {
        Self {
            count: 0,
            values_hll: HllSketch256::new(),
            subjects_hll: HllSketch256::new(),
            last_modified_t: 0,
            datatypes: HashMap::new(),
        }
    }

    /// Create from loaded sketches (for incremental refresh)
    pub fn from_sketches(
        count: i64,
        values_hll: HllSketch256,
        subjects_hll: HllSketch256,
        last_modified_t: i64,
        datatypes: HashMap<u8, i64>,
    ) -> Self {
        Self {
            count,
            values_hll,
            subjects_hll,
            last_modified_t,
            datatypes,
        }
    }

    /// Merge another IdPropertyHll into this one.
    /// HLL: register-wise max. Counts: additive. last_modified_t: max.
    pub fn merge_from(&mut self, other: &IdPropertyHll) {
        self.count += other.count;
        self.values_hll.merge_inplace(&other.values_hll);
        self.subjects_hll.merge_inplace(&other.subjects_hll);
        self.last_modified_t = self.last_modified_t.max(other.last_modified_t);
        for (&dt, &delta) in &other.datatypes {
            *self.datatypes.entry(dt).or_insert(0) += delta;
        }
    }
}

/// A single resolved record for stats collection.
///
/// Bundles the per-op fields needed by `IdStatsHook::on_record`.
#[derive(Debug, Clone, Copy)]
pub struct StatsRecord {
    /// Graph dictionary ID (0 = default)
    pub g_id: GraphId,
    /// Predicate dictionary ID
    pub p_id: u32,
    /// Subject dictionary ID
    pub s_id: u64,
    /// Datatype ID
    pub dt: ValueTypeTag,
    /// Pre-computed object value hash (from `value_hash()`)
    pub o_hash: u64,
    /// Object kind discriminant (for class tracking)
    pub o_kind: u8,
    /// Object key payload (for class tracking; sid64 when o_kind == REF_ID)
    pub o_key: u64,
    /// Transaction time
    pub t: i64,
    /// true = assertion, false = retraction
    pub op: bool,
    /// Language tag dictionary ID (0 = no language tag, >= 1 = lang_id).
    pub lang_id: u16,
}

/// Construct a `StatsRecord` from a V2 `RunRecordV2` + op byte.
///
/// Maps `OType` to the legacy fields needed by `IdStatsHook::on_record`:
/// - `o_kind`: 0x05 (REF_ID) for `OType::IRI_REF`, 0 otherwise (only REF detection matters)
/// - `dt`: derived from `OType` category (approximate but consistent)
/// - `o_hash`: uses `value_hash_v2(o_type, o_key)` (V2-compatible domain separation)
/// - `lang_id`: extracted from `OType` if langString, 0 otherwise
pub fn stats_record_from_v2(
    rec: &fluree_db_binary_index::format::run_record_v2::RunRecordV2,
    op: u8,
) -> StatsRecord {
    use fluree_db_core::o_type::OType;

    let ot = OType::from_u16(rec.o_type);

    // Map OType to legacy o_kind (only REF_ID detection matters for class tracking).
    let o_kind = if ot == OType::IRI_REF {
        0x05 // ObjKind::REF_ID
    } else {
        0 // doesn't matter for stats — only REF_ID is special-cased
    };

    // Map OType to approximate ValueTypeTag for datatype counting.
    let dt = otype_to_value_type_tag(ot);

    // Extract lang_id from OType (0 if not langString).
    let lang_id = if ot.is_lang_string() {
        ot.as_u16() & 0x3FFF // payload = lang_id
    } else {
        0
    };

    StatsRecord {
        g_id: rec.g_id,
        p_id: rec.p_id,
        s_id: rec.s_id.as_u64(),
        dt,
        o_hash: super::hashing::value_hash_v2(rec.o_type, rec.o_key),
        o_kind,
        o_key: rec.o_key,
        t: rec.t as i64,
        op: op != 0,
        lang_id,
    }
}

/// Map an `OType` to a `ValueTypeTag` for stats datatype counting.
///
/// The mapping doesn't need to be perfectly granular — it just needs to be
/// consistent so HLL datatype buckets are stable across rebuild/incremental.
fn otype_to_value_type_tag(ot: fluree_db_core::o_type::OType) -> ValueTypeTag {
    use fluree_db_core::o_type::OType;

    if ot.is_lang_string() {
        return ValueTypeTag::LANG_STRING;
    }

    match ot {
        OType::XSD_BOOLEAN => ValueTypeTag::BOOLEAN,
        OType::XSD_INTEGER => ValueTypeTag::INTEGER,
        OType::XSD_LONG => ValueTypeTag::LONG,
        OType::XSD_INT => ValueTypeTag::INT,
        OType::XSD_SHORT => ValueTypeTag::SHORT,
        OType::XSD_BYTE => ValueTypeTag::BYTE,
        OType::XSD_UNSIGNED_LONG => ValueTypeTag::UNSIGNED_LONG,
        OType::XSD_UNSIGNED_INT => ValueTypeTag::UNSIGNED_INT,
        OType::XSD_UNSIGNED_SHORT => ValueTypeTag::UNSIGNED_SHORT,
        OType::XSD_UNSIGNED_BYTE => ValueTypeTag::UNSIGNED_BYTE,
        OType::XSD_NON_NEGATIVE_INTEGER => ValueTypeTag::NON_NEGATIVE_INTEGER,
        OType::XSD_POSITIVE_INTEGER => ValueTypeTag::POSITIVE_INTEGER,
        OType::XSD_NON_POSITIVE_INTEGER => ValueTypeTag::NON_POSITIVE_INTEGER,
        OType::XSD_NEGATIVE_INTEGER => ValueTypeTag::NEGATIVE_INTEGER,
        OType::XSD_DOUBLE => ValueTypeTag::DOUBLE,
        OType::XSD_FLOAT => ValueTypeTag::FLOAT,
        OType::XSD_DECIMAL => ValueTypeTag::DECIMAL,
        // NUM_BIG_OVERFLOW is intentionally NOT mapped here: it carries both
        // `FlakeValue::Decimal` (arbitrary-precision xsd:decimal) and
        // `FlakeValue::BigInt` (xsd:integer overflow > i64) — they share
        // `ObjKind::NUM_BIG`. Disambiguating requires inspecting the NumBig
        // arena entry (`StoredBigValue::BigDec` vs `BigInt`), which is not
        // available here. Falling through to UNKNOWN is the honest answer
        // until the collector is plumbed with arena access or the semantic
        // dt tag is preserved alongside the o_type byte.
        OType::XSD_DATE => ValueTypeTag::DATE,
        OType::XSD_TIME => ValueTypeTag::TIME,
        OType::XSD_DATE_TIME => ValueTypeTag::DATE_TIME,
        OType::XSD_G_YEAR => ValueTypeTag::G_YEAR,
        OType::XSD_G_YEAR_MONTH => ValueTypeTag::G_YEAR_MONTH,
        OType::XSD_G_MONTH => ValueTypeTag::G_MONTH,
        OType::XSD_G_DAY => ValueTypeTag::G_DAY,
        OType::XSD_G_MONTH_DAY => ValueTypeTag::G_MONTH_DAY,
        OType::XSD_STRING => ValueTypeTag::STRING,
        OType::XSD_ANY_URI => ValueTypeTag::ANY_URI,
        OType::XSD_NORMALIZED_STRING => ValueTypeTag::NORMALIZED_STRING,
        OType::XSD_TOKEN => ValueTypeTag::TOKEN,
        OType::XSD_LANGUAGE => ValueTypeTag::LANGUAGE,
        OType::XSD_BASE64_BINARY => ValueTypeTag::BASE64_BINARY,
        OType::XSD_HEX_BINARY => ValueTypeTag::HEX_BINARY,
        OType::IRI_REF => ValueTypeTag::JSON_LD_ID,
        OType::RDF_JSON => ValueTypeTag::RDF_JSON,
        OType::XSD_DURATION => ValueTypeTag::DURATION,
        OType::XSD_DAY_TIME_DURATION => ValueTypeTag::DAY_TIME_DURATION,
        OType::XSD_YEAR_MONTH_DURATION => ValueTypeTag::YEAR_MONTH_DURATION,
        OType::VECTOR => ValueTypeTag::VECTOR,
        OType::FULLTEXT => ValueTypeTag::FULL_TEXT,
        _ => ValueTypeTag::UNKNOWN,
    }
}

/// Result from `IdStatsHook::finalize()`.
pub struct IdStatsResult {
    /// Per-graph stats entries (authoritative, ID-keyed).
    pub graphs: Vec<GraphStatsEntry>,
    /// Total flake count across all graphs.
    pub total_flakes: u64,
}

/// ID-based stats hook for import/index paths where GlobalDicts are available.
///
/// Maintains per-(graph, property) HLL sketches and datatype usage.
/// All keys are numeric IDs — no Sid anywhere.
///
/// # Usage
///
/// ```ignore
/// let mut hook = IdStatsHook::new();
/// // Per resolved op:
/// hook.on_record(&StatsRecord { g_id, p_id, s_id, dt, o_hash, o_kind, o_key, t, op });
/// // After all ops:
/// let result = hook.finalize();
/// ```
#[derive(Debug, Default)]
pub struct IdStatsHook {
    flake_count: usize,
    properties: HashMap<GraphPropertyKey, IdPropertyHll>,
    /// Per-graph flake count (signed delta)
    graph_flakes: HashMap<GraphId, i64>,
    /// p_id for rdf:type (when set, enables class tracking)
    rdf_type_p_id: Option<u32>,
    /// Whether to track reference target-class edges (class→property→ref-class).
    ///
    /// This can be very memory-intensive for large datasets because it requires
    /// retaining per-subject reference histories until finalize time.
    ///
    /// When disabled, class counts + class→property presence still work, but
    /// `finalize_with_aggregate_properties()` will return an empty `class_ref_targets`.
    track_ref_targets: bool,
    /// When true, skip all per-subject map accumulation (subject_class_deltas,
    /// subject_props, subject_prop_dts, subject_prop_langs, subject_ref_history).
    ///
    /// HLL sketches, per-property stats, and class_counts are still tracked.
    /// Use this for the full rebuild path where class stats are computed via
    /// disk-backed streaming over sorted commit files instead.
    hll_only: bool,
    /// Class membership counts: (g_id, class_sid64) → signed delta count.
    /// Graph-scoped so per-graph ClassStatEntry can be derived.
    class_counts: HashMap<(GraphId, u64), i64>,
    /// (g_id, subject) → class_sid64 → signed delta count (net rdf:type membership).
    ///
    /// This is used at finalize-time to derive the subject's current class set,
    /// which is then used to compute:
    /// - class→property presence
    /// - class→property→target-class ref-edge counts
    ///
    /// Keeping deltas (rather than a set) makes this merge-safe across commits.
    subject_class_deltas: HashMap<(GraphId, u64), HashMap<u64, i64>>,
    /// Per-subject property tracking (for retroactive class attribution).
    /// Graph-scoped: (g_id, subject_sid64) → set of p_ids.
    subject_props: HashMap<(GraphId, u64), HashSet<u32>>,
    /// Per-subject ref history: (g_id, subject sid64) → property → object sid64 → signed delta count.
    ///
    /// At finalize-time, this is combined with derived subject/object class sets
    /// to produce class→property→target-class ref-edge counts.
    subject_ref_history: HashMap<(GraphId, u64), HashMap<u32, HashMap<u64, i64>>>,
    /// Per-subject, per-property datatype tracking: (g_id, subject sid64) → p_id → dt_tag → signed delta.
    ///
    /// Used at finalize-time to derive per-class datatype distributions by
    /// cross-referencing with subject_classes.
    subject_prop_dts: HashMap<(GraphId, u64), HashMap<u32, HashMap<u8, i64>>>,
    /// Per-subject, per-property language tag tracking: (g_id, subject sid64) → p_id → lang_id → signed delta.
    ///
    /// Used at finalize-time to derive per-class language distributions by
    /// cross-referencing with subject_classes.
    subject_prop_langs: HashMap<(GraphId, u64), HashMap<u32, HashMap<u16, i64>>>,
}

impl IdStatsHook {
    pub fn new() -> Self {
        // Default behavior preserves existing incremental stats richness.
        // Import paths may disable ref-target tracking explicitly.
        Self {
            track_ref_targets: true,
            ..Self::default()
        }
    }

    /// Create an HLL-only hook that skips all per-subject map accumulation.
    ///
    /// HLL sketches, per-property stats, and class_counts are still tracked.
    /// Per-subject maps (`subject_class_deltas`, `subject_props`, `subject_prop_dts`,
    /// `subject_prop_langs`, `subject_ref_history`) remain empty.
    ///
    /// Use for the full rebuild path where class stats are computed via
    /// disk-backed streaming over sorted commit files.
    pub fn new_hll_only() -> Self {
        Self {
            hll_only: true,
            track_ref_targets: false,
            ..Self::default()
        }
    }

    /// Create a hook seeded with prior per-property HLL sketches.
    ///
    /// Enables incremental refresh: load prior sketches from a CAS blob,
    /// then process only novelty commits. The hook's `on_record()` will
    /// merge new observations into the existing registers.
    pub fn with_prior_properties(properties: HashMap<GraphPropertyKey, IdPropertyHll>) -> Self {
        Self {
            properties,
            track_ref_targets: true,
            ..Self::default()
        }
    }

    /// Set the predicate ID for rdf:type to enable class tracking.
    pub fn set_rdf_type_p_id(&mut self, p_id: u32) {
        self.rdf_type_p_id = Some(p_id);
    }

    pub fn rdf_type_p_id(&self) -> Option<u32> {
        self.rdf_type_p_id
    }

    /// Enable/disable tracking of reference target-class edges.
    pub fn set_track_ref_targets(&mut self, enabled: bool) {
        self.track_ref_targets = enabled;
    }

    /// Process a single record with resolved IDs.
    ///
    /// Called per-op after the resolver maps Sids to numeric IDs.
    pub fn on_record(&mut self, rec: &StatsRecord) {
        self.flake_count += 1;
        let delta: i64 = if rec.op { 1 } else { -1 };

        // Track per-graph flake count
        *self.graph_flakes.entry(rec.g_id).or_insert(0) += delta;

        let key = GraphPropertyKey {
            g_id: rec.g_id,
            p_id: rec.p_id,
        };
        let hll = self
            .properties
            .entry(key)
            .or_insert_with(IdPropertyHll::new);

        hll.count += delta;

        // HLL: only insert on assertions (NDV is monotone)
        if rec.op {
            hll.values_hll.insert_hash(rec.o_hash);
            hll.subjects_hll.insert_hash(subject_hash(rec.s_id));
        }

        if rec.t > hll.last_modified_t {
            hll.last_modified_t = rec.t;
        }

        // Track datatype usage
        *hll.datatypes.entry(rec.dt.as_u8()).or_insert(0) += delta;

        // Track class membership and class→property attribution (graph-scoped).
        if let Some(rdf_type_pid) = self.rdf_type_p_id {
            if rec.p_id == rdf_type_pid && rec.o_kind == 0x05 {
                // ObjKind::REF_ID == 0x05: this is an rdf:type assertion/retraction.
                // class_counts is always tracked (tiny: bounded by distinct classes).
                *self.class_counts.entry((rec.g_id, rec.o_key)).or_insert(0) += delta;

                if !self.hll_only {
                    *self
                        .subject_class_deltas
                        .entry((rec.g_id, rec.s_id))
                        .or_default()
                        .entry(rec.o_key)
                        .or_insert(0) += delta;
                }
            } else if !self.hll_only && rec.op {
                // Non-rdf:type property assertion: track per-subject and per-class
                self.subject_props
                    .entry((rec.g_id, rec.s_id))
                    .or_default()
                    .insert(rec.p_id);
            }

            if !self.hll_only && rec.p_id != rdf_type_pid {
                // Track per-subject datatype usage for class→property→datatype attribution.
                *self
                    .subject_prop_dts
                    .entry((rec.g_id, rec.s_id))
                    .or_default()
                    .entry(rec.p_id)
                    .or_default()
                    .entry(rec.dt.as_u8())
                    .or_insert(0) += delta;

                // Track per-subject language tag usage.
                if rec.lang_id != 0 && rec.dt == ValueTypeTag::LANG_STRING {
                    *self
                        .subject_prop_langs
                        .entry((rec.g_id, rec.s_id))
                        .or_default()
                        .entry(rec.p_id)
                        .or_default()
                        .entry(rec.lang_id)
                        .or_insert(0) += delta;
                }
            }

            // Track reference-valued properties for class→property ref target stats.
            //
            // We track both assertions and retractions via signed deltas.
            // Only applies to ref objects (ObjKind::REF_ID).
            if self.track_ref_targets
                && !self.hll_only
                && rec.p_id != rdf_type_pid
                && rec.o_kind == 0x05
            {
                // Record per-subject ref history (for retroactive attribution on rdf:type)
                *self
                    .subject_ref_history
                    .entry((rec.g_id, rec.s_id))
                    .or_default()
                    .entry(rec.p_id)
                    .or_default()
                    .entry(rec.o_key)
                    .or_insert(0) += delta;
            }
        }
    }

    /// Merge another hook into this one (for cross-commit accumulation).
    ///
    /// HLL: register-wise max. Counts: additive.
    pub fn merge_from(&mut self, other: IdStatsHook) {
        self.flake_count += other.flake_count;

        for (g_id, delta) in other.graph_flakes {
            *self.graph_flakes.entry(g_id).or_insert(0) += delta;
        }

        for (key, other_hll) in other.properties {
            self.properties
                .entry(key)
                .or_insert_with(IdPropertyHll::new)
                .merge_from(&other_hll);
        }

        // Merge class counts and attribution inputs
        if self.rdf_type_p_id.is_none() {
            self.rdf_type_p_id = other.rdf_type_p_id;
        }
        if !self.track_ref_targets {
            self.track_ref_targets = other.track_ref_targets;
        }
        for (key, delta) in other.class_counts {
            *self.class_counts.entry(key).or_insert(0) += delta;
        }
        if !self.hll_only {
            for (key, class_map) in other.subject_class_deltas {
                let entry = self.subject_class_deltas.entry(key).or_default();
                for (class_sid64, delta) in class_map {
                    *entry.entry(class_sid64).or_insert(0) += delta;
                }
            }
            for (key, props) in other.subject_props {
                self.subject_props.entry(key).or_default().extend(props);
            }
        }
        // Merge per-subject ref history.
        if self.track_ref_targets && !self.hll_only {
            for (key, per_prop) in other.subject_ref_history {
                let entry = self.subject_ref_history.entry(key).or_default();
                for (p_id, objs) in per_prop {
                    let o_entry = entry.entry(p_id).or_default();
                    for (obj, d) in objs {
                        *o_entry.entry(obj).or_insert(0) += d;
                    }
                }
            }
        }
    }

    /// Borrow the internal properties map (for sketch persistence before finalize).
    pub fn properties(&self) -> &HashMap<GraphPropertyKey, IdPropertyHll> {
        &self.properties
    }

    /// Total flake count (all graphs, all ops).
    pub fn flake_count(&self) -> usize {
        self.flake_count
    }

    /// Mutable access to per-graph flake totals.
    ///
    /// Used by incremental indexing to seed base-root flake counts before
    /// feeding novelty records, so `finalize()` produces correct totals
    /// (base + delta) rather than delta-only.
    pub fn graph_flakes_mut(&mut self) -> &mut HashMap<GraphId, i64> {
        &mut self.graph_flakes
    }

    /// Read-only access to class membership count deltas.
    ///
    /// Used by incremental indexing to extract novelty-only class count deltas
    /// (before finalize consumes the hook). Keyed by `(g_id, class_sid64)`,
    /// values are signed deltas: +1 per rdf:type assertion, -1 per retraction.
    pub fn class_count_deltas(&self) -> &HashMap<(GraphId, u64), i64> {
        &self.class_counts
    }

    /// Read-only access to per-subject rdf:type deltas.
    ///
    /// Keyed by `(g_id, subject_sid64)`, values are `class_sid64 -> signed delta`.
    /// Used by incremental indexing to merge novelty rdf:type deltas with
    /// base class memberships from the PSOT index.
    pub fn subject_class_deltas(&self) -> &HashMap<(GraphId, u64), HashMap<u64, i64>> {
        &self.subject_class_deltas
    }

    /// Read-only access to per-subject property sets.
    ///
    /// Keyed by `(g_id, subject_sid64)`, values are the set of predicate IDs
    /// that subject has in novelty. Used by incremental indexing for
    /// class-property attribution.
    pub fn subject_props(&self) -> &HashMap<(GraphId, u64), HashSet<u32>> {
        &self.subject_props
    }

    /// Read-only access to per-subject ref history.
    ///
    /// Keyed by `(g_id, subject_sid64)`, values are
    /// `property_p_id -> object_sid64 -> signed delta`.
    /// Used by incremental indexing for computing ref-class edges.
    #[allow(clippy::type_complexity)]
    pub fn subject_ref_history(&self) -> &HashMap<(GraphId, u64), HashMap<u32, HashMap<u64, i64>>> {
        &self.subject_ref_history
    }

    /// Read-only access to per-subject, per-property datatype deltas.
    ///
    /// Keyed by `(g_id, subject_sid64)`, values are `p_id -> dt_tag(u8) -> signed delta`.
    /// Used by incremental indexing for class→property→datatype attribution.
    #[allow(clippy::type_complexity)]
    pub fn subject_prop_dts(&self) -> &HashMap<(GraphId, u64), HashMap<u32, HashMap<u8, i64>>> {
        &self.subject_prop_dts
    }

    /// Read-only access to per-subject, per-property language tag deltas.
    ///
    /// Keyed by `(g_id, subject_sid64)`, values are `p_id -> lang_id(u16) -> signed delta`.
    /// Used by incremental indexing for class→property→lang attribution.
    #[allow(clippy::type_complexity)]
    pub fn subject_prop_langs(&self) -> &HashMap<(GraphId, u64), HashMap<u32, HashMap<u16, i64>>> {
        &self.subject_prop_langs
    }

    /// Produce per-graph stats and aggregate property stats.
    ///
    /// Includes all graphs (default, txn-meta, config, user named graphs).
    /// Clamps all signed deltas to 0.
    pub fn finalize(self) -> IdStatsResult {
        // Group by g_id, then by p_id
        let mut graph_map: HashMap<GraphId, Vec<(&GraphPropertyKey, &IdPropertyHll)>> =
            HashMap::new();
        for (key, hll) in &self.properties {
            graph_map.entry(key.g_id).or_default().push((key, hll));
        }

        // Build per-graph entries
        let mut graphs: Vec<GraphStatsEntry> = Vec::new();

        for (&g_id, entries) in &graph_map {
            let mut props: Vec<GraphPropertyStatEntry> = Vec::new();
            for (key, hll) in entries {
                // Clamp count to 0
                let count = hll.count.max(0) as u64;
                let datatypes: Vec<(u8, u64)> = hll
                    .datatypes
                    .iter()
                    .filter(|(_, &v)| v > 0)
                    .map(|(&dt, &v)| (dt, v.max(0) as u64))
                    .collect();

                props.push(GraphPropertyStatEntry {
                    p_id: key.p_id,
                    count,
                    ndv_values: hll.values_hll.estimate(),
                    ndv_subjects: hll.subjects_hll.estimate(),
                    last_modified_t: hll.last_modified_t,
                    datatypes,
                });
            }

            // Sort properties by p_id for determinism
            props.sort_by_key(|p| p.p_id);

            let graph_flake_count =
                self.graph_flakes.get(&g_id).copied().unwrap_or(0).max(0) as u64;

            graphs.push(GraphStatsEntry {
                g_id,
                flakes: graph_flake_count,
                size: 0, // Populated by index build, not available here
                properties: props,
                classes: None, // Populated by caller after finalize
            });
        }

        // Sort graphs by g_id for determinism
        graphs.sort_by_key(|g| g.g_id);

        let total_flakes: u64 = self
            .graph_flakes
            .iter()
            .map(|(_, &delta)| delta.max(0) as u64)
            .sum();

        IdStatsResult {
            graphs,
            total_flakes,
        }
    }

    /// Like [`finalize`], but also moves out the per-subject tracking maps
    /// instead of requiring the caller to `.clone()` them beforehand.
    ///
    /// This avoids temporarily doubling memory by cloning large maps before
    /// `finalize()` consumes the hook. The maps are moved out via
    /// `std::mem::take()`, then `finalize()` runs on the emptied struct.
    #[allow(clippy::type_complexity)]
    pub fn finalize_into_parts(
        mut self,
    ) -> (
        IdStatsResult,
        HashMap<(GraphId, u64), i64>,               // class_count_deltas
        HashMap<(GraphId, u64), HashMap<u64, i64>>, // subject_class_deltas
        HashMap<(GraphId, u64), HashSet<u32>>,      // subject_props
        HashMap<(GraphId, u64), HashMap<u32, HashMap<u8, i64>>>, // subject_prop_dts
        HashMap<(GraphId, u64), HashMap<u32, HashMap<u16, i64>>>, // subject_prop_langs
        HashMap<(GraphId, u64), HashMap<u32, HashMap<u64, i64>>>, // subject_ref_history
    ) {
        let class_count_deltas = std::mem::take(&mut self.class_counts);
        let subject_class_deltas = std::mem::take(&mut self.subject_class_deltas);
        let subject_props = std::mem::take(&mut self.subject_props);
        let subject_prop_dts = std::mem::take(&mut self.subject_prop_dts);
        let subject_prop_langs = std::mem::take(&mut self.subject_prop_langs);
        let subject_ref_history = std::mem::take(&mut self.subject_ref_history);
        let result = self.finalize();
        (
            result,
            class_count_deltas,
            subject_class_deltas,
            subject_props,
            subject_prop_dts,
            subject_prop_langs,
            subject_ref_history,
        )
    }

    /// Finalize into per-graph stats plus a ledger-wide aggregate property view
    /// and graph-scoped class membership counts.
    ///
    /// The aggregate view is keyed only by `p_id` (across all graphs), with HLL sketches
    /// merged across graphs so NDV estimates remain meaningful. Datatype counts are summed
    /// across graphs.
    ///
    /// Class outputs are graph-scoped:
    /// - `class_counts`: `(g_id, class_sid64, count)` triples
    /// - `class_properties`: `(g_id, class_sid64) -> HashSet<p_id>`
    /// - `class_ref_targets`: `(g_id, class_sid64) -> p_id -> target_class_sid64 -> delta`
    ///
    /// Includes all graphs (default, txn-meta, config, user named graphs).
    #[allow(clippy::type_complexity)]
    pub fn finalize_with_aggregate_properties(
        self,
    ) -> (
        IdStatsResult,
        Vec<GraphPropertyStatEntry>,
        Vec<(GraphId, u64, u64)>,
        HashMap<(GraphId, u64), HashSet<u32>>,
        HashMap<(GraphId, u64), HashMap<u32, HashMap<u64, i64>>>,
    ) {
        // Aggregate by p_id across all graphs
        let mut agg: HashMap<u32, IdPropertyHll> = HashMap::new();
        for (key, hll) in &self.properties {
            agg.entry(key.p_id)
                .or_insert_with(IdPropertyHll::new)
                .merge_from(hll);
        }

        let mut properties: Vec<GraphPropertyStatEntry> = agg
            .into_iter()
            .map(|(p_id, hll)| {
                let count = hll.count.max(0) as u64;
                let datatypes: Vec<(u8, u64)> = hll
                    .datatypes
                    .iter()
                    .filter(|(_, &v)| v > 0)
                    .map(|(&dt, &v)| (dt, v.max(0) as u64))
                    .collect();

                GraphPropertyStatEntry {
                    p_id,
                    count,
                    ndv_values: hll.values_hll.estimate(),
                    ndv_subjects: hll.subjects_hll.estimate(),
                    last_modified_t: hll.last_modified_t,
                    datatypes,
                }
            })
            .collect();

        // Deterministic ordering
        properties.sort_by_key(|p| p.p_id);

        // Extract class counts ((g_id, sid64) → count), clamped to 0
        let mut class_counts: Vec<(GraphId, u64, u64)> = self
            .class_counts
            .iter()
            .filter(|(_, &delta)| delta > 0)
            .map(|(&(g_id, sid64), &delta)| (g_id, sid64, delta as u64))
            .collect();
        class_counts.sort_by_key(|&(g_id, sid64, _)| (g_id, sid64));

        // Derive current (g_id, subject) → classes from rdf:type deltas (net membership).
        let mut subject_classes: HashMap<(GraphId, u64), Vec<u64>> = HashMap::new();
        for (&(g_id, subj_sid64), class_map) in &self.subject_class_deltas {
            let mut classes: Vec<u64> = class_map
                .iter()
                .filter_map(|(&class_sid64, &d)| (d > 0).then_some(class_sid64))
                .collect();
            if classes.is_empty() {
                continue;
            }
            classes.sort_unstable();
            subject_classes.insert((g_id, subj_sid64), classes);
        }

        // Compute class→property presence from subject_props + current classes (graph-scoped).
        let mut class_properties: HashMap<(GraphId, u64), HashSet<u32>> = HashMap::new();
        for (&(g_id, subj_sid64), props) in &self.subject_props {
            let Some(classes) = subject_classes.get(&(g_id, subj_sid64)) else {
                continue;
            };
            for &class_sid64 in classes {
                class_properties
                    .entry((g_id, class_sid64))
                    .or_default()
                    .extend(props.iter().copied());
            }
        }

        // Compute class→property→target-class ref-edge counts from subject_ref_history
        // and current (net) subject/object class sets (graph-scoped).
        let mut class_ref_targets: HashMap<(GraphId, u64), HashMap<u32, HashMap<u64, i64>>> =
            HashMap::new();
        for (&(g_id, subj_sid64), per_prop) in &self.subject_ref_history {
            let Some(subj_classes) = subject_classes.get(&(g_id, subj_sid64)) else {
                continue;
            };
            for (&p_id, objs) in per_prop {
                for (&obj_sid64, &edge_delta) in objs {
                    if edge_delta == 0 {
                        continue;
                    }
                    let Some(obj_classes) = subject_classes.get(&(g_id, obj_sid64)) else {
                        continue;
                    };
                    for &subj_class_sid64 in subj_classes {
                        for &obj_class_sid64 in obj_classes {
                            *class_ref_targets
                                .entry((g_id, subj_class_sid64))
                                .or_default()
                                .entry(p_id)
                                .or_default()
                                .entry(obj_class_sid64)
                                .or_insert(0) += edge_delta;
                        }
                    }
                }
            }
        }

        let graphs = self.finalize();
        (
            graphs,
            properties,
            class_counts,
            class_properties,
            class_ref_targets,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_id_stats_hook_with_prior_properties() {
        let mut prior = HashMap::new();
        let mut hll = IdPropertyHll::new();
        hll.values_hll.insert_hash(100);
        hll.subjects_hll.insert_hash(1000);
        hll.count = 5;
        hll.last_modified_t = 3;
        prior.insert(GraphPropertyKey { g_id: 0, p_id: 1 }, hll);

        let hook = IdStatsHook::with_prior_properties(prior);
        let props = hook.properties();
        assert_eq!(props.len(), 1);

        let key = GraphPropertyKey { g_id: 0, p_id: 1 };
        assert_eq!(props[&key].count, 5);
        assert_eq!(props[&key].last_modified_t, 3);
    }
}
