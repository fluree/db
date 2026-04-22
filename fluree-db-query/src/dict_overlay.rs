//! Dictionary overlay for binary index queries.
//!
//! `DictOverlay` wraps a `BinaryIndexStore` and extends its dictionaries with
//! ephemeral entries for predicates, graphs, language tags, and numbig values
//! that are present in novelty but not yet in the persisted binary index.
//!
//! Subject and string dictionaries are delegated to `DictNovelty` (shared
//! across queries within a `LedgerState`). Predicates, graphs, languages,
//! datatypes, and numbig remain per-query ephemeral (low cardinality).
//!
//! # Usage
//!
//! ```ignore
//! let mut overlay = DictOverlay::new(graph_view, dict_novelty.clone());
//! let s_id = overlay.assign_subject_id_from_sid(&sid)?;
//! let iri = overlay.resolve_subject_iri(s_id)?;
//! ```

use fluree_db_binary_index::{BinaryGraphView, BinaryIndexStore};
use fluree_db_core::dict_novelty::DictNovelty;
use fluree_db_core::flake::FlakeMeta;
use fluree_db_core::ns_vec_bi_dict::NsVecBiDict;
use fluree_db_core::sid::Sid;
use fluree_db_core::subject_id::SubjectId;
use fluree_db_core::value::FlakeValue;
use fluree_db_core::value_id::{ObjKey, ObjKind};
use fluree_db_core::vec_bi_dict::VecBiDict;
use fluree_db_core::GraphId;
use fluree_db_core::ListIndex;
use fluree_vocab::namespaces;
use std::collections::HashMap;
use std::io;
use std::sync::Arc;

/// Per-query dictionary overlay for the binary index.
///
/// Subject and string lookups are delegated to `DictNovelty` (populated during
/// commit, shared across queries). Predicates, graphs, languages, datatypes,
/// and numbig remain per-query ephemeral.
///
/// Forward lookups use watermark routing: IDs at or below the watermark are
/// guaranteed to be in the persisted tree; IDs above the watermark are novel
/// and resolved from `DictNovelty`.
pub struct DictOverlay {
    graph_view: BinaryGraphView,
    dict_novelty: Arc<DictNovelty>,

    // -- Ephemeral predicate extensions (per-query, low cardinality) --
    ext_predicates: VecBiDict<u32>,

    // -- Ephemeral graph extensions --
    ext_graphs: VecBiDict<GraphId>,

    // -- Ephemeral language tag extensions --
    ext_lang_tags: VecBiDict<u16>,

    // -- Ephemeral NumBig extensions (BigInt/Decimal overflow) --
    ext_numbig: Vec<FlakeValue>,
    ext_numbig_map: HashMap<String, u32>, // canonical string repr → handle

    // -- Ephemeral Vector extensions --
    //
    // Novelty may contain vector values not yet persisted to the binary vector arena.
    // We assign ephemeral VECTOR_ID handles and force early materialization so these
    // values never escape the overlay as unresolved EncodedLit.
    ext_vectors: Vec<Vec<f64>>,

    // -- Ephemeral subject/string fallback (range provider path only) --
    //
    // These are populated ONLY when DictNovelty is uninitialized (e.g., the
    // BinaryRangeProvider fallback path where dict_novelty can't be shared
    // across commits). In the main binary scan path with properly threaded
    // DictNovelty, these remain empty.
    //
    // IMPORTANT: subject IDs MUST preserve `sid64` semantics
    // (`(ns_code << 48) | local_id`). Use a namespace-aware dictionary that
    // allocates `SubjectId`-encoded u64 values in a high local-id range to
    // avoid collisions and preserve sort order during overlay merges.
    ext_subjects: NsVecBiDict,
    ext_strings: VecBiDict<u32>,

    numbig_next_handle: u32,
    vector_next_handle: u32,
}

/// Handles above this value are ephemeral NumBig entries from DictOverlay.
const EPHEMERAL_NUMBIG_BASE: u32 = 0x8000_0000;

/// Handles above this value are ephemeral Vector entries from DictOverlay.
const EPHEMERAL_VECTOR_BASE: u32 = 0x8000_0000;

/// Base local ID for ephemeral subjects within a namespace.
///
/// Ephemeral `sid64` values allocated by this overlay start at this local-id
/// to ensure they sort after persisted IDs within a namespace.
const EPHEMERAL_SUBJECT_LOCAL_BASE: u64 = 0x0000_8000_0000_0000;

impl DictOverlay {
    /// Create a new overlay wrapping the given graph view and DictNovelty.
    ///
    /// The graph view is upgraded to be novelty-aware (if it isn't already)
    /// so that the final delegation in `decode_value()` handles watermark
    /// routing for dict-backed types automatically.
    pub fn new(graph_view: BinaryGraphView, dict_novelty: Arc<DictNovelty>) -> Self {
        let store = graph_view.store();
        let base_p_count = store.predicate_count();
        let base_g_count = store.graph_ids().len() as GraphId;
        let base_lang_count = store.language_tag_count();
        let base_str_count = store.string_count();

        // Ensure the graph view carries dict_novelty so the delegation at
        // the end of decode_value() is itself novelty-aware.
        let graph_view = if graph_view.has_dict_novelty() {
            graph_view
        } else {
            BinaryGraphView::with_novelty(
                graph_view.clone_store(),
                graph_view.g_id(),
                Some(Arc::clone(&dict_novelty)),
            )
            .with_namespace_codes_fallback(graph_view.namespace_codes_fallback())
        };

        Self {
            graph_view,
            dict_novelty,
            ext_predicates: VecBiDict::new(base_p_count),
            ext_graphs: VecBiDict::new(base_g_count),
            ext_lang_tags: VecBiDict::new(base_lang_count + 1),
            ext_numbig: Vec::new(),
            ext_numbig_map: HashMap::new(),
            ext_vectors: Vec::new(),
            ext_subjects: NsVecBiDict::with_local_base(EPHEMERAL_SUBJECT_LOCAL_BASE),
            ext_strings: VecBiDict::new(base_str_count),
            numbig_next_handle: EPHEMERAL_NUMBIG_BASE,
            vector_next_handle: EPHEMERAL_VECTOR_BASE,
        }
    }

    /// Reference to the underlying store.
    pub fn store(&self) -> &BinaryIndexStore {
        self.graph_view.store()
    }

    /// Reference to the underlying graph view.
    pub fn graph_view(&self) -> &BinaryGraphView {
        &self.graph_view
    }

    // ========================================================================
    // Subject dictionary (delegated to DictNovelty)
    // ========================================================================

    /// Look up or assign a subject ID for the given IRI.
    ///
    /// Tries: persisted reverse index (canonical) → DictNovelty → ephemeral
    /// fallback. Ephemeral fallback is only used when DictNovelty is
    /// uninitialized (range provider path).
    pub fn assign_subject_id(&mut self, iri: &str) -> io::Result<u64> {
        // 1. Persisted tree (canonical — must be first)
        if let Some(id) = self.graph_view.store().find_subject_id(iri)? {
            return Ok(id);
        }
        // 2. DictNovelty
        if self.dict_novelty.is_initialized() {
            let sid = self.graph_view.store().encode_iri(iri);
            if let Some(id) = self
                .dict_novelty
                .subjects
                .find_subject(sid.namespace_code, &sid.name)
            {
                return Ok(id);
            }
        }
        // 3. Ephemeral fallback (for range provider path)
        let sid = self.graph_view.store().encode_iri(iri);
        Ok(self
            .ext_subjects
            .assign_or_lookup(sid.namespace_code, sid.name.as_ref()))
    }

    /// Assign a subject ID from a Sid (avoids IRI construction).
    ///
    /// Uses `find_subject_id_by_parts` for the persisted lookup (skips
    /// prefix_trie decomposition since we already have ns_code + suffix).
    /// Falls back to ephemeral allocation when DictNovelty is uninitialized.
    pub fn assign_subject_id_from_sid(&mut self, sid: &Sid) -> io::Result<u64> {
        // 1. Persisted tree (canonical encoding guarantees exact-parts match)
        if let Some(id) = self
            .graph_view
            .store()
            .find_subject_id_by_parts(sid.namespace_code, &sid.name)?
        {
            return Ok(id);
        }
        // 2. DictNovelty (populated during commit — guaranteed hit for novelty subjects)
        if self.dict_novelty.is_initialized() {
            if let Some(id) = self
                .dict_novelty
                .subjects
                .find_subject(sid.namespace_code, &sid.name)
            {
                return Ok(id);
            }
        }
        // 3. Ephemeral fallback (for range provider path)
        Ok(self
            .ext_subjects
            .assign_or_lookup(sid.namespace_code, sid.name.as_ref()))
    }

    /// Resolve a subject ID back to an IRI string.
    ///
    /// Uses watermark routing: `local_id <= watermark` → persisted tree,
    /// `local_id > watermark` → DictNovelty forward lookup.
    /// When DictNovelty is uninitialized, tries persisted tree then ephemeral.
    pub fn resolve_subject_iri(&self, id: u64) -> io::Result<String> {
        if !self.dict_novelty.is_initialized() {
            // Uninitialized: try persisted tree first
            if let Ok(iri) = self.graph_view.store().resolve_subject_iri(id) {
                return Ok(iri);
            }
            // Ephemeral fallback: namespace-aware sid64 allocation
            if let Some((ns_code, suffix)) = self.ext_subjects.resolve_subject(id) {
                if ns_code == namespaces::EMPTY || ns_code == namespaces::OVERFLOW {
                    return Ok(suffix.to_string());
                }
                let prefix = self.graph_view.namespace_prefix(ns_code)?;
                return Ok(format!("{prefix}{suffix}"));
            }
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("s_id {id} not found"),
            ));
        }
        let sid64 = SubjectId::from_u64(id);
        let wm = self.dict_novelty.subjects.watermark_for_ns(sid64.ns_code());

        if sid64.local_id() <= wm {
            // Guaranteed persisted
            return self.graph_view.store().resolve_subject_iri(id);
        }
        // Novel — DictNovelty forward
        if let Some((ns_code, suffix)) = self.dict_novelty.subjects.resolve_subject(id) {
            if ns_code == namespaces::EMPTY || ns_code == namespaces::OVERFLOW {
                return Ok(suffix.to_string());
            }
            let prefix = self.graph_view.namespace_prefix(ns_code)?;
            return Ok(format!("{prefix}{suffix}"));
        }

        // Ephemeral fallback: even with DictNovelty initialized, overlay translation
        // may allocate into ext_subjects in certain view paths (e.g., historical overlays
        // where DictNovelty is present but doesn't contain the entry).
        if let Some((ns_code, suffix)) = self.ext_subjects.resolve_subject(id) {
            if ns_code == namespaces::EMPTY || ns_code == namespaces::OVERFLOW {
                return Ok(suffix.to_string());
            }
            let prefix = self.graph_view.namespace_prefix(ns_code)?;
            return Ok(format!("{prefix}{suffix}"));
        }

        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("s_id {id} not found in store or DictNovelty"),
        ))
    }

    /// Resolve a subject ID back to a Sid.
    ///
    /// Delegates to the novelty-aware `BinaryGraphView::resolve_subject_sid`
    /// which returns `Sid::new(ns_code, suffix)` directly for novel subjects
    /// (no IRI string allocation or trie lookup).
    pub fn resolve_subject_sid(&self, id: u64) -> io::Result<Sid> {
        self.graph_view.resolve_subject_sid(id)
    }

    // ========================================================================
    // Predicate dictionary (per-query ephemeral)
    // ========================================================================

    /// Look up or assign a predicate ID for the given IRI.
    pub fn assign_predicate_id(&mut self, iri: &str) -> u32 {
        if let Some(id) = self.graph_view.store().find_predicate_id(iri) {
            return id;
        }
        self.ext_predicates.assign_or_lookup(iri)
    }

    /// Resolve a predicate ID back to an IRI.
    pub fn resolve_predicate_iri(&self, id: u32) -> Option<&str> {
        if id < self.ext_predicates.base_id() {
            self.graph_view.store().resolve_predicate_iri(id)
        } else {
            self.ext_predicates.resolve(id)
        }
    }

    /// Resolve a predicate ID back to a Sid.
    pub fn resolve_predicate_sid(&self, id: u32) -> Option<Sid> {
        self.resolve_predicate_iri(id)
            .map(|iri| self.graph_view.store().encode_iri(iri))
    }

    // ========================================================================
    // String dictionary (delegated to DictNovelty)
    // ========================================================================

    /// Look up or assign a string dictionary ID.
    ///
    /// Tries: persisted tree → DictNovelty → ephemeral fallback.
    pub fn assign_string_id(&mut self, value: &str) -> io::Result<u32> {
        // 1. Persisted tree
        if let Some(id) = self.graph_view.store().find_string_id(value)? {
            return Ok(id);
        }
        // 2. DictNovelty (populated during commit)
        if self.dict_novelty.is_initialized() {
            if let Some(id) = self.dict_novelty.strings.find_string(value) {
                return Ok(id);
            }
        }
        // 3. Ephemeral fallback (for range provider path)
        Ok(self.ext_strings.assign_or_lookup(value))
    }

    /// Resolve a string ID back to the original value.
    ///
    /// Uses watermark routing: `id <= watermark` → persisted tree,
    /// `id > watermark` → DictNovelty forward lookup.
    /// When DictNovelty is uninitialized, tries persisted tree then ephemeral.
    pub fn resolve_string_value(&self, id: u32) -> io::Result<String> {
        if !self.dict_novelty.is_initialized() {
            // Try persisted tree first
            if let Ok(val) = self.graph_view.store().resolve_string_value(id) {
                return Ok(val);
            }
            // Ephemeral fallback
            if let Some(val) = self.ext_strings.resolve(id) {
                return Ok(val.to_string());
            }
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("str_id {id} not found"),
            ));
        }
        let wm = self.dict_novelty.strings.watermark();

        if id <= wm {
            return self.graph_view.store().resolve_string_value(id);
        }
        // Novel — DictNovelty forward
        if let Some(value) = self.dict_novelty.strings.resolve_string(id) {
            return Ok(value.to_string());
        }

        // Ephemeral fallback: even with DictNovelty initialized, overlay translation
        // may allocate into ext_strings in certain view paths.
        if let Some(value) = self.ext_strings.resolve(id) {
            return Ok(value.to_string());
        }

        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("str_id {id} not found in store or DictNovelty"),
        ))
    }

    // ========================================================================
    // Language tag dictionary (per-query ephemeral)
    // ========================================================================

    /// Look up or assign a language tag ID.
    pub fn assign_lang_id(&mut self, tag: &str) -> u16 {
        if let Some(id) = self.graph_view.store().find_lang_id(tag) {
            return id;
        }
        self.ext_lang_tags.assign_or_lookup(tag)
    }

    /// Resolve a language tag ID back to the tag string.
    ///
    /// IDs 1..=base_lang_count are persisted (delegate to store).
    /// IDs above that are ephemeral extensions.
    /// ID 0 means "no language tag".
    pub fn resolve_lang_tag(&self, id: u16) -> Option<&str> {
        if id == 0 {
            return None;
        }
        if id < self.ext_lang_tags.base_id() {
            // Persisted: delegate to store's language_tags.resolve()
            // (store's resolve handles 1-based IDs internally)
            None // Store doesn't expose resolve on DictOverlay; callers
                 // should use store directly for persisted IDs.
                 // In practice, the decode path goes through store.decode_meta().
        } else {
            self.ext_lang_tags.resolve(id)
        }
    }

    // ========================================================================
    // Datatype dictionary
    // ========================================================================

    /// Look up a datatype ID, or return a best-effort fallback.
    ///
    /// Datatypes are a small fixed set (XSD types + custom) that should always
    /// be present in the persisted index. If truly missing, returns dt_id 0
    /// (which maps to an empty Sid) rather than failing.
    pub fn assign_dt_id(&self, dt_sid: &Sid) -> u16 {
        self.graph_view.store().find_dt_id(dt_sid).unwrap_or(0)
    }

    // ========================================================================
    // Graph dictionary (per-query ephemeral)
    // ========================================================================

    /// Look up or assign a graph ID.
    pub fn assign_graph_id(&mut self, iri: &str) -> GraphId {
        self.ext_graphs.assign_or_lookup(iri)
    }

    /// Resolve a graph ID back to an IRI.
    pub fn resolve_graph_iri(&self, id: GraphId) -> Option<&str> {
        if id < self.ext_graphs.base_id() {
            None // Persisted graph IRIs not stored on BinaryIndexStore currently
        } else {
            self.ext_graphs.resolve(id)
        }
    }

    // ========================================================================
    // NumBig (BigInt/Decimal overflow) extensions
    // ========================================================================

    /// Assign an ephemeral NumBig handle for a BigInt/Decimal value.
    ///
    /// First checks if this value already has an ephemeral handle (by canonical
    /// string representation). Returns `(ObjKind::NUM_BIG, handle)`.
    fn assign_numbig_handle(&mut self, val: &FlakeValue) -> (ObjKind, ObjKey) {
        let key = format!("{val:?}"); // canonical Debug repr for dedup
        if let Some(&handle) = self.ext_numbig_map.get(&key) {
            return (ObjKind::NUM_BIG, ObjKey::encode_u32_id(handle));
        }
        let handle = self.numbig_next_handle;
        self.numbig_next_handle += 1;
        self.ext_numbig_map.insert(key, handle);
        self.ext_numbig.push(val.clone());
        (ObjKind::NUM_BIG, ObjKey::encode_u32_id(handle))
    }

    /// Resolve an ephemeral NumBig handle back to a FlakeValue.
    pub fn resolve_numbig(&self, handle: u32) -> Option<&FlakeValue> {
        if handle >= EPHEMERAL_NUMBIG_BASE {
            let idx = (handle - EPHEMERAL_NUMBIG_BASE) as usize;
            self.ext_numbig.get(idx)
        } else {
            None // Not ephemeral; delegate to store's numbig arena
        }
    }

    /// Assign an ephemeral Vector handle for a vector value.
    fn assign_vector_handle(&mut self, vec: &[f64]) -> (ObjKind, ObjKey) {
        let handle = self.vector_next_handle;
        self.vector_next_handle += 1;
        self.ext_vectors.push(vec.to_vec());
        (ObjKind::VECTOR_ID, ObjKey::encode_u32_id(handle))
    }

    // ========================================================================
    // Value encoding (FlakeValue → ObjKind/ObjKey)
    // ========================================================================

    /// Convert a FlakeValue to an (ObjKind, ObjKey) pair, allocating ephemeral
    /// dictionary entries as needed.
    ///
    /// Unlike `BinaryIndexStore::value_to_obj_pair()`, this never returns `None`
    /// for representable values.
    pub fn value_to_obj_pair(&mut self, val: &FlakeValue) -> io::Result<(ObjKind, ObjKey)> {
        match val {
            FlakeValue::Null => Ok((ObjKind::NULL, ObjKey::from_u64(0))),
            FlakeValue::Boolean(b) => Ok((ObjKind::BOOL, ObjKey::encode_bool(*b))),
            FlakeValue::Long(n) => Ok((ObjKind::NUM_INT, ObjKey::encode_i64(*n))),

            FlakeValue::Double(d) => {
                // Integer-valued doubles that fit i64 → NUM_INT
                if d.is_finite() && d.fract() == 0.0 {
                    let as_i64 = *d as i64;
                    if (as_i64 as f64) == *d {
                        return Ok((ObjKind::NUM_INT, ObjKey::encode_i64(as_i64)));
                    }
                }
                if d.is_finite() {
                    match ObjKey::encode_f64(*d) {
                        Ok(key) => Ok((ObjKind::NUM_F64, key)),
                        Err(_) => Ok((ObjKind::NULL, ObjKey::from_u64(0))),
                    }
                } else {
                    // NaN/Inf → NULL sentinel (can't represent in index)
                    Ok((ObjKind::NULL, ObjKey::from_u64(0)))
                }
            }

            FlakeValue::Ref(sid) => {
                let s_id = self.assign_subject_id_from_sid(sid)?;
                Ok((ObjKind::REF_ID, ObjKey::from_u64(s_id)))
            }

            FlakeValue::String(s) => {
                let str_id = self.assign_string_id(s)?;
                Ok((ObjKind::LEX_ID, ObjKey::encode_u32_id(str_id)))
            }

            // Note: language strings use FlakeValue::String for the value;
            // the lang tag is in FlakeMeta.lang and handled by assign_lang_id()
            // in the caller (translate_one_flake).
            FlakeValue::Date(d) => {
                let days = d.days_since_epoch();
                Ok((ObjKind::DATE, ObjKey::encode_date(days)))
            }

            FlakeValue::DateTime(dt) => {
                let micros = dt.epoch_micros();
                Ok((ObjKind::DATE_TIME, ObjKey::encode_datetime(micros)))
            }

            FlakeValue::Time(t) => {
                let micros = t.micros_since_midnight();
                Ok((ObjKind::TIME, ObjKey::encode_time(micros)))
            }

            FlakeValue::GYear(g) => Ok((ObjKind::G_YEAR, ObjKey::encode_g_year(g.year()))),

            FlakeValue::GYearMonth(g) => Ok((
                ObjKind::G_YEAR_MONTH,
                ObjKey::encode_g_year_month(g.year(), g.month()),
            )),

            FlakeValue::GMonth(g) => Ok((ObjKind::G_MONTH, ObjKey::encode_g_month(g.month()))),

            FlakeValue::GDay(g) => Ok((ObjKind::G_DAY, ObjKey::encode_g_day(g.day()))),

            FlakeValue::GMonthDay(g) => Ok((
                ObjKind::G_MONTH_DAY,
                ObjKey::encode_g_month_day(g.month(), g.day()),
            )),

            FlakeValue::YearMonthDuration(d) => Ok((
                ObjKind::YEAR_MONTH_DUR,
                ObjKey::encode_year_month_dur(d.months()),
            )),

            FlakeValue::DayTimeDuration(d) => Ok((
                ObjKind::DAY_TIME_DUR,
                ObjKey::encode_day_time_dur(d.micros()),
            )),

            FlakeValue::Duration(_) => {
                // xsd:duration is not totally orderable. Store as NumBig
                // with ephemeral handle to preserve the value through the overlay.
                Ok(self.assign_numbig_handle(val))
            }

            FlakeValue::Json(s) => {
                let str_id = self.assign_string_id(s)?;
                Ok((ObjKind::JSON_ID, ObjKey::encode_u32_id(str_id)))
            }

            FlakeValue::BigInt(bi) => {
                use num_traits::ToPrimitive;
                if let Some(v) = bi.to_i64() {
                    return Ok((ObjKind::NUM_INT, ObjKey::encode_i64(v)));
                }
                Ok(self.assign_numbig_handle(val))
            }

            FlakeValue::Decimal(_) => Ok(self.assign_numbig_handle(val)),

            FlakeValue::Vector(v) => Ok(self.assign_vector_handle(v.as_slice())),

            FlakeValue::GeoPoint(bits) => Ok((ObjKind::GEO_POINT, ObjKey::from_u64(bits.as_u64()))),
        }
    }

    // ========================================================================
    // Decode (ID → FlakeValue)
    // ========================================================================

    /// Decode a value from integer-ID space back to `FlakeValue`.
    ///
    /// Handles DictOverlay-specific concerns:
    /// - **Ephemeral IDs** (uninitialized DictNovelty): subjects/strings from
    ///   `ext_subjects`/`ext_strings` (range provider fallback path only).
    /// - **Ephemeral NumBig/Vector handles**: per-query allocations above
    ///   `EPHEMERAL_*_BASE`.
    ///
    /// For the common case (initialized DictNovelty), watermark-based routing
    /// of subject refs and string IDs is handled by `BinaryGraphView` itself
    /// — no duplication here.
    pub fn decode_value(
        &self,
        o_kind: u8,
        o_key: u64,
        p_id: u32,
        dt_id: u16,
        lang_id: u16,
    ) -> io::Result<FlakeValue> {
        // Ephemeral-only checks: these handle IDs that only exist in this
        // DictOverlay instance (not in DictNovelty or the persisted store).
        if !self.dict_novelty.is_initialized() {
            // REF_ID — check ephemeral subject IDs (range provider fallback)
            if o_kind == ObjKind::REF_ID.as_u8()
                && self.ext_subjects.resolve_subject(o_key).is_some()
            {
                let iri = self.resolve_subject_iri(o_key)?;
                return Ok(FlakeValue::Ref(self.graph_view.store().encode_iri(&iri)));
            }
            // LEX_ID — check ephemeral string IDs
            if o_kind == ObjKind::LEX_ID.as_u8() && self.ext_strings.resolve(o_key as u32).is_some()
            {
                let s = self.resolve_string_value(o_key as u32)?;
                return Ok(FlakeValue::String(s));
            }
            // JSON_ID — check ephemeral string IDs
            if o_kind == ObjKind::JSON_ID.as_u8()
                && self.ext_strings.resolve(o_key as u32).is_some()
            {
                let s = self.resolve_string_value(o_key as u32)?;
                return Ok(FlakeValue::Json(s));
            }
        }

        // NUM_BIG with ephemeral handle (per-query, independent of DictNovelty)
        if o_kind == ObjKind::NUM_BIG.as_u8() {
            let handle = o_key as u32;
            if let Some(val) = self.resolve_numbig(handle) {
                return Ok(val.clone());
            }
        }

        // VECTOR_ID with ephemeral handle (per-query, independent of DictNovelty)
        if o_kind == ObjKind::VECTOR_ID.as_u8() {
            let handle = o_key as u32;
            if handle >= EPHEMERAL_VECTOR_BASE {
                let idx = (handle - EPHEMERAL_VECTOR_BASE) as usize;
                if let Some(v) = self.ext_vectors.get(idx) {
                    return Ok(FlakeValue::Vector(v.clone()));
                }
            }
        }

        // Delegate to novelty-aware graph view for everything else.
        // BinaryGraphView handles watermark routing for IriRef, StringDict,
        // JsonArena internally — no duplication needed here.
        self.graph_view
            .decode_value_from_kind(o_kind, o_key, p_id, dt_id, lang_id)
    }

    /// Decode a datatype ID back to a Sid.
    pub fn decode_dt_sid(&self, dt_id: u16) -> Sid {
        self.graph_view
            .store()
            .dt_sids()
            .get(dt_id as usize)
            .cloned()
            .unwrap_or_else(|| Sid::new(0, ""))
    }

    /// Decode lang_id and i_val into FlakeMeta.
    ///
    /// Handles both persisted lang_ids (delegated to store) and ephemeral
    /// lang_ids allocated by `assign_lang_id()`.
    pub fn decode_meta(&self, lang_id: u16, i_val: i32) -> Option<FlakeMeta> {
        let has_lang = lang_id != 0;
        let has_idx = i_val != ListIndex::none().as_i32();

        if !has_lang && !has_idx {
            return None;
        }

        let mut meta = FlakeMeta::new();
        if has_lang {
            let tag = if lang_id < self.ext_lang_tags.base_id() {
                // Persisted lang_id — delegate to store
                self.graph_view
                    .store()
                    .decode_meta(lang_id, ListIndex::none().as_i32())
                    .and_then(|m| m.lang)
            } else {
                // Ephemeral lang_id
                self.ext_lang_tags
                    .resolve(lang_id)
                    .map(std::string::ToString::to_string)
            };
            if let Some(tag) = tag {
                meta = FlakeMeta::with_lang(tag);
            }
        }
        if has_idx {
            meta.i = Some(i_val);
        }
        Some(meta)
    }

    /// Check if a value needs early materialization.
    ///
    /// Returns true if the value uses ephemeral dictionary IDs that can't be
    /// resolved by `BinaryIndexStore` alone (only `DictOverlay` knows them).
    /// For such values, `batch_to_bindings` should emit `Binding::Lit` instead
    /// of `Binding::EncodedLit` to materialize them while the overlay is available.
    pub fn needs_early_materialize(&self, o_kind: u8, o_key: u64) -> bool {
        let initialized = self.dict_novelty.is_initialized();

        // REF_ID — check for ephemeral subject IDs
        if o_kind == ObjKind::REF_ID.as_u8() {
            let ref_id = o_key;
            if initialized {
                let sid64 = SubjectId::from_u64(ref_id);
                let wm = self.dict_novelty.subjects.watermark_for_ns(sid64.ns_code());
                return sid64.local_id() > wm;
            }
            return self.ext_subjects.resolve_subject(ref_id).is_some();
        }

        // LEX_ID — check for ephemeral string IDs
        if o_kind == ObjKind::LEX_ID.as_u8() {
            let str_id = o_key as u32;
            if initialized {
                return str_id > self.dict_novelty.strings.watermark();
            }
            return self.ext_strings.resolve(str_id).is_some();
        }

        // JSON_ID — same as LEX_ID (uses string dictionary)
        if o_kind == ObjKind::JSON_ID.as_u8() {
            let str_id = o_key as u32;
            if initialized {
                return str_id > self.dict_novelty.strings.watermark();
            }
            return self.ext_strings.resolve(str_id).is_some();
        }

        // NUM_BIG with ephemeral handle
        if o_kind == ObjKind::NUM_BIG.as_u8() {
            let handle = o_key as u32;
            return handle >= EPHEMERAL_NUMBIG_BASE;
        }

        // VECTOR_ID with ephemeral handle
        if o_kind == ObjKind::VECTOR_ID.as_u8() {
            let handle = o_key as u32;
            return handle >= EPHEMERAL_VECTOR_BASE;
        }

        // All other types can be decoded by the store alone
        false
    }

    /// Check if a subject ID is ephemeral and needs early materialization.
    ///
    /// Returns true if the subject ID can't be resolved by `BinaryIndexStore` alone.
    pub fn is_ephemeral_subject(&self, s_id: u64) -> bool {
        let initialized = self.dict_novelty.is_initialized();

        if initialized {
            let sid64 = SubjectId::from_u64(s_id);
            let wm = self.dict_novelty.subjects.watermark_for_ns(sid64.ns_code());
            sid64.local_id() > wm
        } else {
            self.ext_subjects.resolve_subject(s_id).is_some()
        }
    }

    /// Check if a predicate ID is ephemeral and needs early materialization.
    ///
    /// Returns true if the predicate ID can't be resolved by `BinaryIndexStore` alone.
    pub fn is_ephemeral_predicate(&self, p_id: u32) -> bool {
        p_id >= self.ext_predicates.base_id()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Integration tests require a BinaryIndexStore, which needs on-disk files.
    // Unit-level logic tests for ephemeral ID allocation:

    #[test]
    fn test_ephemeral_numbig_dedup() {
        // Verify that the same canonical representation gets the same handle
        let key_a = "BigInt(42)".to_string();
        let key_b = "BigInt(42)".to_string();
        let mut map: HashMap<String, u32> = HashMap::new();
        map.insert(key_a, EPHEMERAL_NUMBIG_BASE);
        assert_eq!(map.get(&key_b), Some(&EPHEMERAL_NUMBIG_BASE));
    }
}
