//! ImportSink — a `GraphSink` that streams triples directly to a `StreamingCommitWriter`.
//!
//! Like [`FlakeSink`](crate::flake_sink::FlakeSink), this resolves parser events
//! (IRIs, blank nodes, literals) into `Flake` components. But instead of
//! accumulating `Vec<Flake>`, each flake is immediately pushed to a
//! [`StreamingCommitWriter`](crate::commit_v2::StreamingCommitWriter) which
//! encodes the op and spools it through zstd compression to a tempfile.
//!
//! This eliminates the intermediate flake buffer entirely, keeping memory
//! bounded regardless of how large the input is.
//!
//! ## Spool integration (Tier 2)
//!
//! When a [`SpoolContext`] is attached, each triple is also written as a 44-byte
//! [`RunRecord`](fluree_db_binary_index::RunRecord) to a spool file.
//! Subject and string IDs are **chunk-local** sequential counters assigned by
//! per-chunk dictionaries ([`ChunkSubjectDict`], [`ChunkStringDict`]). After all
//! chunks are parsed, a merge pass deduplicates across chunks and builds remap
//! tables. Parallel remap threads then convert chunk-local IDs to global IDs
//! and produce sorted run files for the index builder.
//!
//! Predicates, datatypes, and graphs use **globally-assigned IDs** via
//! [`SharedDictAllocator`] worker caches — no remap needed for those domains.

mod inner {
    use crate::commit_v2::StreamingCommitWriter;
    use crate::generate::{infer_datatype, DT_ID, DT_JSON};
    use crate::namespace::{NamespaceRegistry, NsAllocator, SharedNamespaceAllocator, WorkerCache};
    use crate::value_convert::{convert_native_literal, convert_string_literal};
    use fluree_db_binary_index::format::run_record::LIST_INDEX_NONE;
    use fluree_db_binary_index::RunRecord;
    use fluree_db_core::commit::codec::CommitCodecError;
    use fluree_db_core::subject_id::SubjectId;
    use fluree_db_core::value_id::{ObjKey, ObjKind};
    use fluree_db_core::DatatypeConstraint;
    use fluree_db_core::{Flake, FlakeMeta, FlakeValue, GraphId, Sid};
    use fluree_db_indexer::run_index::chunk_dict::{ChunkStringDict, ChunkSubjectDict};
    use fluree_db_indexer::run_index::global_dict::{DictWorkerCache, SharedDictAllocator};
    use fluree_db_indexer::run_index::shared_pool::{SharedNumBigPool, SharedVectorArenaPool};
    use fluree_db_indexer::run_index::spool::{SpoolFileInfo, SpoolWriter};
    use fluree_graph_ir::{Datatype, GraphSink, LiteralValue, TermId};
    use rustc_hash::FxHashMap;
    use std::collections::HashMap;
    use std::sync::Arc;

    // -----------------------------------------------------------------------
    // ResolvedTerm — local term representation (same shape as FlakeSink's)
    // -----------------------------------------------------------------------

    /// A term resolved to its Flake-ready form.
    enum ResolvedTerm {
        /// IRI or blank node (already resolved to a Sid)
        Sid(Sid),
        /// Literal value with datatype constraint
        Literal {
            value: FlakeValue,
            dtc: DatatypeConstraint,
        },
    }

    // -----------------------------------------------------------------------
    // SpoolContext — per-chunk context for writing spool records (Phase B)
    // -----------------------------------------------------------------------

    /// Configuration for creating a [`SpoolContext`] — bundles all shared
    /// allocators needed by the parallel import pipeline.
    pub struct SpoolConfig {
        /// Shared predicate allocator (global IDs, no remap).
        pub predicate_alloc: Arc<SharedDictAllocator>,
        /// Shared datatype allocator (global IDs, no remap).
        pub datatype_alloc: Arc<SharedDictAllocator>,
        /// Shared graph allocator (global IDs, no remap).
        pub graph_alloc: Arc<SharedDictAllocator>,
        /// Shared numbig pool (global handles, no remap).
        pub numbig_pool: Arc<SharedNumBigPool>,
        /// Shared vector pool (global handles, no remap).
        pub vector_pool: Arc<SharedVectorArenaPool>,
        /// Shared namespace allocator (for prefix lookup).
        pub ns_alloc: Arc<SharedNamespaceAllocator>,
    }

    /// Result of finishing a [`SpoolContext`] via [`SpoolContext::finish`] —
    /// contains the spool file info and the chunk-local dictionaries needed for
    /// the merge phase.
    pub struct SpoolResult {
        /// Spool file metadata (path, record count).
        pub spool_info: SpoolFileInfo,
        /// Chunk-local subject dictionary (chunk-local ID → ns_code + name).
        pub subjects: ChunkSubjectDict,
        /// Chunk-local string dictionary (chunk-local ID → string bytes).
        pub strings: ChunkStringDict,
    }

    /// Result of finishing a [`SpoolContext`] via [`SpoolContext::finish_buffered`] —
    /// returns the buffered records in memory (no spool file written) along with
    /// chunk-local dictionaries and language map.
    pub struct BufferedSpoolResult {
        /// All buffered RunRecords (insertion-order, chunk-local IDs).
        pub records: Vec<RunRecord>,
        /// Chunk-local subject dictionary (chunk-local ID → ns_code + name).
        pub subjects: ChunkSubjectDict,
        /// Chunk-local string dictionary (chunk-local ID → string bytes).
        pub strings: ChunkStringDict,
        /// Per-chunk language tags (tag string → chunk-local lang_id).
        pub languages: rustc_hash::FxHashMap<String, u16>,
        /// Chunk index (for deterministic ordering in merge phase).
        pub chunk_idx: usize,
    }

    /// Per-chunk context for writing spool records during parse (Phase B).
    ///
    /// **Subjects and strings** get chunk-local sequential IDs via
    /// [`ChunkSubjectDict`] / [`ChunkStringDict`]. These are remapped to
    /// global IDs after a merge pass.
    ///
    /// **Predicates, datatypes, and graphs** get globally unique IDs via
    /// [`DictWorkerCache`] backed by [`SharedDictAllocator`]. No remap needed.
    ///
    /// **BigInt/Decimal and vectors** get global handles via shared pools.
    /// No remap needed.
    pub struct SpoolContext {
        /// Buffered records (insertion-order, chunk-local IDs).
        records: Vec<RunRecord>,
        /// Path for writing spool file (used by `finish()` backward-compat path).
        spool_path: std::path::PathBuf,
        chunk_idx: usize,
        // Chunk-local dicts (need remap after merge)
        subjects: ChunkSubjectDict,
        strings: ChunkStringDict,
        // Shared global allocators (no remap needed)
        predicates: DictWorkerCache,
        datatypes: DictWorkerCache,
        // Kept for: TriG named graph support in parallel import.
        // Use when: T2.13 orchestration enables TriG parallel parsing.
        #[expect(dead_code)]
        graphs: DictWorkerCache,
        // Shared global pools (no remap needed)
        numbig_pool: Arc<SharedNumBigPool>,
        vector_pool: Arc<SharedVectorArenaPool>,
        // Namespace prefix cache (ns_code → prefix string)
        ns_alloc: Arc<SharedNamespaceAllocator>,
        ns_prefix_cache: FxHashMap<u16, String>,
        // Per-chunk language tags (remapped at run-write time)
        languages: FxHashMap<String, u16>,
        next_lang_id: u16,
        /// Graph ID for all records in this chunk (0 = default).
        g_id: GraphId,
    }

    impl SpoolContext {
        /// Create a new spool context with shared allocators.
        ///
        /// `chunk_idx` identifies this chunk (0-based) for deterministic merge ordering.
        /// `g_id` is the graph ID for all records (0 = default graph).
        pub fn new(
            spool_path: impl Into<std::path::PathBuf>,
            chunk_idx: usize,
            g_id: GraphId,
            config: &SpoolConfig,
        ) -> std::io::Result<Self> {
            Ok(Self {
                records: Vec::new(),
                spool_path: spool_path.into(),
                chunk_idx,
                subjects: ChunkSubjectDict::new(),
                strings: ChunkStringDict::new(),
                predicates: DictWorkerCache::new(Arc::clone(&config.predicate_alloc)),
                datatypes: DictWorkerCache::new(Arc::clone(&config.datatype_alloc)),
                graphs: DictWorkerCache::new(Arc::clone(&config.graph_alloc)),
                numbig_pool: Arc::clone(&config.numbig_pool),
                vector_pool: Arc::clone(&config.vector_pool),
                ns_alloc: Arc::clone(&config.ns_alloc),
                ns_prefix_cache: FxHashMap::default(),
                languages: FxHashMap::default(),
                next_lang_id: 1, // 0 = no language tag
                g_id,
            })
        }

        /// Number of records buffered so far.
        pub fn record_count(&self) -> u64 {
            self.records.len() as u64
        }

        /// Finish the spool context by writing buffered records to a spool file.
        ///
        /// Returns [`SpoolResult`] containing the spool file info and chunk-local
        /// dictionaries for the merge phase. This is the backward-compatible path
        /// used by the existing import pipeline.
        pub fn finish(self) -> Result<SpoolResult, std::io::Error> {
            let mut writer = SpoolWriter::new(&self.spool_path, self.chunk_idx)?;
            for record in &self.records {
                writer.push(record)?;
            }
            let spool_info = writer.finish()?;
            Ok(SpoolResult {
                spool_info,
                subjects: self.subjects,
                strings: self.strings,
            })
        }

        /// Finish the spool context, returning buffered records in memory.
        ///
        /// No spool file is written. The returned [`BufferedSpoolResult`]
        /// contains all records with chunk-local IDs ready for post-parse
        /// sorting and sorted commit file writing.
        pub fn finish_buffered(self) -> BufferedSpoolResult {
            BufferedSpoolResult {
                records: self.records,
                subjects: self.subjects,
                strings: self.strings,
                languages: self.languages,
                chunk_idx: self.chunk_idx,
            }
        }

        // -- ID assignment helpers --

        /// Assign a chunk-local subject ID. Uses `ChunkSubjectDict` with
        /// xxh3_128 hashing of `(ns_code, name_bytes)`.
        fn assign_subject_id(&mut self, sid: &Sid) -> u64 {
            self.subjects
                .get_or_insert(sid.namespace_code, sid.name.as_bytes())
        }

        /// Assign a global predicate ID via `DictWorkerCache`.
        fn assign_predicate_id(&mut self, sid: &Sid) -> u32 {
            // Look up the namespace prefix, then use parts-based insertion
            // to get a global predicate ID.
            //
            // Important: avoid returning a `&str` from a `&mut self` helper here,
            // because that would borrow the whole `SpoolContext` mutably and
            // prevent a simultaneous mutable borrow of `self.predicates`.
            let code = sid.namespace_code;
            if !self.ns_prefix_cache.contains_key(&code) {
                let prefix = self.ns_alloc.get_prefix(code).unwrap_or_default();
                self.ns_prefix_cache.insert(code, prefix);
            }
            let prefix = self
                .ns_prefix_cache
                .get(&code)
                .map(std::string::String::as_str)
                .unwrap_or("");
            self.predicates.get_or_insert_parts(prefix, &sid.name)
        }

        /// Assign a global datatype ID via `DictWorkerCache`.
        fn assign_datatype_id(&mut self, sid: &Sid) -> u16 {
            let code = sid.namespace_code;
            if !self.ns_prefix_cache.contains_key(&code) {
                let prefix = self.ns_alloc.get_prefix(code).unwrap_or_default();
                self.ns_prefix_cache.insert(code, prefix);
            }
            let prefix = self
                .ns_prefix_cache
                .get(&code)
                .map(std::string::String::as_str)
                .unwrap_or("");
            self.datatypes.get_or_insert_parts(prefix, &sid.name) as u16
        }

        /// Assign a chunk-local string ID via `ChunkStringDict`.
        fn assign_string_id(&mut self, s: &str) -> u32 {
            self.strings.get_or_insert(s.as_bytes())
        }

        fn assign_lang_id(&mut self, lang: &str) -> u16 {
            if let Some(&id) = self.languages.get(lang) {
                return id;
            }
            let id = self.next_lang_id;
            self.next_lang_id += 1;
            self.languages.insert(lang.to_string(), id);
            id
        }

        /// Convert a FlakeValue to (ObjKind, o_key).
        ///
        /// Uses the same encoders as `CommitResolver` in the indexer for inline
        /// types. For subjects and strings, assigns chunk-local IDs (remapped
        /// during merge). For BigInt/Decimal/Vector, uses shared global pools.
        #[allow(clippy::too_many_arguments)]
        fn resolve_object_value(&mut self, value: &FlakeValue, p_id: u32) -> Option<(u8, u64)> {
            Some(match value {
                FlakeValue::Ref(sid) => {
                    let local_id = self.assign_subject_id(sid);
                    (ObjKind::REF_ID.as_u8(), local_id)
                }
                FlakeValue::String(s) => {
                    let id = self.assign_string_id(s);
                    (ObjKind::LEX_ID.as_u8(), ObjKey::encode_u32_id(id).as_u64())
                }
                FlakeValue::Long(v) => (ObjKind::NUM_INT.as_u8(), ObjKey::encode_i64(*v).as_u64()),
                FlakeValue::Double(v) => {
                    // NOTE: Do not optimize integral doubles to NUM_INT here.
                    // The decode path uses the property's datatype to select
                    // DecodeKind::F64, which would reinterpret integer-encoded
                    // bits as IEEE 754 floats, producing garbage. (fluree/db-r#142)
                    match ObjKey::encode_f64(*v) {
                        Ok(key) => (ObjKind::NUM_F64.as_u8(), key.as_u64()),
                        Err(_) => {
                            let id = self.assign_string_id(&v.to_string());
                            (ObjKind::LEX_ID.as_u8(), ObjKey::encode_u32_id(id).as_u64())
                        }
                    }
                }
                FlakeValue::Boolean(b) => (ObjKind::BOOL.as_u8(), ObjKey::encode_bool(*b).as_u64()),
                FlakeValue::Null => (ObjKind::NULL.as_u8(), 0),
                FlakeValue::DateTime(dt) => (
                    ObjKind::DATE_TIME.as_u8(),
                    ObjKey::encode_datetime(dt.epoch_micros()).as_u64(),
                ),
                FlakeValue::Date(d) => (
                    ObjKind::DATE.as_u8(),
                    ObjKey::encode_date(d.days_since_epoch()).as_u64(),
                ),
                FlakeValue::Time(t) => (
                    ObjKind::TIME.as_u8(),
                    ObjKey::encode_time(t.micros_since_midnight()).as_u64(),
                ),
                FlakeValue::GYear(y) => (
                    ObjKind::G_YEAR.as_u8(),
                    ObjKey::encode_g_year(y.year()).as_u64(),
                ),
                FlakeValue::GYearMonth(ym) => (
                    ObjKind::G_YEAR_MONTH.as_u8(),
                    ObjKey::encode_g_year_month(ym.year(), ym.month()).as_u64(),
                ),
                FlakeValue::GMonth(m) => (
                    ObjKind::G_MONTH.as_u8(),
                    ObjKey::encode_g_month(m.month()).as_u64(),
                ),
                FlakeValue::GDay(d) => (
                    ObjKind::G_DAY.as_u8(),
                    ObjKey::encode_g_day(d.day()).as_u64(),
                ),
                FlakeValue::GMonthDay(md) => (
                    ObjKind::G_MONTH_DAY.as_u8(),
                    ObjKey::encode_g_month_day(md.month(), md.day()).as_u64(),
                ),
                FlakeValue::YearMonthDuration(ymd) => (
                    ObjKind::YEAR_MONTH_DUR.as_u8(),
                    ObjKey::encode_year_month_dur(ymd.months()).as_u64(),
                ),
                FlakeValue::DayTimeDuration(dtd) => (
                    ObjKind::DAY_TIME_DUR.as_u8(),
                    ObjKey::encode_day_time_dur(dtd.micros()).as_u64(),
                ),
                FlakeValue::Duration(dur) => {
                    let id = self.assign_string_id(dur.original());
                    (ObjKind::LEX_ID.as_u8(), ObjKey::encode_u32_id(id).as_u64())
                }
                FlakeValue::Json(json_str) => {
                    let id = self.assign_string_id(json_str);
                    (ObjKind::JSON_ID.as_u8(), ObjKey::encode_u32_id(id).as_u64())
                }
                FlakeValue::GeoPoint(bits) => (ObjKind::GEO_POINT.as_u8(), bits.0),
                FlakeValue::BigInt(bi) => {
                    use num_bigint::BigInt;
                    use std::convert::TryFrom;
                    if let Ok(v) = i64::try_from(bi.as_ref() as &BigInt) {
                        (ObjKind::NUM_INT.as_u8(), ObjKey::encode_i64(v).as_u64())
                    } else {
                        // Overflow: use shared numbig pool for global handle.
                        let handle =
                            self.numbig_pool
                                .get_or_insert_bigint(self.g_id, p_id, bi.as_ref());
                        (
                            ObjKind::NUM_BIG.as_u8(),
                            ObjKey::encode_u32_id(handle).as_u64(),
                        )
                    }
                }
                FlakeValue::Decimal(dec) => {
                    // Use shared numbig pool for global handle.
                    let handle =
                        self.numbig_pool
                            .get_or_insert_bigdec(self.g_id, p_id, dec.as_ref());
                    (
                        ObjKind::NUM_BIG.as_u8(),
                        ObjKey::encode_u32_id(handle).as_u64(),
                    )
                }
                FlakeValue::Vector(v) => {
                    // Use shared vector pool for global handle.
                    match self.vector_pool.insert_f64(self.g_id, p_id, v) {
                        Ok(handle) => (
                            ObjKind::VECTOR_ID.as_u8(),
                            ObjKey::encode_u32_id(handle).as_u64(),
                        ),
                        Err(e) => {
                            tracing::error!("SpoolContext: vector insert failed: {}", e);
                            return None;
                        }
                    }
                }
            })
        }

        /// Write a spool record for one flake.
        #[allow(clippy::too_many_arguments)]
        fn write_record(
            &mut self,
            s: &Sid,
            p: &Sid,
            o: &FlakeValue,
            dt: &Sid,
            lang: Option<&str>,
            list_index: Option<i32>,
            t: i64,
        ) {
            let s_id = self.assign_subject_id(s);
            let p_id = self.assign_predicate_id(p);
            let dt_id = self.assign_datatype_id(dt);
            let Some((o_kind, o_key)) = self.resolve_object_value(o, p_id) else {
                return; // skip spool record on unresolvable value (e.g. bad vector)
            };
            let lang_id = lang.map(|l| self.assign_lang_id(l)).unwrap_or(0);
            let i = list_index
                .map(|idx| {
                    debug_assert!(idx >= 0, "negative list index {idx} in import data");
                    u32::try_from(idx).unwrap_or(LIST_INDEX_NONE)
                })
                .unwrap_or(LIST_INDEX_NONE);

            let record = RunRecord {
                g_id: self.g_id,
                s_id: SubjectId::from_u64(s_id),
                p_id,
                dt: dt_id,
                o_kind,
                op: 1, // always assert during import
                o_key,
                t: t as u32,
                lang_id,
                i,
            };

            self.records.push(record);
        }
    }

    // -----------------------------------------------------------------------
    // ImportSink
    // -----------------------------------------------------------------------

    /// A `GraphSink` that streams parsed triples directly to a commit-v2 writer.
    ///
    /// Optionally also writes spool records via an attached [`SpoolContext`].
    ///
    /// # Example
    ///
    /// ```ignore
    /// let mut sink = ImportSink::new(&mut ns, t, txn_id, true)?;
    /// fluree_graph_turtle::parse(ttl, &mut sink)?;
    /// let writer = sink.finish();
    /// let result = writer.finish(&envelope)?;
    /// ```
    pub struct ImportSink<'a> {
        terms: Vec<ResolvedTerm>,
        blank_labels: HashMap<String, TermId>,
        blank_counter: u32,
        ns: NsAllocator<'a>,
        t: i64,
        txn_id: String,
        writer: StreamingCommitWriter,
        /// First encoding error encountered (checked after parse).
        encode_error: Option<CommitCodecError>,
        /// Turtle @prefix short names: IRI → short prefix (e.g., "http://example.org/" → "ex").
        /// Captured from `on_prefix()` calls during parse.
        prefix_map: HashMap<String, String>,
        /// Optional spool context for Tier 2 parallel pipeline.
        spool_ctx: Option<SpoolContext>,
    }

    impl<'a> ImportSink<'a> {
        /// Create an ImportSink for serial paths (chunk 0, TriG, small files).
        ///
        /// # Arguments
        /// * `ns_registry` — namespace registry (seeded from predefined codes)
        /// * `t` — transaction time
        /// * `txn_id` — unique ID for blank node skolemization
        /// * `compress` — whether to zstd-compress the ops stream
        pub fn new(
            ns_registry: &'a mut NamespaceRegistry,
            t: i64,
            txn_id: String,
            compress: bool,
        ) -> Result<Self, CommitCodecError> {
            Ok(Self {
                terms: Vec::new(),
                blank_labels: HashMap::new(),
                blank_counter: 0,
                ns: NsAllocator::Exclusive(ns_registry),
                t,
                txn_id,
                writer: StreamingCommitWriter::new(compress)?,
                encode_error: None,
                prefix_map: HashMap::new(),
                spool_ctx: None,
            })
        }

        /// Create an ImportSink for parallel import workers.
        pub fn new_cached(
            worker_cache: &'a mut WorkerCache,
            t: i64,
            txn_id: String,
            compress: bool,
        ) -> Result<Self, CommitCodecError> {
            Ok(Self {
                terms: Vec::new(),
                blank_labels: HashMap::new(),
                blank_counter: 0,
                ns: NsAllocator::Cached(worker_cache),
                t,
                txn_id,
                writer: StreamingCommitWriter::new(compress)?,
                encode_error: None,
                prefix_map: HashMap::new(),
                spool_ctx: None,
            })
        }

        /// Attach a spool context for writing spool records during parse.
        pub fn set_spool_context(&mut self, ctx: SpoolContext) {
            self.spool_ctx = Some(ctx);
        }

        /// Consume the sink and return the writer for finalization.
        ///
        /// Returns an error if any flake failed to encode during parsing.
        #[allow(clippy::type_complexity)]
        pub fn finish(
            self,
        ) -> Result<
            (
                StreamingCommitWriter,
                HashMap<String, String>,
                Option<SpoolContext>,
            ),
            CommitCodecError,
        > {
            if let Some(err) = self.encode_error {
                return Err(err);
            }
            Ok((self.writer, self.prefix_map, self.spool_ctx))
        }

        // -- helpers ---------------------------------------------------------

        fn add_term(&mut self, term: ResolvedTerm) -> TermId {
            let id = TermId::new(self.terms.len() as u32);
            self.terms.push(term);
            id
        }

        fn skolemize(&mut self, local: &str) -> Sid {
            let unique_id = format!("{}-{}", self.txn_id, local);
            self.ns.blank_node_sid(&unique_id)
        }

        fn resolve_sid(&self, id: TermId) -> Option<Sid> {
            match &self.terms[id.index() as usize] {
                ResolvedTerm::Sid(sid) => Some(sid.clone()),
                ResolvedTerm::Literal { .. } => None,
            }
        }

        fn resolve_object(&self, id: TermId) -> Option<(FlakeValue, DatatypeConstraint)> {
            match &self.terms[id.index() as usize] {
                ResolvedTerm::Sid(sid) => Some((
                    FlakeValue::Ref(sid.clone()),
                    DatatypeConstraint::Explicit(DT_ID.clone()),
                )),
                ResolvedTerm::Literal { value, dtc } => Some((value.clone(), dtc.clone())),
            }
        }

        fn push_triple(
            &mut self,
            subject: TermId,
            predicate: TermId,
            object: TermId,
            list_index: Option<i32>,
        ) {
            let Some(s) = self.resolve_sid(subject) else {
                return;
            };
            let Some(p) = self.resolve_sid(predicate) else {
                return;
            };
            let Some((o, dtc)) = self.resolve_object(object) else {
                return;
            };

            let dt = dtc.datatype().clone();
            let lang = dtc.lang_tag().map(std::string::ToString::to_string);

            let meta = match (&lang, list_index) {
                (Some(l), Some(i)) => Some(FlakeMeta {
                    lang: Some(l.clone()),
                    i: Some(i),
                }),
                (Some(l), None) => Some(FlakeMeta::with_lang(l)),
                (None, Some(i)) => Some(FlakeMeta::with_index(i)),
                (None, None) => None,
            };

            let flake = Flake::new(
                s.clone(),
                p.clone(),
                o.clone(),
                dt.clone(),
                self.t,
                true,
                meta,
            );
            if let Err(e) = self.writer.push_flake(&flake) {
                if self.encode_error.is_none() {
                    tracing::error!("ImportSink: flake encode failed: {}", e);
                    self.encode_error = Some(e);
                }
                return; // Don't spool a flake that failed to encode
            }

            // Write spool record only after commit encoding succeeded
            if let Some(ctx) = &mut self.spool_ctx {
                ctx.write_record(&s, &p, &o, &dt, lang.as_deref(), list_index, self.t);
            }
        }
    }

    impl GraphSink for ImportSink<'_> {
        fn on_base(&mut self, _base_iri: &str) {
            // No-op — parser resolves relative IRIs before calling term_iri
        }

        fn on_prefix(&mut self, prefix: &str, namespace_iri: &str) {
            self.ns.get_or_allocate(namespace_iri);
            if !prefix.is_empty() {
                self.prefix_map
                    .insert(namespace_iri.to_string(), prefix.to_string());
            }
        }

        fn term_iri(&mut self, iri: &str) -> TermId {
            let sid = self.ns.sid_for_iri(iri);
            self.add_term(ResolvedTerm::Sid(sid))
        }

        fn term_blank(&mut self, label: Option<&str>) -> TermId {
            match label {
                Some(l) => {
                    if let Some(&id) = self.blank_labels.get(l) {
                        return id;
                    }
                    let sid = self.skolemize(l);
                    let id = self.add_term(ResolvedTerm::Sid(sid));
                    self.blank_labels.insert(l.to_string(), id);
                    id
                }
                None => {
                    self.blank_counter += 1;
                    let label = format!("b{}", self.blank_counter);
                    let sid = self.skolemize(&label);
                    self.add_term(ResolvedTerm::Sid(sid))
                }
            }
        }

        fn term_literal(
            &mut self,
            value: &str,
            datatype: Datatype,
            language: Option<&str>,
        ) -> TermId {
            let dt_iri = datatype.as_iri();
            let (flake_value, dt_sid) = convert_string_literal(value, dt_iri, &mut self.ns);

            let dtc = match language {
                Some(lang) => DatatypeConstraint::LangTag(Arc::from(lang)),
                None => DatatypeConstraint::Explicit(dt_sid),
            };

            self.add_term(ResolvedTerm::Literal {
                value: flake_value,
                dtc,
            })
        }

        fn term_literal_value(&mut self, value: LiteralValue, datatype: Datatype) -> TermId {
            let flake_value = convert_native_literal(&value);
            let dt_sid = if datatype.is_json() {
                DT_JSON.clone()
            } else {
                infer_datatype(&flake_value)
            };

            self.add_term(ResolvedTerm::Literal {
                value: flake_value,
                dtc: DatatypeConstraint::Explicit(dt_sid),
            })
        }

        fn emit_triple(&mut self, subject: TermId, predicate: TermId, object: TermId) {
            self.push_triple(subject, predicate, object, None);
        }

        fn emit_list_item(
            &mut self,
            subject: TermId,
            predicate: TermId,
            object: TermId,
            index: i32,
        ) {
            self.push_triple(subject, predicate, object, Some(index));
        }
    }

    // -----------------------------------------------------------------------
    // Tests
    // -----------------------------------------------------------------------

    #[cfg(test)]
    mod tests {
        use super::*;
        use fluree_db_core::commit::codec::read_commit;
        use fluree_db_indexer::run_index::global_dict::PredicateDict;

        fn make_sink_and_parse(
            ns: &mut NamespaceRegistry,
            t: i64,
        ) -> Result<ImportSink<'_>, CommitCodecError> {
            ImportSink::new(ns, t, "test-txn".to_string(), true)
        }

        fn make_envelope(t: i64) -> crate::commit_v2::CodecEnvelope {
            crate::commit_v2::CodecEnvelope {
                t,
                previous_refs: Vec::new(),
                namespace_delta: HashMap::new(),
                txn: None,
                time: None,
                txn_signature: None,
                txn_meta: Vec::new(),
                graph_delta: HashMap::new(),
                ns_split_mode: None,
            }
        }

        /// Create a test SpoolConfig with empty shared allocators.
        fn make_spool_config(ns: &NamespaceRegistry) -> SpoolConfig {
            SpoolConfig {
                predicate_alloc: Arc::new(SharedDictAllocator::from_predicate_dict(
                    &PredicateDict::new(),
                )),
                datatype_alloc: Arc::new(SharedDictAllocator::from_predicate_dict(
                    &PredicateDict::new(),
                )),
                graph_alloc: Arc::new(SharedDictAllocator::from_predicate_dict(
                    &PredicateDict::new(),
                )),
                numbig_pool: Arc::new(SharedNumBigPool::new()),
                vector_pool: Arc::new(SharedVectorArenaPool::new()),
                ns_alloc: Arc::new(SharedNamespaceAllocator::from_registry(ns)),
            }
        }

        #[test]
        fn test_basic_iri_triple() {
            let mut ns = NamespaceRegistry::new();
            let mut sink = make_sink_and_parse(&mut ns, 1).unwrap();

            let s = sink.term_iri("http://example.org/alice");
            let p = sink.term_iri("http://example.org/name");
            let o = sink.term_literal("Alice", Datatype::xsd_string(), None);
            sink.emit_triple(s, p, o);

            let (writer, _prefix_map, _spool) = sink.finish().unwrap();
            assert_eq!(writer.op_count(), 1);

            let result = writer.finish(&make_envelope(1)).unwrap();
            let decoded = read_commit(&result.bytes).unwrap();
            assert_eq!(decoded.flakes.len(), 1);
            assert!(decoded.flakes[0].op);
            assert!(matches!(&decoded.flakes[0].o, FlakeValue::String(s) if s == "Alice"));
        }

        #[test]
        fn test_all_value_types() {
            let mut ns = NamespaceRegistry::new();
            let mut sink = make_sink_and_parse(&mut ns, 1).unwrap();

            let s = sink.term_iri("http://example.org/x");

            // String
            let p = sink.term_iri("http://example.org/str");
            let o = sink.term_literal("hello", Datatype::xsd_string(), None);
            sink.emit_triple(s, p, o);

            // Integer
            let p = sink.term_iri("http://example.org/num");
            let o = sink.term_literal_value(LiteralValue::Integer(42), Datatype::xsd_integer());
            sink.emit_triple(s, p, o);

            // Double
            let p = sink.term_iri("http://example.org/dbl");
            let o = sink.term_literal_value(LiteralValue::Double(3.13), Datatype::xsd_double());
            sink.emit_triple(s, p, o);

            // Boolean
            let p = sink.term_iri("http://example.org/flag");
            let o = sink.term_literal_value(LiteralValue::Boolean(true), Datatype::xsd_boolean());
            sink.emit_triple(s, p, o);

            // Ref (IRI in object position)
            let p = sink.term_iri("http://example.org/knows");
            let o = sink.term_iri("http://example.org/bob");
            sink.emit_triple(s, p, o);

            let (writer, _prefix_map, _spool) = sink.finish().unwrap();
            assert_eq!(writer.op_count(), 5);

            let result = writer.finish(&make_envelope(1)).unwrap();
            let decoded = read_commit(&result.bytes).unwrap();
            assert_eq!(decoded.flakes.len(), 5);
            assert!(matches!(&decoded.flakes[0].o, FlakeValue::String(s) if s == "hello"));
            assert!(matches!(&decoded.flakes[1].o, FlakeValue::Long(42)));
            assert!(matches!(&decoded.flakes[2].o, FlakeValue::Double(_)));
            assert!(matches!(&decoded.flakes[3].o, FlakeValue::Boolean(true)));
            assert!(matches!(&decoded.flakes[4].o, FlakeValue::Ref(_)));
        }

        #[test]
        fn test_language_tagged_literal() {
            let mut ns = NamespaceRegistry::new();
            let mut sink = make_sink_and_parse(&mut ns, 1).unwrap();

            let s = sink.term_iri("http://example.org/alice");
            let p = sink.term_iri("http://example.org/name");
            let o = sink.term_literal("Alice", Datatype::rdf_lang_string(), Some("en"));
            sink.emit_triple(s, p, o);

            let (writer, _prefix_map, _spool) = sink.finish().unwrap();
            let result = writer.finish(&make_envelope(1)).unwrap();
            let decoded = read_commit(&result.bytes).unwrap();

            assert_eq!(decoded.flakes.len(), 1);
            let f = &decoded.flakes[0];
            assert_eq!(f.dt, Sid::new(fluree_vocab::namespaces::RDF, "langString"));
            let meta = f.m.as_ref().expect("should have meta");
            assert_eq!(meta.lang.as_deref(), Some("en"));
        }

        #[test]
        fn test_blank_node_consistency() {
            let mut ns = NamespaceRegistry::new();
            let mut sink = make_sink_and_parse(&mut ns, 1).unwrap();

            let b1 = sink.term_blank(Some("foo"));
            let b2 = sink.term_blank(Some("foo"));
            let b3 = sink.term_blank(Some("bar"));
            let b4 = sink.term_blank(None);

            assert_eq!(b1, b2);
            assert_ne!(b1, b3);
            assert_ne!(b3, b4);
        }

        #[test]
        fn test_list_items() {
            let mut ns = NamespaceRegistry::new();
            let mut sink = make_sink_and_parse(&mut ns, 1).unwrap();

            let s = sink.term_iri("http://example.org/alice");
            let p = sink.term_iri("http://example.org/scores");
            let o0 = sink.term_literal_value(LiteralValue::Integer(10), Datatype::xsd_integer());
            let o1 = sink.term_literal_value(LiteralValue::Integer(20), Datatype::xsd_integer());
            let o2 = sink.term_literal_value(LiteralValue::Integer(30), Datatype::xsd_integer());
            sink.emit_list_item(s, p, o0, 0);
            sink.emit_list_item(s, p, o1, 1);
            sink.emit_list_item(s, p, o2, 2);

            let (writer, _prefix_map, _spool) = sink.finish().unwrap();
            assert_eq!(writer.op_count(), 3);

            let result = writer.finish(&make_envelope(1)).unwrap();
            let decoded = read_commit(&result.bytes).unwrap();
            assert_eq!(decoded.flakes.len(), 3);
            for (i, f) in decoded.flakes.iter().enumerate() {
                let meta = f.m.as_ref().expect("list items should have meta");
                assert_eq!(meta.i, Some(i as i32));
            }
        }

        #[test]
        fn test_spool_integration_basic() {
            let dir = std::env::temp_dir().join("fluree_test_spool_sink");
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).unwrap();

            let mut ns = NamespaceRegistry::new();
            let config = make_spool_config(&ns);
            let mut sink = ImportSink::new(&mut ns, 1, "test-txn".to_string(), true).unwrap();

            // Attach spool context
            let spool_path = dir.join("chunk_0.spool");
            let spool_ctx = SpoolContext::new(&spool_path, 0, 0, &config).unwrap();
            sink.set_spool_context(spool_ctx);

            // Emit triples — both commit and spool should get records
            let s = sink.term_iri("http://example.org/alice");
            let p_name = sink.term_iri("http://example.org/name");
            let p_age = sink.term_iri("http://example.org/age");
            let p_knows = sink.term_iri("http://example.org/knows");
            let o_name = sink.term_literal("Alice", Datatype::xsd_string(), None);
            let o_age = sink.term_literal_value(LiteralValue::Integer(30), Datatype::xsd_integer());
            let bob = sink.term_iri("http://example.org/bob");
            sink.emit_triple(s, p_name, o_name);
            sink.emit_triple(s, p_age, o_age);
            sink.emit_triple(s, p_knows, bob);

            let (writer, _prefix_map, spool_ctx) = sink.finish().unwrap();
            assert_eq!(writer.op_count(), 3);

            // Verify spool recorded the same number of records
            let spool_ctx = spool_ctx.expect("spool context should be returned");
            assert_eq!(spool_ctx.record_count(), 3);
            let result = spool_ctx.finish().unwrap();
            assert_eq!(result.spool_info.record_count, 3);

            // Read back and verify structure
            use fluree_db_indexer::run_index::spool::SpoolReader;
            let reader =
                SpoolReader::open(&result.spool_info.path, result.spool_info.record_count).unwrap();
            let records: Vec<_> = reader.map(|r| r.unwrap()).collect();
            assert_eq!(records.len(), 3);

            // First record: "Alice" (string) → should be LEX_ID
            assert_eq!(records[0].o_kind, ObjKind::LEX_ID.as_u8());
            // Second record: 30 (integer) → should be NUM_INT
            assert_eq!(records[1].o_kind, ObjKind::NUM_INT.as_u8());
            // Third record: bob (IRI ref) → should be REF_ID
            assert_eq!(records[2].o_kind, ObjKind::REF_ID.as_u8());

            // All should have same subject (alice) — same s_id
            assert_eq!(records[0].s_id, records[1].s_id);
            assert_eq!(records[1].s_id, records[2].s_id);

            // All should have op=1 (assert) and t=1
            for r in &records {
                assert_eq!(r.op, 1);
                assert_eq!(r.t, 1);
                assert_eq!(r.g_id, 0);
            }

            // Different predicates → different p_ids
            assert_ne!(records[0].p_id, records[1].p_id);
            assert_ne!(records[1].p_id, records[2].p_id);

            let _ = std::fs::remove_dir_all(&dir);
        }

        #[test]
        fn test_spool_with_language_and_list_index() {
            let dir = std::env::temp_dir().join("fluree_test_spool_lang_list");
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).unwrap();

            let mut ns = NamespaceRegistry::new();
            let config = make_spool_config(&ns);
            let mut sink = ImportSink::new(&mut ns, 1, "test-txn".to_string(), true).unwrap();

            let spool_path = dir.join("chunk_0.spool");
            let spool_ctx = SpoolContext::new(&spool_path, 0, 0, &config).unwrap();
            sink.set_spool_context(spool_ctx);

            let s = sink.term_iri("http://example.org/alice");

            // Language-tagged string
            let p = sink.term_iri("http://example.org/name");
            let o = sink.term_literal("Alice", Datatype::rdf_lang_string(), Some("en"));
            sink.emit_triple(s, p, o);

            // List items
            let p = sink.term_iri("http://example.org/scores");
            let o0 = sink.term_literal_value(LiteralValue::Integer(10), Datatype::xsd_integer());
            let o1 = sink.term_literal_value(LiteralValue::Integer(20), Datatype::xsd_integer());
            sink.emit_list_item(s, p, o0, 0);
            sink.emit_list_item(s, p, o1, 1);

            let (_writer, _prefix_map, spool_ctx) = sink.finish().unwrap();
            let spool_ctx = spool_ctx.unwrap();
            assert_eq!(spool_ctx.record_count(), 3);
            let result = spool_ctx.finish().unwrap();

            use fluree_db_indexer::run_index::spool::SpoolReader;
            let reader =
                SpoolReader::open(&result.spool_info.path, result.spool_info.record_count).unwrap();
            let records: Vec<_> = reader.map(|r| r.unwrap()).collect();

            // First: language-tagged string → lang_id > 0
            assert_ne!(records[0].lang_id, 0);

            // Second and third: list items → i = 0, 1
            assert_eq!(records[1].i, 0);
            assert_eq!(records[2].i, 1);

            let _ = std::fs::remove_dir_all(&dir);
        }

        #[test]
        fn test_spool_ref_dedup() {
            let dir = std::env::temp_dir().join("fluree_test_spool_dedup");
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).unwrap();

            let mut ns = NamespaceRegistry::new();
            let config = make_spool_config(&ns);
            let mut sink = ImportSink::new(&mut ns, 1, "test-txn".to_string(), true).unwrap();

            let spool_path = dir.join("chunk_0.spool");
            let spool_ctx = SpoolContext::new(&spool_path, 0, 0, &config).unwrap();
            sink.set_spool_context(spool_ctx);

            // Same subject used twice, same object ref used twice
            let alice = sink.term_iri("http://example.org/alice");
            let bob = sink.term_iri("http://example.org/bob");
            let p1 = sink.term_iri("http://example.org/knows");
            let p2 = sink.term_iri("http://example.org/likes");
            sink.emit_triple(alice, p1, bob);
            sink.emit_triple(alice, p2, bob);

            let (_writer, _prefix_map, spool_ctx) = sink.finish().unwrap();
            let spool_ctx = spool_ctx.unwrap();
            let result = spool_ctx.finish().unwrap();

            use fluree_db_indexer::run_index::spool::SpoolReader;
            let reader =
                SpoolReader::open(&result.spool_info.path, result.spool_info.record_count).unwrap();
            let records: Vec<_> = reader.map(|r| r.unwrap()).collect();
            assert_eq!(records.len(), 2);

            // Same subject → same s_id
            assert_eq!(records[0].s_id, records[1].s_id);
            // Bob as object ref → same o_key (deduped)
            assert_eq!(records[0].o_kind, ObjKind::REF_ID.as_u8());
            assert_eq!(records[1].o_kind, ObjKind::REF_ID.as_u8());
            assert_eq!(records[0].o_key, records[1].o_key);

            let _ = std::fs::remove_dir_all(&dir);
        }
    }
}

pub use inner::BufferedSpoolResult;
pub use inner::ImportSink;
pub use inner::SpoolConfig;
pub use inner::SpoolContext;
pub use inner::SpoolResult;
