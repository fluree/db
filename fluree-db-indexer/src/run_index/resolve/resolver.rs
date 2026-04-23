//! CommitResolver: transforms RawOps into RunRecords using global dictionaries.
//!
//! This is the core of Phase B -- dictionary resolution. For each commit's ops:
//! 1. Look up namespace prefix from ns_code
//! 2. Hash prefix + name using streaming xxh3_128 (no IRI concatenation on hot path)
//! 3. Resolve to global u32 ID via SubjectDict/PredicateDict
//! 4. Encode object value as (ObjKind, ObjKey)
//! 5. Resolve datatype -> dict ID from (dt_ns_code, dt_name)
//! 6. Emit RunRecord

use super::global_dict::GlobalDicts;
use crate::run_index::runs::run_writer::RecordSink;
use bigdecimal::BigDecimal;
use chrono;
use fluree_db_binary_index::format::run_record::{RunRecord, LIST_INDEX_NONE};
use fluree_db_core::commit::codec::envelope::CodecEnvelope;
use fluree_db_core::commit::codec::raw_reader::{CommitOps, RawObject, RawOp};
use fluree_db_core::commit::codec::{load_commit_ops, CommitCodecError};
use fluree_db_core::subject_id::SubjectId;
use fluree_db_core::temporal::{
    Date, DateTime, DayTimeDuration, Duration as XsdDuration, GDay, GMonth, GMonthDay, GYear,
    GYearMonth, Time, YearMonthDuration,
};
use fluree_db_core::value_id::{ObjKey, ObjKind};
use fluree_db_core::DatatypeDictId;
use fluree_db_core::GraphId;
use fluree_vocab::namespaces::{FLUREE_COMMIT, FLUREE_DB, FLUREE_URN};
use fluree_vocab::{db, fluree};

/// Namespace codes reserved for Fluree system provenance. User-supplied
/// `TxnMetaEntry`s must never land here — `extract_txn_meta` in
/// fluree-db-transact rejects them at parse time. This list is duplicated
/// (not imported) to avoid adding a transact → indexer layering inversion;
/// the two sites are asserted in sync by the debug_assert in emit paths.
const RESERVED_PREDICATE_NAMESPACES: &[u16] = &[FLUREE_DB, FLUREE_COMMIT, FLUREE_URN];
use num_bigint::BigInt;
use rustc_hash::FxHashMap;
use std::collections::HashMap;
use std::io;
use xxhash_rust::xxh3::Xxh3;

/// Statistics for a single resolved commit.
#[derive(Debug, Clone, Copy)]
pub struct ResolvedCommit {
    /// Total records emitted (ops + txn-meta records).
    pub total_records: u32,
    /// Transaction time of this commit.
    pub t: u32,
    /// Size of the commit blob in bytes.
    pub size: u64,
    /// Number of assertions in this commit.
    pub asserts: u32,
    /// Number of retractions in this commit.
    pub retracts: u32,
}

/// Resolves commit-local ops into globally-addressed RunRecords.
pub struct CommitResolver {
    /// namespace_code -> prefix IRI.
    /// Seeded from `default_namespace_codes()`, updated by commit namespace_deltas.
    ///
    /// **Invariant:** ns_code -> prefix is stable once assigned. A namespace
    /// delta can introduce new codes but never changes existing mappings.
    ns_prefixes: HashMap<u16, String>,
    /// Reusable xxh3 streaming hasher (avoids per-op hasher construction).
    hasher: Xxh3,
    /// Optional per-(graph, property) stats hook. When set, `on_record()` is
    /// called for every resolved user-data op (not txn-meta).
    stats_hook: Option<crate::stats::IdStatsHook>,
    /// Optional spatial geometry collection hook. When set, `on_op()` is
    /// called for every resolved user-data op to collect non-POINT WKT geometries.
    spatial_hook: Option<crate::spatial_hook::SpatialHook>,
    /// Optional fulltext collection hook. When set, `on_op()` is called for
    /// every resolved user-data op to route `@fulltext`-typed string values
    /// and values on configured full-text properties into BM25 arena building.
    fulltext_hook: Option<crate::fulltext_hook::FulltextHook>,
    /// Per-indexing-run configured full-text property set. Built once from
    /// the effective per-graph `ResolvedConfig.full_text.properties` and
    /// consulted on every op to decide whether a plain-string value should
    /// be collected. Empty by default so the `@fulltext`-datatype path keeps
    /// working without any config setup.
    fulltext_hook_config: crate::fulltext_hook::FulltextHookConfig,
}

impl CommitResolver {
    /// Create a new resolver seeded with the default namespace prefix mappings.
    pub fn new() -> Self {
        Self {
            ns_prefixes: fluree_db_core::default_namespace_codes(),
            hasher: Xxh3::new(),
            stats_hook: None,
            spatial_hook: None,
            fulltext_hook: None,
            fulltext_hook_config: crate::fulltext_hook::FulltextHookConfig::default(),
        }
    }

    /// Set the ID-based stats hook for per-op stats collection.
    pub fn set_stats_hook(&mut self, hook: crate::stats::IdStatsHook) {
        self.stats_hook = Some(hook);
    }

    /// Take the stats hook out of the resolver (for finalization / merge).
    pub fn take_stats_hook(&mut self) -> Option<crate::stats::IdStatsHook> {
        self.stats_hook.take()
    }

    /// Set the spatial geometry collection hook for non-POINT WKT geometries.
    pub fn set_spatial_hook(&mut self, hook: crate::spatial_hook::SpatialHook) {
        self.spatial_hook = Some(hook);
    }

    /// Take the spatial hook out of the resolver (for finalization).
    pub fn take_spatial_hook(&mut self) -> Option<crate::spatial_hook::SpatialHook> {
        self.spatial_hook.take()
    }

    /// Set the fulltext collection hook for `@fulltext`-typed literals.
    pub fn set_fulltext_hook(&mut self, hook: crate::fulltext_hook::FulltextHook) {
        self.fulltext_hook = Some(hook);
    }

    /// Set the configured full-text property set for this indexing run.
    pub fn set_fulltext_hook_config(&mut self, config: crate::fulltext_hook::FulltextHookConfig) {
        self.fulltext_hook_config = config;
    }

    /// Take the fulltext hook out of the resolver (for finalization).
    pub fn take_fulltext_hook(&mut self) -> Option<crate::fulltext_hook::FulltextHook> {
        self.fulltext_hook.take()
    }

    /// Apply a commit's namespace delta to update prefix mappings.
    ///
    /// New namespace codes are added; existing codes are never overwritten
    /// (the prefix for a code is stable once assigned).
    pub fn apply_namespace_delta(&mut self, delta: &HashMap<u16, String>) {
        for (&code, prefix) in delta {
            self.ns_prefixes
                .entry(code)
                .or_insert_with(|| prefix.clone());
        }
    }

    /// Resolve one commit's ops into RunRecords, pushing them to the writer.
    ///
    /// Returns `(asserts, retracts)` - the count of assertions and retractions.
    pub fn resolve_commit_ops<W: RecordSink>(
        &mut self,
        commit_ops: &CommitOps,
        dicts: &mut GlobalDicts,
        writer: &mut W,
    ) -> Result<(u32, u32), ResolverError> {
        let t = u32::try_from(commit_ops.t).map_err(|_| {
            ResolverError::Resolve(format!("commit t={} does not fit in u32", commit_ops.t))
        })?;
        let mut asserts = 0u32;
        let mut retracts = 0u32;

        commit_ops.for_each_op(|raw_op: RawOp<'_>| {
            let record = self.resolve_single_op(&raw_op, t, dicts)?;

            // Feed resolved record to ID-based stats hook (user-data ops only)
            if let Some(ref mut hook) = self.stats_hook {
                // IMPORTANT: `record.dt` is the binary run's datatype-dict ID (dt_id),
                // not `fluree_db_core::ValueTypeTag`. For stats we want stable datatypes,
                // so derive ValueTypeTag from the commit's declared datatype IRI.
                let dt = fluree_db_core::value_id::ValueTypeTag::from_ns_name(
                    raw_op.dt_ns_code,
                    raw_op.dt_name,
                );
                hook.on_record(&crate::stats::StatsRecord {
                    g_id: record.g_id,
                    p_id: record.p_id,
                    s_id: record.s_id.as_u64(),
                    dt,
                    o_hash: crate::stats::value_hash(record.o_kind, record.o_key),
                    o_kind: record.o_kind,
                    o_key: record.o_key,
                    t: record.t as i64,
                    op: record.op != 0,
                    lang_id: record.lang_id,
                });
            }

            // Feed raw op to spatial hook (needs raw WKT string + resolved IDs)
            if let Some(ref mut spatial) = self.spatial_hook {
                spatial.on_op(
                    &raw_op,
                    record.g_id,
                    record.s_id.as_u64(),
                    record.p_id,
                    t as i64,
                );
            }

            // Feed resolved record to fulltext hook — routes `@fulltext`-datatype
            // values (always English) and configured-property values (language
            // from lang_id / default_language) into BM25 arena building.
            if let Some(ref mut ft) = self.fulltext_hook {
                ft.on_op(crate::fulltext_hook::FulltextOpInput {
                    g_id: record.g_id,
                    p_id: record.p_id,
                    dt_id: record.dt,
                    o_kind: record.o_kind,
                    o_key: record.o_key,
                    lang_id: record.lang_id,
                    t: t as i64,
                    is_assert: record.op != 0,
                    config: &self.fulltext_hook_config,
                });
            }

            writer
                .push(record, &mut dicts.languages)
                .map_err(|e| CommitCodecError::InvalidOp(format!("run writer error: {e}")))?;
            if record.op != 0 {
                asserts += 1;
            } else {
                retracts += 1;
            }
            Ok(())
        })?;

        Ok((asserts, retracts))
    }

    /// Resolve a raw commit blob end-to-end: parse, apply namespace delta, resolve ops,
    /// and emit txn-meta records.
    ///
    /// Convenience wrapper that combines [`load_commit_ops`], [`apply_namespace_delta`],
    /// [`resolve_commit_ops`], and [`emit_txn_meta`]. Returns [`ResolvedCommit`] with
    /// per-commit statistics for accumulation.
    pub fn resolve_blob<W: RecordSink>(
        &mut self,
        bytes: &[u8],
        commit_hash_hex: &str,
        dicts: &mut GlobalDicts,
        writer: &mut W,
    ) -> Result<ResolvedCommit, ResolverError> {
        let commit_size = bytes.len() as u64;
        let commit_ops = load_commit_ops(bytes)?;
        self.apply_namespace_delta(&commit_ops.envelope.namespace_delta);
        let (asserts, retracts) = self.resolve_commit_ops(&commit_ops, dicts, writer)?;
        let meta_count = self.emit_txn_meta(
            commit_hash_hex,
            &commit_ops.envelope,
            commit_size,
            asserts,
            retracts,
            dicts,
            writer,
        )?;
        Ok(ResolvedCommit {
            total_records: asserts + retracts + meta_count,
            t: u32::try_from(commit_ops.t).map_err(|_| {
                ResolverError::Resolve(format!("commit t={} does not fit in u32", commit_ops.t))
            })?,
            size: commit_size,
            asserts,
            retracts,
        })
    }

    /// Access the accumulated namespace prefix map (code -> prefix IRI).
    pub fn ns_prefixes(&self) -> &HashMap<u16, String> {
        &self.ns_prefixes
    }

    /// Emit txn-meta RunRecords for a single commit into the txn-meta graph (g_id=1).
    ///
    /// Emits commit metadata as queryable triples:
    /// - **Commit subject** (`fluree:commit:sha256:<hex>`): address, time, previous,
    ///   t, size (commit blob bytes), asserts, retracts.
    /// - **User metadata**: any `txn_meta` entries from the envelope.
    ///
    /// The `commit_hash_hex` parameter is the 64-char SHA-256 hex digest identifying
    /// the commit (typically from `ContentId::digest_hex()`).
    ///
    /// Returns the number of records emitted.
    #[allow(clippy::too_many_arguments)]
    pub fn emit_txn_meta<W: RecordSink>(
        &mut self,
        commit_hash_hex: &str,
        envelope: &CodecEnvelope,
        commit_size: u64,
        asserts: u32,
        retracts: u32,
        dicts: &mut GlobalDicts,
        writer: &mut W,
    ) -> Result<u32, ResolverError> {
        // 1. Validate commit hash hex
        let hex = commit_hash_hex;

        // 2. g_id=1 (pre-reserved in GlobalDicts/SharedResolverState construction)
        //    The txn-meta IRI is always the first graph entry (dict_id=0, g_id=0+1=1).
        let g_id: u16 = 1;

        let t = u32::try_from(envelope.t).map_err(|_| {
            ResolverError::Resolve(format!("commit t={} does not fit in u32", envelope.t))
        })?;

        // 3. Resolve commit subject: "fluree:commit:sha256:<hex>"
        let commit_iri = format!("{}{}", fluree::COMMIT, hex);
        let commit_s_id = dicts
            .subjects
            .get_or_insert(&commit_iri, fluree_vocab::namespaces::FLUREE_COMMIT)?;

        // 4. Resolve predicate p_ids
        let p_address = dicts
            .predicates
            .get_or_insert_parts(fluree::DB, db::ADDRESS);
        let p_time = dicts.predicates.get_or_insert_parts(fluree::DB, db::TIME);
        let p_previous = dicts
            .predicates
            .get_or_insert_parts(fluree::DB, db::PREVIOUS);
        let p_t = dicts.predicates.get_or_insert_parts(fluree::DB, db::T);
        let p_size = dicts.predicates.get_or_insert_parts(fluree::DB, db::SIZE);
        let p_asserts = dicts
            .predicates
            .get_or_insert_parts(fluree::DB, db::ASSERTS);
        let p_retracts = dicts
            .predicates
            .get_or_insert_parts(fluree::DB, db::RETRACTS);

        let mut count = 0u32;

        // Helper to push a record into the writer
        let mut push = |s_id: u64,
                        p_id: u32,
                        o_kind: ObjKind,
                        o_key: ObjKey,
                        dt: u16|
         -> Result<(), ResolverError> {
            let record = RunRecord {
                g_id,
                s_id: SubjectId::from_u64(s_id),
                p_id,
                dt,
                o_kind: o_kind.as_u8(),
                op: 1, // assert
                o_key: o_key.as_u64(),
                t,
                lang_id: 0,
                i: LIST_INDEX_NONE,
            };
            writer
                .push(record, &mut dicts.languages)
                .map_err(ResolverError::Io)?;
            count += 1;
            Ok(())
        };

        // === Commit subject records ===

        // ledger:address (STRING) — stores CID hex digest as the commit identifier
        let addr_str_id = dicts.strings.get_or_insert(commit_hash_hex)?;
        push(
            commit_s_id,
            p_address,
            ObjKind::LEX_ID,
            ObjKey::encode_u32_id(addr_str_id),
            DatatypeDictId::STRING.as_u16(),
        )?;

        // ledger:time (LONG) -- epoch milliseconds (skipped if ISO parse fails)
        if let Some(time_str) = &envelope.time {
            if let Some(epoch_ms) = iso_to_epoch_ms(time_str) {
                push(
                    commit_s_id,
                    p_time,
                    ObjKind::NUM_INT,
                    ObjKey::encode_i64(epoch_ms),
                    DatatypeDictId::LONG.as_u16(),
                )?;
            }
        }

        // ledger:t (INTEGER)
        push(
            commit_s_id,
            p_t,
            ObjKind::NUM_INT,
            ObjKey::encode_i64(t as i64),
            DatatypeDictId::INTEGER.as_u16(),
        )?;

        // ledger:size (LONG) -- commit blob size in bytes
        push(
            commit_s_id,
            p_size,
            ObjKind::NUM_INT,
            ObjKey::encode_i64(commit_size as i64),
            DatatypeDictId::LONG.as_u16(),
        )?;

        // ledger:asserts (INTEGER) -- number of assertions in this commit
        push(
            commit_s_id,
            p_asserts,
            ObjKind::NUM_INT,
            ObjKey::encode_i64(asserts as i64),
            DatatypeDictId::INTEGER.as_u16(),
        )?;

        // ledger:retracts (INTEGER) -- number of retractions in this commit
        push(
            commit_s_id,
            p_retracts,
            ObjKind::NUM_INT,
            ObjKey::encode_i64(retracts as i64),
            DatatypeDictId::INTEGER.as_u16(),
        )?;

        // ledger:previous (ID) -- ref to parent commit(s)
        for prev_ref in &envelope.previous_refs {
            // Use CID digest hex as the subject name in FLUREE_COMMIT namespace
            let prev_digest = prev_ref.id.digest_hex();
            let prev_s_id = dicts
                .subjects
                .get_or_insert(&prev_digest, fluree_vocab::namespaces::FLUREE_COMMIT)?;
            push(
                commit_s_id,
                p_previous,
                ObjKind::REF_ID,
                ObjKey::encode_sid64(prev_s_id),
                DatatypeDictId::ID.as_u16(),
            )?;
        }

        // ledger:author (STRING) -- transaction signer DID
        if let Some(txn_sig) = &envelope.txn_signature {
            let p_author = dicts.predicates.get_or_insert_parts(fluree::DB, db::AUTHOR);
            let author_str_id = dicts.strings.get_or_insert(&txn_sig.signer)?;
            push(
                commit_s_id,
                p_author,
                ObjKind::LEX_ID,
                ObjKey::encode_u32_id(author_str_id),
                DatatypeDictId::STRING.as_u16(),
            )?;
        }

        // ledger:txn (STRING) -- transaction CID string
        if let Some(txn_id) = &envelope.txn {
            let p_txn = dicts.predicates.get_or_insert_parts(fluree::DB, db::TXN);
            let txn_str = txn_id.to_string();
            let txn_str_id = dicts.strings.get_or_insert(&txn_str)?;
            push(
                commit_s_id,
                p_txn,
                ObjKind::LEX_ID,
                ObjKey::encode_u32_id(txn_str_id),
                DatatypeDictId::STRING.as_u16(),
            )?;
        }

        // === User-provided txn_meta entries ===
        for entry in &envelope.txn_meta {
            count += self.emit_txn_meta_entry(commit_s_id, g_id, t, entry, dicts, writer)?;
        }

        Ok(count)
    }

    /// Emit a single user-provided txn_meta entry as a RunRecord.
    fn emit_txn_meta_entry<W: RecordSink>(
        &mut self,
        commit_s_id: u64,
        g_id: u16,
        t: u32,
        entry: &fluree_db_novelty::TxnMetaEntry,
        dicts: &mut GlobalDicts,
        writer: &mut W,
    ) -> Result<u32, ResolverError> {
        debug_assert!(
            !RESERVED_PREDICATE_NAMESPACES.contains(&entry.predicate_ns),
            "TxnMetaEntry in reserved namespace {} reached resolver — extract_txn_meta guard bypassed?",
            entry.predicate_ns
        );

        // Resolve predicate using ns_code + name
        let p_prefix = self.lookup_prefix(entry.predicate_ns);
        let p_id = dicts
            .predicates
            .get_or_insert_parts(p_prefix, &entry.predicate_name);

        // Resolve value to (o_kind, o_key, dt, lang_id)
        let (o_kind, o_key, dt, lang_id) = self.resolve_txn_meta_value(&entry.value, dicts)?;

        let record = RunRecord {
            g_id,
            s_id: SubjectId::from_u64(commit_s_id),
            p_id,
            dt,
            o_kind: o_kind.as_u8(),
            op: 1, // assert
            o_key: o_key.as_u64(),
            t,
            lang_id,
            i: LIST_INDEX_NONE,
        };
        writer
            .push(record, &mut dicts.languages)
            .map_err(ResolverError::Io)?;

        Ok(1)
    }

    /// Resolve a TxnMetaValue to (ObjKind, ObjKey, dt_id, lang_id).
    fn resolve_txn_meta_value(
        &mut self,
        value: &fluree_db_novelty::TxnMetaValue,
        dicts: &mut GlobalDicts,
    ) -> Result<(ObjKind, ObjKey, u16, u16), ResolverError> {
        use fluree_db_novelty::TxnMetaValue;

        match value {
            TxnMetaValue::String(s) => {
                let str_id = dicts.strings.get_or_insert(s)?;
                Ok((
                    ObjKind::LEX_ID,
                    ObjKey::encode_u32_id(str_id),
                    DatatypeDictId::STRING.as_u16(),
                    0,
                ))
            }
            TxnMetaValue::Long(n) => Ok((
                ObjKind::NUM_INT,
                ObjKey::encode_i64(*n),
                DatatypeDictId::LONG.as_u16(),
                0,
            )),
            TxnMetaValue::Double(n) => {
                // Defense in depth: reject non-finite doubles even if envelope decode allowed them
                if !n.is_finite() {
                    return Err(ResolverError::Resolve(
                        "txn_meta does not support non-finite double values".into(),
                    ));
                }
                // Always encode as NUM_F64 to avoid NUM_INT + dt DOUBLE edge cases
                let key = ObjKey::encode_f64(*n)
                    .map_err(|e| ResolverError::Resolve(format!("txn_meta double: {e}")))?;
                Ok((ObjKind::NUM_F64, key, DatatypeDictId::DOUBLE.as_u16(), 0))
            }
            TxnMetaValue::Boolean(b) => Ok((
                ObjKind::BOOL,
                ObjKey::encode_bool(*b),
                DatatypeDictId::BOOLEAN.as_u16(),
                0,
            )),
            TxnMetaValue::Ref { ns, name } => {
                // Resolve ref IRI -> global sid64
                let prefix = self
                    .ns_prefixes
                    .get(ns)
                    .map(std::string::String::as_str)
                    .unwrap_or("");
                self.hasher.reset();
                self.hasher.update(prefix.as_bytes());
                self.hasher.update(name.as_bytes());
                let hash = self.hasher.digest128();

                let sid64 = dicts.subjects.get_or_insert_with_hash(hash, *ns, || {
                    let mut s = String::with_capacity(prefix.len() + name.len());
                    s.push_str(prefix);
                    s.push_str(name);
                    s
                })?;
                Ok((
                    ObjKind::REF_ID,
                    ObjKey::encode_sid64(sid64),
                    DatatypeDictId::ID.as_u16(),
                    0,
                ))
            }
            TxnMetaValue::LangString { value, lang } => {
                let str_id = dicts.strings.get_or_insert(value)?;
                let lang_id = dicts.languages.get_or_insert(Some(lang.as_str()));
                Ok((
                    ObjKind::LEX_ID,
                    ObjKey::encode_u32_id(str_id),
                    DatatypeDictId::LANG_STRING.as_u16(),
                    lang_id,
                ))
            }
            TxnMetaValue::TypedLiteral {
                value,
                dt_ns,
                dt_name,
            } => {
                // Store the value as a string, with custom datatype.
                //
                // NOTE: Legacy v3 datatype canonicalization (rewriting
                // corrupt shapes like `EMPTY + "xsd:string"` to
                // canonical `(XSD, "string")`) is applied at decode time
                // by `legacy_v3::read_commit_v3` / `read_commit_envelope_v3`
                // before the envelope reaches this point. By the time the
                // resolver sees `TxnMetaValue::TypedLiteral`, both v3 and
                // v4 commits carry canonical `(dt_ns, dt_name)` pairs.
                let str_id = dicts.strings.get_or_insert(value)?;
                let dt_prefix = self.lookup_prefix(*dt_ns);
                let dt_id = dicts.datatypes.get_or_insert_parts(dt_prefix, dt_name);
                // Match resolve_single_op()'s u8 constraint for format consistency
                if dt_id > u8::MAX as u32 {
                    return Err(ResolverError::Resolve(format!(
                        "txn_meta datatype dict overflow (dt_id={dt_id} exceeds u8 max)"
                    )));
                }
                Ok((
                    ObjKind::LEX_ID,
                    ObjKey::encode_u32_id(str_id),
                    dt_id as u16,
                    0,
                ))
            }
        }
    }

    /// Resolve a single RawOp into a RunRecord.
    fn resolve_single_op(
        &mut self,
        op: &RawOp<'_>,
        t: u32,
        dicts: &mut GlobalDicts,
    ) -> Result<RunRecord, CommitCodecError> {
        // 1. Resolve graph
        let g_id = self
            .resolve_graph(op.g_ns_code, op.g_name, dicts)
            .map_err(|e| CommitCodecError::InvalidOp(format!("graph resolve: {e}")))?;

        // 2. Resolve subject (streaming hash) → sid64
        let s_id = self
            .resolve_subject(op.s_ns_code, op.s_name, dicts)
            .map_err(|e| CommitCodecError::InvalidOp(format!("subject resolve: {e}")))?;

        // 3. Resolve predicate
        let p_id = self.resolve_predicate(op.p_ns_code, op.p_name, dicts);

        // 4. Resolve datatype via dict lookup (lossless -- any IRI gets an ID).
        //
        // V3 legacy canonicalization has already been applied at iteration
        // time by `CommitOps::for_each_op` when the ops came from
        // `legacy_v3::load_commit_ops_v3`; v4 ops come through clean. Either
        // way, `(op.dt_ns_code, op.dt_name)` here is guaranteed canonical.
        let prefix = self.lookup_prefix(op.dt_ns_code);
        let dt_id = dicts.datatypes.get_or_insert_parts(prefix, op.dt_name);
        // Bulk import path: enforce u8 dt ids for now (imports are allowed to error here).
        // Operationally, the binary format supports widening dt to u16.
        if dt_id > u8::MAX as u32 {
            return Err(CommitCodecError::InvalidOp(format!(
                "import not available: datatype dict overflow (dt_id={dt_id} exceeds u8 max)"
            )));
        }
        let dt_id = dt_id as u16;

        // 5. Encode object -> (ObjKind, ObjKey)
        let (o_kind, o_key) = self
            .resolve_object(&op.o, g_id, p_id, dt_id, dicts)
            .map_err(|e| CommitCodecError::InvalidOp(format!("object resolve: {e}")))?;

        // 6. Language tag
        let lang_id = dicts.languages.get_or_insert(op.lang);

        // 7. List index (convert Option<i32> to u32 with sentinel)
        let i = match op.i {
            Some(idx) if idx >= 0 => idx as u32,
            Some(idx) => {
                return Err(CommitCodecError::InvalidOp(format!(
                    "negative list index {idx} is invalid"
                )));
            }
            None => LIST_INDEX_NONE,
        };

        Ok(RunRecord {
            g_id,
            s_id: SubjectId::from_u64(s_id),
            p_id,
            dt: dt_id,
            o_kind: o_kind.as_u8(),
            op: op.op as u8,
            o_key: o_key.as_u64(),
            t,
            lang_id,
            i,
        })
    }

    // ---- Field resolvers ----

    /// Resolve graph: default graph (ns=0, name="") -> g_id=0.
    /// Named graphs -> g_id = graphs.get_or_insert(full_iri) + 1.
    fn resolve_graph(
        &mut self,
        ns_code: u16,
        name: &str,
        dicts: &mut GlobalDicts,
    ) -> io::Result<u16> {
        if ns_code == 0 && name.is_empty() {
            return Ok(0); // default graph
        }
        let prefix = self.lookup_prefix(ns_code);
        // +1 to reserve 0 for default graph
        let raw = dicts.graphs.get_or_insert_parts(prefix, name) + 1;
        if raw > u16::MAX as u32 {
            return Err(io::Error::other(format!(
                "graph count {raw} exceeds u16::MAX"
            )));
        }
        Ok(raw as u16)
    }

    /// Resolve subject IRI -> global sid64 using streaming xxh3_128.
    fn resolve_subject(
        &mut self,
        ns_code: u16,
        name: &str,
        dicts: &mut GlobalDicts,
    ) -> io::Result<u64> {
        // Access ns_prefixes directly (not via lookup_prefix) so the borrow checker
        // can see that ns_prefixes and hasher are disjoint field borrows.
        let prefix = self
            .ns_prefixes
            .get(&ns_code)
            .map(std::string::String::as_str)
            .unwrap_or("");

        // Streaming hash: feed prefix + name without concatenation
        self.hasher.reset();
        self.hasher.update(prefix.as_bytes());
        self.hasher.update(name.as_bytes());
        let hash = self.hasher.digest128();

        // Closure captures &str refs -- only allocates on miss (novel entry).
        dicts.subjects.get_or_insert_with_hash(hash, ns_code, || {
            let mut s = String::with_capacity(prefix.len() + name.len());
            s.push_str(prefix);
            s.push_str(name);
            s
        })
    }

    /// Resolve predicate IRI -> global p_id.
    fn resolve_predicate(&mut self, ns_code: u16, name: &str, dicts: &mut GlobalDicts) -> u32 {
        let prefix = self.lookup_prefix(ns_code);
        dicts.predicates.get_or_insert_parts(prefix, name)
    }

    /// Encode object value as (ObjKind, ObjKey).
    ///
    /// Numeric routing:
    /// - Integers -> NumInt (full i64 range, order-preserving)
    /// - Finite floats -> NumF64 (inline); see fluree/db-r#142
    /// - NaN / Inf -> REJECT (error)
    /// - Overflow BigInt / BigDecimal -> NumBig (per-predicate equality-only arena)
    fn resolve_object(
        &mut self,
        obj: &RawObject<'_>,
        g_id: GraphId,
        p_id: u32,
        _dt_id: u16,
        dicts: &mut GlobalDicts,
    ) -> Result<(ObjKind, ObjKey), String> {
        match obj {
            RawObject::Long(v) => Ok((ObjKind::NUM_INT, ObjKey::encode_i64(*v))),
            RawObject::Double(v) => {
                // NOTE: Do not optimize integral doubles to NUM_INT here.
                // The decode path uses the property's datatype to select
                // DecodeKind, and F64→I64 mismatch corrupts values. (fluree/db-r#142)
                let key = ObjKey::encode_f64(*v)
                    .map_err(|e| format!("f64 encode for p_id={p_id}: {e}"))?;
                Ok((ObjKind::NUM_F64, key))
            }
            RawObject::Str(s) => {
                let id = dicts
                    .strings
                    .get_or_insert(s)
                    .map_err(|e| format!("string dict write: {e}"))?;
                Ok((ObjKind::LEX_ID, ObjKey::encode_u32_id(id)))
            }
            RawObject::Boolean(b) => Ok((ObjKind::BOOL, ObjKey::encode_bool(*b))),
            RawObject::Ref { ns_code, name } => {
                // Resolve ref IRI -> global sid64 -> REF_ID.
                let prefix = self
                    .ns_prefixes
                    .get(ns_code)
                    .map(std::string::String::as_str)
                    .unwrap_or("");
                self.hasher.reset();
                self.hasher.update(prefix.as_bytes());
                self.hasher.update(name.as_bytes());
                let hash = self.hasher.digest128();

                let sid64 = dicts
                    .subjects
                    .get_or_insert_with_hash(hash, *ns_code, || {
                        let mut s = String::with_capacity(prefix.len() + name.len());
                        s.push_str(prefix);
                        s.push_str(name);
                        s
                    })
                    .map_err(|e| format!("ref resolve: {e}"))?;
                Ok((ObjKind::REF_ID, ObjKey::encode_sid64(sid64)))
            }
            RawObject::DateTimeStr(s) => DateTime::parse(s)
                .map_err(|e| format!("datetime parse: {e}"))
                .map(|dt| {
                    let micros = dt.epoch_micros();
                    (ObjKind::DATE_TIME, ObjKey::encode_datetime(micros))
                }),
            RawObject::DateStr(s) => Date::parse(s)
                .map(|d| (ObjKind::DATE, ObjKey::encode_date(d.days_since_epoch())))
                .map_err(|e| format!("date parse: {e}")),
            RawObject::TimeStr(s) => Time::parse(s)
                .map(|t| {
                    (
                        ObjKind::TIME,
                        ObjKey::encode_time(t.micros_since_midnight()),
                    )
                })
                .map_err(|e| format!("time parse: {e}")),
            RawObject::BigIntStr(s) => {
                // Try to parse as i64 first for NumInt fast path
                if let Ok(v) = s.parse::<i64>() {
                    return Ok((ObjKind::NUM_INT, ObjKey::encode_i64(v)));
                }
                // Parse as BigInt
                match s.parse::<BigInt>() {
                    Ok(bi) => {
                        if let Some(v) = num_traits::ToPrimitive::to_i64(&bi) {
                            return Ok((ObjKind::NUM_INT, ObjKey::encode_i64(v)));
                        }
                        // Overflow BigInt -> NumBig
                        let handle = dicts
                            .numbigs
                            .entry(g_id)
                            .or_default()
                            .entry(p_id)
                            .or_default()
                            .get_or_insert_bigint(&bi);
                        Ok((ObjKind::NUM_BIG, ObjKey::encode_u32_id(handle)))
                    }
                    Err(_) => {
                        // Cannot parse as BigInt -- store as string
                        let id = dicts
                            .strings
                            .get_or_insert(s)
                            .map_err(|e| format!("string dict write: {e}"))?;
                        Ok((ObjKind::LEX_ID, ObjKey::encode_u32_id(id)))
                    }
                }
            }
            RawObject::DecimalStr(s) => {
                // All typed xsd:decimal values route to NumBig by default
                match s.parse::<BigDecimal>() {
                    Ok(bd) => {
                        let handle = dicts
                            .numbigs
                            .entry(g_id)
                            .or_default()
                            .entry(p_id)
                            .or_default()
                            .get_or_insert_bigdec(&bd);
                        Ok((ObjKind::NUM_BIG, ObjKey::encode_u32_id(handle)))
                    }
                    Err(_) => {
                        // Cannot parse as BigDecimal -- store as string
                        let id = dicts
                            .strings
                            .get_or_insert(s)
                            .map_err(|e| format!("string dict write: {e}"))?;
                        Ok((ObjKind::LEX_ID, ObjKey::encode_u32_id(id)))
                    }
                }
            }
            RawObject::JsonStr(s) => {
                let id = dicts
                    .strings
                    .get_or_insert(s)
                    .map_err(|e| format!("string dict write: {e}"))?;
                Ok((ObjKind::JSON_ID, ObjKey::encode_u32_id(id)))
            }
            RawObject::Null => Ok((ObjKind::NULL, ObjKey::ZERO)),
            RawObject::GYearStr(s) => GYear::parse(s)
                .map(|g| (ObjKind::G_YEAR, ObjKey::encode_g_year(g.year())))
                .map_err(|e| format!("gYear parse: {e}")),
            RawObject::GYearMonthStr(s) => GYearMonth::parse(s)
                .map(|g| {
                    (
                        ObjKind::G_YEAR_MONTH,
                        ObjKey::encode_g_year_month(g.year(), g.month()),
                    )
                })
                .map_err(|e| format!("gYearMonth parse: {e}")),
            RawObject::GMonthStr(s) => GMonth::parse(s)
                .map(|g| (ObjKind::G_MONTH, ObjKey::encode_g_month(g.month())))
                .map_err(|e| format!("gMonth parse: {e}")),
            RawObject::GDayStr(s) => GDay::parse(s)
                .map(|g| (ObjKind::G_DAY, ObjKey::encode_g_day(g.day())))
                .map_err(|e| format!("gDay parse: {e}")),
            RawObject::GMonthDayStr(s) => GMonthDay::parse(s)
                .map(|g| {
                    (
                        ObjKind::G_MONTH_DAY,
                        ObjKey::encode_g_month_day(g.month(), g.day()),
                    )
                })
                .map_err(|e| format!("gMonthDay parse: {e}")),
            RawObject::YearMonthDurationStr(s) => YearMonthDuration::parse(s)
                .map(|d| {
                    (
                        ObjKind::YEAR_MONTH_DUR,
                        ObjKey::encode_year_month_dur(d.months()),
                    )
                })
                .map_err(|e| format!("yearMonthDuration parse: {e}")),
            RawObject::DayTimeDurationStr(s) => DayTimeDuration::parse(s)
                .map(|d| {
                    (
                        ObjKind::DAY_TIME_DUR,
                        ObjKey::encode_day_time_dur(d.micros()),
                    )
                })
                .map_err(|e| format!("dayTimeDuration parse: {e}")),
            RawObject::DurationStr(s) => {
                // General xsd:duration has no total order — store as canonical string
                let d = XsdDuration::parse(s).map_err(|e| format!("duration parse: {e}"))?;
                let canonical = d.to_canonical_string();
                let id = dicts
                    .strings
                    .get_or_insert(&canonical)
                    .map_err(|e| format!("string dict write: {e}"))?;
                Ok((ObjKind::LEX_ID, ObjKey::encode_u32_id(id)))
            }
            RawObject::GeoPoint { lat, lng } => {
                let key = ObjKey::encode_geo_point(*lat, *lng)
                    .map_err(|e| format!("geo point encode: {e}"))?;
                Ok((ObjKind::GEO_POINT, key))
            }
            RawObject::Vector(v) => {
                let handle = dicts
                    .vectors
                    .entry(g_id)
                    .or_default()
                    .entry(p_id)
                    .or_default()
                    .insert_f64(v)
                    .map_err(|e| format!("vector arena insert: {e}"))?;
                Ok((ObjKind::VECTOR_ID, ObjKey::encode_u32_id(handle)))
            }
        }
    }

    /// Look up the prefix IRI for a namespace code.
    /// Returns "" if the code is unknown (should not happen with proper delta replay).
    fn lookup_prefix(&self, ns_code: u16) -> &str {
        self.ns_prefixes
            .get(&ns_code)
            .map(std::string::String::as_str)
            .unwrap_or("")
    }
}

impl Default for CommitResolver {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// SharedResolverState + RebuildChunk (commit-based rebuild pipeline)
// ============================================================================

/// Shared state across all rebuild chunks.
///
/// Holds the global dictionaries (predicates, datatypes, graphs, languages)
/// and namespace prefix mappings. Per-chunk local dicts for subjects/strings
/// live in [`RebuildChunk`].
pub struct SharedResolverState {
    /// namespace_code -> prefix IRI (seeded from default_namespace_codes, updated by deltas).
    pub ns_prefixes: HashMap<u16, String>,
    /// Global predicate dict (small cardinality, shared across all chunks).
    pub predicates: super::global_dict::PredicateDict,
    /// Global datatype dict (pre-seeded with reserved entries).
    pub datatypes: super::global_dict::PredicateDict,
    /// Global graph dict (g_id = dict_id + 1; 0 = default graph, 1 = txn-meta).
    pub graphs: super::global_dict::PredicateDict,
    /// Global language tag dict (shared across all chunks — no per-chunk remap needed).
    pub languages: super::global_dict::LanguageTagDict,
    /// Per-graph, per-predicate overflow numeric arenas (BigInt/BigDecimal).
    /// Outer key = g_id, inner key = p_id.
    pub numbigs:
        FxHashMap<GraphId, FxHashMap<u32, fluree_db_binary_index::arena::numbig::NumBigArena>>,
    /// Per-graph, per-predicate vector arenas (packed f32).
    /// Outer key = g_id, inner key = p_id.
    pub vectors:
        FxHashMap<GraphId, FxHashMap<u32, fluree_db_binary_index::arena::vector::VectorArena>>,
    /// Datatype dict ID → ValueTypeTag mapping, populated at insertion time.
    /// Indexed by dt_id (u32). Pre-seeded with reserved entries in `new()`.
    pub dt_tags: Vec<fluree_db_core::value_id::ValueTypeTag>,
    /// Optional spatial geometry collection hook. When set, `on_op()` is
    /// called for every resolved user-data op to collect non-POINT WKT geometries.
    /// Subject IDs in entries are chunk-local and must be remapped after dict merge.
    pub spatial_hook: Option<crate::spatial_hook::SpatialHook>,
    /// Optional fulltext collection hook. When set, `on_op()` is called for
    /// every resolved user-data op to route `@fulltext`-typed string values
    /// and values on configured full-text properties into BM25 arena building.
    pub fulltext_hook: Option<crate::fulltext_hook::FulltextHook>,
    /// Per-indexing-run configured full-text property set (ledger-wide union
    /// of per-graph effective `full_text.properties`). Empty by default; the
    /// `@fulltext`-datatype path still works without any config setup.
    pub fulltext_hook_config: crate::fulltext_hook::FulltextHookConfig,
    /// Optional schema hierarchy extractor hook. When set, `on_flake()` is called
    /// for every `rdfs:subClassOf` / `rdfs:subPropertyOf` user-data op so rebuild
    /// can populate `IndexSchema` in the FIR6 root.
    pub schema_hook: Option<crate::stats::SchemaExtractor>,
}

impl SharedResolverState {
    /// Create a new shared state seeded with default namespace prefixes
    /// and pre-reserved system graphs (txn-meta + config).
    ///
    /// The ledger_id is used to construct the ledger-scoped txn-meta IRI
    /// (`urn:fluree:{ledger_id}#txn-meta`).
    pub fn new_for_ledger(ledger_id: &str) -> Self {
        use fluree_db_core::value_id::ValueTypeTag;

        let mut graphs = super::global_dict::PredicateDict::new();
        // Reserve system graph IDs in stable order:
        // - dict_id=0 → g_id=1 txn-meta
        // - dict_id=1 → g_id=2 config
        let txn_meta_iri = fluree_db_core::graph_registry::txn_meta_graph_iri(ledger_id);
        let config_iri = fluree_db_core::graph_registry::config_graph_iri(ledger_id);
        graphs.get_or_insert(&txn_meta_iri);
        graphs.get_or_insert(&config_iri);

        let datatypes = super::global_dict::new_datatype_dict();

        // Pre-populate dt_tags for the 14 reserved datatype entries.
        // Must match the insertion order in new_datatype_dict().
        let dt_tags: Vec<ValueTypeTag> = (0..datatypes.len())
            .map(|id| {
                fluree_db_core::DatatypeDictId(id as u16)
                    .to_value_type_tag()
                    .unwrap_or(ValueTypeTag::UNKNOWN)
            })
            .collect();

        Self {
            ns_prefixes: fluree_db_core::default_namespace_codes(),
            predicates: super::global_dict::PredicateDict::new(),
            datatypes,
            graphs,
            languages: super::global_dict::LanguageTagDict::new(),
            numbigs: FxHashMap::default(),
            vectors: FxHashMap::default(),
            dt_tags,
            spatial_hook: None,
            fulltext_hook: None,
            fulltext_hook_config: crate::fulltext_hook::FulltextHookConfig::default(),
            schema_hook: None,
        }
    }

    /// Reconstruct shared resolver state from an existing index root.
    ///
    /// Seeds all global dicts (predicates, graphs, datatypes, languages)
    /// from the root's inline vectors using `from_ordered_vec()` for exact
    /// ID stability. Validates critical invariants:
    /// - First 14 datatypes match `new_datatype_dict()` reserved order
    /// - `graph_iris[0]` is the txn-meta graph IRI
    pub fn from_index_root(
        root: &fluree_db_binary_index::format::index_root::IndexRoot,
    ) -> Result<Self, ResolverError> {
        use fluree_db_core::value_id::ValueTypeTag;
        use std::sync::Arc;

        let ns_prefixes: HashMap<u16, String> = root
            .namespace_codes
            .iter()
            .map(|(&code, prefix)| (code, prefix.clone()))
            .collect();

        let pred_iris: Vec<Arc<str>> = root
            .predicate_sids
            .iter()
            .map(|(ns_code, suffix)| {
                let prefix = ns_prefixes
                    .get(ns_code)
                    .map(std::string::String::as_str)
                    .unwrap_or("");
                let mut iri = String::with_capacity(prefix.len() + suffix.len());
                iri.push_str(prefix);
                iri.push_str(suffix);
                Arc::from(iri.as_str())
            })
            .collect();
        let predicates = super::global_dict::PredicateDict::from_ordered_iris(pred_iris);

        let expected_txn_meta = fluree_db_core::graph_registry::txn_meta_graph_iri(&root.ledger_id);
        if root.graph_iris.is_empty() || root.graph_iris[0] != expected_txn_meta {
            return Err(ResolverError::Resolve(format!(
                "graph_iris[0] must be txn-meta IRI '{}', got: {:?}",
                expected_txn_meta,
                root.graph_iris.first()
            )));
        }
        // Upgrade legacy roots that only contain txn-meta by inserting the config graph IRI
        // into slot 1 (g_id=2). This aligns indexed graph IDs with in-memory graph ID
        // allocation (`GraphRegistry::apply_delta` starts user graphs at g_id=3).
        //
        // Safe because legacy roots with `graph_iris.len() == 1` cannot have any user
        // named graphs yet — inserting config does not shift existing user graphs.
        let mut graph_iris: Vec<Arc<str>> = root
            .graph_iris
            .iter()
            .map(|s| Arc::from(s.as_str()))
            .collect();
        if graph_iris.len() == 1 {
            let config_iri = fluree_db_core::graph_registry::config_graph_iri(&root.ledger_id);
            graph_iris.push(Arc::from(config_iri.as_str()));
        }
        let graphs = super::global_dict::PredicateDict::from_ordered_iris(graph_iris);

        let reference = super::global_dict::new_datatype_dict();
        if root.datatype_iris.len() < 14 {
            return Err(ResolverError::Resolve(format!(
                "datatype_iris has {} entries, expected at least 14 reserved",
                root.datatype_iris.len()
            )));
        }
        for i in 0..14u32 {
            let expected = reference
                .resolve(i)
                .expect("reserved datatype missing from reference");
            if root.datatype_iris[i as usize] != expected {
                return Err(ResolverError::Resolve(format!(
                    "datatype_iris[{}] mismatch: expected '{}', got '{}'",
                    i, expected, root.datatype_iris[i as usize]
                )));
            }
        }
        let dt_iris: Vec<Arc<str>> = root
            .datatype_iris
            .iter()
            .map(|s| Arc::from(s.as_str()))
            .collect();
        let datatypes = super::global_dict::PredicateDict::from_ordered_iris(dt_iris);

        let lang_tags: Vec<Arc<str>> = root
            .language_tags
            .iter()
            .map(|s| Arc::from(s.as_str()))
            .collect();
        let languages = super::global_dict::LanguageTagDict::from_ordered_tags(lang_tags);

        let prefix_to_code: Vec<(&str, u16)> = ns_prefixes
            .iter()
            .map(|(&code, prefix)| (prefix.as_str(), code))
            .collect();

        let mut dt_tags = Vec::with_capacity(root.datatype_iris.len());
        for (i, iri) in root.datatype_iris.iter().enumerate() {
            if i < 14 {
                let tag = fluree_db_core::DatatypeDictId(i as u16)
                    .to_value_type_tag()
                    .unwrap_or(ValueTypeTag::UNKNOWN);
                dt_tags.push(tag);
            } else {
                let tag = split_iri_to_value_type_tag(iri, &prefix_to_code);
                dt_tags.push(tag);
            }
        }

        Ok(Self {
            ns_prefixes,
            predicates,
            datatypes,
            graphs,
            languages,
            numbigs: FxHashMap::default(),
            vectors: FxHashMap::default(),
            dt_tags,
            spatial_hook: None,
            fulltext_hook: None,
            fulltext_hook_config: crate::fulltext_hook::FulltextHookConfig::default(),
            schema_hook: None,
        })
    }

    /// Insert or look up a datatype, recording its ValueTypeTag deterministically.
    ///
    /// The caller is responsible for passing a canonical `(ns_code, name)`
    /// pair. V3 legacy canonicalization is applied at v3 raw-op iteration
    /// time (see `CommitOps::for_each_op`), so every caller in the rebuild
    /// pipeline hands this function a clean pair regardless of on-disk
    /// commit format.
    fn resolve_datatype(&mut self, ns_code: u16, name: &str) -> u32 {
        let prefix = self
            .ns_prefixes
            .get(&ns_code)
            .map(std::string::String::as_str)
            .unwrap_or("");
        let dt_id = self.datatypes.get_or_insert_parts(prefix, name);
        // Grow dt_tags if this is a new entry.
        if dt_id as usize >= self.dt_tags.len() {
            let tag = fluree_db_core::value_id::ValueTypeTag::from_ns_name(ns_code, name);
            self.dt_tags.resize(dt_id as usize + 1, tag);
        }
        dt_id
    }

    /// Apply a commit's namespace delta to update prefix mappings.
    pub fn apply_namespace_delta(&mut self, delta: &HashMap<u16, String>) {
        for (&code, prefix) in delta {
            self.ns_prefixes
                .entry(code)
                .or_insert_with(|| prefix.clone());
        }
    }

    /// Seed [`fulltext_hook_config`](Self::fulltext_hook_config) from the
    /// caller-provided configured full-text properties.
    ///
    /// For each entry:
    /// - Resolve `graph_iri` to a `GraphId`, pre-registering named graphs
    ///   in `self.graphs` so the assigned ID is stable for the rest of the
    ///   run. `None` resolves to `g_id = 0` (default graph).
    /// - Pre-register `property_iri` in `self.predicates` to get a stable
    ///   `p_id`, even if no triple in the current commit window uses it.
    ///
    /// The resulting `(GraphId, p_id)` set is loaded into
    /// `self.fulltext_hook_config` and consulted by
    /// [`FulltextHook::on_op`](crate::fulltext_hook::FulltextHook::on_op)
    /// on every record.
    pub fn configure_fulltext_properties(
        &mut self,
        properties: &[crate::config::ConfiguredFulltextProperty],
    ) {
        use crate::config::ConfiguredFulltextScope;

        let mut config = crate::fulltext_hook::FulltextHookConfig::default();
        // [DIAG] Record (iri → p_id, scope, hook-key) so we can compare
        // against the (g_id, p_id) the resolver produces at commit time.
        // A mismatch here is the likely bug on Solo c3000-04: same IRI
        // resolving to different p_ids in this pre-registration vs the
        // per-commit resolve path. Remove after the bug is diagnosed.
        let mut diag_seeded: Vec<(String, u32, String, Option<u16>)> = Vec::new();
        for entry in properties {
            let p_id = self.predicates.get_or_insert(&entry.property_iri);
            match &entry.scope {
                ConfiguredFulltextScope::AnyGraph => {
                    config.add_any_graph(p_id);
                    diag_seeded.push((
                        entry.property_iri.clone(),
                        p_id,
                        "AnyGraph".to_string(),
                        None,
                    ));
                }
                ConfiguredFulltextScope::DefaultGraph => {
                    // Default graph is always `g_id = 0` — not in the graph dict.
                    config.add_per_graph(0, p_id);
                    diag_seeded.push((
                        entry.property_iri.clone(),
                        p_id,
                        "DefaultGraph".to_string(),
                        Some(0),
                    ));
                }
                ConfiguredFulltextScope::TxnMetaGraph => {
                    // `new_for_ledger` pre-reserves txn-meta at graph-dict slot 0
                    // (→ `g_id = 1`). No need to hit the dict here — the ID is
                    // structurally fixed, and `get_or_insert(sentinel_iri)`
                    // would allocate an unrelated entry for the literal
                    // sentinel string, which is the bug we're avoiding.
                    config.add_per_graph(1, p_id);
                    diag_seeded.push((
                        entry.property_iri.clone(),
                        p_id,
                        "TxnMetaGraph".to_string(),
                        Some(1),
                    ));
                }
                ConfiguredFulltextScope::NamedGraph(iri) => {
                    let raw = self.graphs.get_or_insert(iri) + 1;
                    if raw > u16::MAX as u32 {
                        tracing::warn!(
                            graph_iri = %iri,
                            raw,
                            "configure_fulltext_properties: graph count exceeds u16::MAX — skipping"
                        );
                        continue;
                    }
                    config.add_per_graph(raw as u16, p_id);
                    diag_seeded.push((
                        entry.property_iri.clone(),
                        p_id,
                        format!("NamedGraph({iri})"),
                        Some(raw as u16),
                    ));
                }
            }
        }
        // [DIAG] Remove once the config vs resolver p_id alignment is verified.
        for (iri, p_id, scope, g_id) in &diag_seeded {
            tracing::info!(
                iri = %iri,
                p_id,
                scope = %scope,
                g_id = ?g_id,
                "[DIAG] fulltext hook pre-registered"
            );
        }
        self.fulltext_hook_config = config;
    }

    /// Resolve a single commit's ops into chunk-local RunRecords, appending to
    /// the active chunk. Caller decides when to flush the chunk.
    ///
    /// Subjects and strings use chunk-local dicts; predicates, datatypes, graphs,
    /// and languages use the shared global dicts.
    pub fn resolve_commit_into_chunk(
        &mut self,
        bytes: &[u8],
        commit_hash_hex: &str,
        chunk: &mut RebuildChunk,
    ) -> Result<ResolvedCommit, ResolverError> {
        let commit_size = bytes.len() as u64;
        let commit_ops = load_commit_ops(bytes)?;

        // Apply namespace delta (forward order guarantees correctness).
        self.apply_namespace_delta(&commit_ops.envelope.namespace_delta);

        let t = u32::try_from(commit_ops.t).map_err(|_| {
            ResolverError::Resolve(format!("commit t={} does not fit in u32", commit_ops.t))
        })?;
        let mut asserts = 0u32;
        let mut retracts = 0u32;

        // Resolve user-data ops into chunk-local records.
        commit_ops.for_each_op(|raw_op: RawOp<'_>| {
            // Schema extraction (rebuild only): capture class/property hierarchy
            // directly from commit ops before dict remap.
            if let Some(ref mut schema) = self.schema_hook {
                if raw_op.p_ns_code == fluree_vocab::namespaces::RDFS
                    && (raw_op.p_name == "subClassOf" || raw_op.p_name == "subPropertyOf")
                {
                    if let RawObject::Ref { ns_code, name } = raw_op.o {
                        let flake = fluree_db_core::Flake::new(
                            fluree_db_core::Sid::new(raw_op.s_ns_code, raw_op.s_name),
                            fluree_db_core::Sid::new(raw_op.p_ns_code, raw_op.p_name),
                            fluree_db_core::FlakeValue::Ref(fluree_db_core::Sid::new(
                                ns_code, name,
                            )),
                            fluree_db_core::Sid::new(0, ""),
                            t as i64,
                            raw_op.op,
                            None,
                        );
                        schema.on_flake(&flake);
                    }
                }
            }

            let record = self.resolve_op_chunk(&raw_op, t, chunk)?;

            // Feed raw op to spatial hook (needs raw WKT string + resolved IDs).
            // Note: record.s_id is chunk-local here; subject IDs in spatial entries
            // must be remapped after dict merge (Phase C).
            if let Some(ref mut spatial) = self.spatial_hook {
                spatial.on_op(
                    &raw_op,
                    record.g_id,
                    record.s_id.as_u64(),
                    record.p_id,
                    t as i64,
                );
            }

            // Feed resolved record to fulltext hook — routes `@fulltext`-datatype
            // values (always English) and configured-property values (language
            // from lang_id / default_language) into BM25 arena building.
            // Note: string_id (o_key) is chunk-local here; entries must be remapped
            // to global IDs after dict reconciliation (see incremental_resolve.rs step 7).
            if let Some(ref mut ft) = self.fulltext_hook {
                ft.on_op(crate::fulltext_hook::FulltextOpInput {
                    g_id: record.g_id,
                    p_id: record.p_id,
                    dt_id: record.dt,
                    o_kind: record.o_kind,
                    o_key: record.o_key,
                    lang_id: record.lang_id,
                    t: t as i64,
                    is_assert: record.op != 0,
                    config: &self.fulltext_hook_config,
                });
            }

            chunk.records.push(record);
            chunk.flake_count += 1;
            if record.op != 0 {
                asserts += 1;
            } else {
                retracts += 1;
            }
            Ok(())
        })?;

        // Emit txn-meta records into the same chunk.
        let meta_count = self.emit_txn_meta_chunk(
            commit_hash_hex,
            &commit_ops.envelope,
            commit_size,
            asserts,
            retracts,
            chunk,
        )?;

        Ok(ResolvedCommit {
            total_records: asserts + retracts + meta_count,
            t,
            size: commit_size,
            asserts,
            retracts,
        })
    }

    /// Resolve a single RawOp into a RunRecord using chunk-local subject/string dicts.
    fn resolve_op_chunk(
        &mut self,
        op: &RawOp<'_>,
        t: u32,
        chunk: &mut RebuildChunk,
    ) -> Result<RunRecord, CommitCodecError> {
        // 1. Resolve graph (global)
        let g_id = self
            .resolve_graph(op.g_ns_code, op.g_name)
            .map_err(|e| CommitCodecError::InvalidOp(format!("graph resolve: {e}")))?;

        // 2. Resolve subject (chunk-local)
        let s_id = self.resolve_subject_chunk(op.s_ns_code, op.s_name, chunk);

        // 3. Resolve predicate (global)
        let p_id = self.resolve_predicate(op.p_ns_code, op.p_name);

        // 4. Resolve datatype (global, with ValueTypeTag capture)
        let dt_id = self.resolve_datatype(op.dt_ns_code, op.dt_name);
        if dt_id > u8::MAX as u32 {
            return Err(CommitCodecError::InvalidOp(format!(
                "datatype dict overflow (dt_id={dt_id} exceeds u8 max)"
            )));
        }
        let dt_id = dt_id as u16;

        // 5. Encode object (subjects/strings → chunk-local)
        let (o_kind, o_key) = self
            .resolve_object_chunk(&op.o, g_id, p_id, dt_id, chunk)
            .map_err(|e| CommitCodecError::InvalidOp(format!("object resolve: {e}")))?;

        // 6. Language tag (global)
        let lang_id = self.languages.get_or_insert(op.lang);

        // 7. List index
        let i = match op.i {
            Some(idx) if idx >= 0 => idx as u32,
            Some(idx) => {
                return Err(CommitCodecError::InvalidOp(format!(
                    "negative list index {idx} is invalid"
                )));
            }
            None => LIST_INDEX_NONE,
        };

        Ok(RunRecord {
            g_id,
            s_id: SubjectId::from_u64(s_id),
            p_id,
            dt: dt_id,
            o_kind: o_kind.as_u8(),
            op: op.op as u8,
            o_key: o_key.as_u64(),
            t,
            lang_id,
            i,
        })
    }

    /// Resolve subject to a chunk-local sequential u64 ID.
    fn resolve_subject_chunk(&mut self, ns_code: u16, name: &str, chunk: &mut RebuildChunk) -> u64 {
        chunk.subjects.get_or_insert(ns_code, name.as_bytes())
    }

    /// Resolve graph: default graph (ns=0, name="") -> g_id=0.
    /// Named graphs -> g_id = graphs.get_or_insert(full_iri) + 1.
    fn resolve_graph(&mut self, ns_code: u16, name: &str) -> io::Result<u16> {
        if ns_code == 0 && name.is_empty() {
            return Ok(0); // default graph
        }
        let prefix = self
            .ns_prefixes
            .get(&ns_code)
            .map(std::string::String::as_str)
            .unwrap_or("");
        let raw = self.graphs.get_or_insert_parts(prefix, name) + 1;
        if raw > u16::MAX as u32 {
            return Err(io::Error::other(format!(
                "graph count {raw} exceeds u16::MAX"
            )));
        }
        Ok(raw as u16)
    }

    /// Resolve predicate IRI -> global p_id.
    fn resolve_predicate(&mut self, ns_code: u16, name: &str) -> u32 {
        let prefix = self
            .ns_prefixes
            .get(&ns_code)
            .map(std::string::String::as_str)
            .unwrap_or("");
        self.predicates.get_or_insert_parts(prefix, name)
    }

    /// Encode object value using chunk-local dicts for subjects/strings.
    fn resolve_object_chunk(
        &mut self,
        obj: &RawObject<'_>,
        g_id: GraphId,
        p_id: u32,
        _dt_id: u16,
        chunk: &mut RebuildChunk,
    ) -> Result<(ObjKind, ObjKey), String> {
        match obj {
            RawObject::Long(v) => Ok((ObjKind::NUM_INT, ObjKey::encode_i64(*v))),
            RawObject::Double(v) => {
                // NOTE: Do not optimize integral doubles to NUM_INT here.
                // The decode path uses the property's datatype to select
                // DecodeKind, and F64→I64 mismatch corrupts values. (fluree/db-r#142)
                let key = ObjKey::encode_f64(*v)
                    .map_err(|e| format!("f64 encode for p_id={p_id}: {e}"))?;
                Ok((ObjKind::NUM_F64, key))
            }
            RawObject::Str(s) => {
                let id = chunk.strings.get_or_insert(s.as_bytes());
                Ok((ObjKind::LEX_ID, ObjKey::encode_u32_id(id)))
            }
            RawObject::Boolean(b) => Ok((ObjKind::BOOL, ObjKey::encode_bool(*b))),
            RawObject::Ref { ns_code, name } => {
                // Ref object → chunk-local subject ID
                let sid = chunk.subjects.get_or_insert(*ns_code, name.as_bytes());
                Ok((ObjKind::REF_ID, ObjKey::encode_sid64(sid)))
            }
            RawObject::DateTimeStr(s) => DateTime::parse(s)
                .map_err(|e| format!("datetime parse: {e}"))
                .map(|dt| {
                    let micros = dt.epoch_micros();
                    (ObjKind::DATE_TIME, ObjKey::encode_datetime(micros))
                }),
            RawObject::DateStr(s) => Date::parse(s)
                .map(|d| (ObjKind::DATE, ObjKey::encode_date(d.days_since_epoch())))
                .map_err(|e| format!("date parse: {e}")),
            RawObject::TimeStr(s) => Time::parse(s)
                .map(|t| {
                    (
                        ObjKind::TIME,
                        ObjKey::encode_time(t.micros_since_midnight()),
                    )
                })
                .map_err(|e| format!("time parse: {e}")),
            RawObject::BigIntStr(s) => {
                if let Ok(v) = s.parse::<i64>() {
                    return Ok((ObjKind::NUM_INT, ObjKey::encode_i64(v)));
                }
                match s.parse::<BigInt>() {
                    Ok(bi) => {
                        if let Some(v) = num_traits::ToPrimitive::to_i64(&bi) {
                            return Ok((ObjKind::NUM_INT, ObjKey::encode_i64(v)));
                        }
                        let handle = self
                            .numbigs
                            .entry(g_id)
                            .or_default()
                            .entry(p_id)
                            .or_default()
                            .get_or_insert_bigint(&bi);
                        Ok((ObjKind::NUM_BIG, ObjKey::encode_u32_id(handle)))
                    }
                    Err(_) => {
                        let id = chunk.strings.get_or_insert(s.as_bytes());
                        Ok((ObjKind::LEX_ID, ObjKey::encode_u32_id(id)))
                    }
                }
            }
            RawObject::DecimalStr(s) => match s.parse::<BigDecimal>() {
                Ok(bd) => {
                    let handle = self
                        .numbigs
                        .entry(g_id)
                        .or_default()
                        .entry(p_id)
                        .or_default()
                        .get_or_insert_bigdec(&bd);
                    Ok((ObjKind::NUM_BIG, ObjKey::encode_u32_id(handle)))
                }
                Err(_) => {
                    let id = chunk.strings.get_or_insert(s.as_bytes());
                    Ok((ObjKind::LEX_ID, ObjKey::encode_u32_id(id)))
                }
            },
            RawObject::JsonStr(s) => {
                let id = chunk.strings.get_or_insert(s.as_bytes());
                Ok((ObjKind::JSON_ID, ObjKey::encode_u32_id(id)))
            }
            RawObject::Null => Ok((ObjKind::NULL, ObjKey::ZERO)),
            RawObject::GYearStr(s) => GYear::parse(s)
                .map(|g| (ObjKind::G_YEAR, ObjKey::encode_g_year(g.year())))
                .map_err(|e| format!("gYear parse: {e}")),
            RawObject::GYearMonthStr(s) => GYearMonth::parse(s)
                .map(|g| {
                    (
                        ObjKind::G_YEAR_MONTH,
                        ObjKey::encode_g_year_month(g.year(), g.month()),
                    )
                })
                .map_err(|e| format!("gYearMonth parse: {e}")),
            RawObject::GMonthStr(s) => GMonth::parse(s)
                .map(|g| (ObjKind::G_MONTH, ObjKey::encode_g_month(g.month())))
                .map_err(|e| format!("gMonth parse: {e}")),
            RawObject::GDayStr(s) => GDay::parse(s)
                .map(|g| (ObjKind::G_DAY, ObjKey::encode_g_day(g.day())))
                .map_err(|e| format!("gDay parse: {e}")),
            RawObject::GMonthDayStr(s) => GMonthDay::parse(s)
                .map(|g| {
                    (
                        ObjKind::G_MONTH_DAY,
                        ObjKey::encode_g_month_day(g.month(), g.day()),
                    )
                })
                .map_err(|e| format!("gMonthDay parse: {e}")),
            RawObject::YearMonthDurationStr(s) => YearMonthDuration::parse(s)
                .map(|d| {
                    (
                        ObjKind::YEAR_MONTH_DUR,
                        ObjKey::encode_year_month_dur(d.months()),
                    )
                })
                .map_err(|e| format!("yearMonthDuration parse: {e}")),
            RawObject::DayTimeDurationStr(s) => DayTimeDuration::parse(s)
                .map(|d| {
                    (
                        ObjKind::DAY_TIME_DUR,
                        ObjKey::encode_day_time_dur(d.micros()),
                    )
                })
                .map_err(|e| format!("dayTimeDuration parse: {e}")),
            RawObject::DurationStr(s) => {
                let d = XsdDuration::parse(s).map_err(|e| format!("duration parse: {e}"))?;
                let canonical = d.to_canonical_string();
                let id = chunk.strings.get_or_insert(canonical.as_bytes());
                Ok((ObjKind::LEX_ID, ObjKey::encode_u32_id(id)))
            }
            RawObject::GeoPoint { lat, lng } => {
                let key = ObjKey::encode_geo_point(*lat, *lng)
                    .map_err(|e| format!("geo point encode: {e}"))?;
                Ok((ObjKind::GEO_POINT, key))
            }
            RawObject::Vector(v) => {
                let handle = self
                    .vectors
                    .entry(g_id)
                    .or_default()
                    .entry(p_id)
                    .or_default()
                    .insert_f64(v)
                    .map_err(|e| format!("vector arena insert: {e}"))?;
                Ok((ObjKind::VECTOR_ID, ObjKey::encode_u32_id(handle)))
            }
        }
    }

    /// Emit txn-meta RunRecords into the chunk using chunk-local subject/string dicts.
    fn emit_txn_meta_chunk(
        &mut self,
        commit_hash_hex: &str,
        envelope: &CodecEnvelope,
        commit_size: u64,
        asserts: u32,
        retracts: u32,
        chunk: &mut RebuildChunk,
    ) -> Result<u32, ResolverError> {
        // g_id=1 (pre-reserved in SharedResolverState construction)
        // The txn-meta IRI is always the first graph entry (dict_id=0, g_id=0+1=1).
        let g_id: u16 = 1;

        let t = u32::try_from(envelope.t).map_err(|_| {
            ResolverError::Resolve(format!("commit t={} does not fit in u32", envelope.t))
        })?;

        // Resolve commit subject using chunk-local subject dict.
        let commit_ns_code = fluree_vocab::namespaces::FLUREE_COMMIT;
        let commit_s_id = chunk
            .subjects
            .get_or_insert(commit_ns_code, commit_hash_hex.as_bytes());

        // Resolve predicate p_ids (global)
        let p_address = self.predicates.get_or_insert_parts(fluree::DB, db::ADDRESS);
        let p_time = self.predicates.get_or_insert_parts(fluree::DB, db::TIME);
        let p_previous = self
            .predicates
            .get_or_insert_parts(fluree::DB, db::PREVIOUS);
        let p_t = self.predicates.get_or_insert_parts(fluree::DB, db::T);
        let p_size = self.predicates.get_or_insert_parts(fluree::DB, db::SIZE);
        let p_asserts = self.predicates.get_or_insert_parts(fluree::DB, db::ASSERTS);
        let p_retracts = self
            .predicates
            .get_or_insert_parts(fluree::DB, db::RETRACTS);

        let mut count = 0u32;

        let mut push = |s_id: u64, p_id: u32, o_kind: ObjKind, o_key: ObjKey, dt: u16| {
            let record = RunRecord {
                g_id,
                s_id: SubjectId::from_u64(s_id),
                p_id,
                dt,
                o_kind: o_kind.as_u8(),
                op: 1, // assert
                o_key: o_key.as_u64(),
                t,
                lang_id: 0,
                i: LIST_INDEX_NONE,
            };
            chunk.records.push(record);
            chunk.flake_count += 1;
            count += 1;
        };

        // ledger:address (STRING) — CID hex digest via chunk-local string dict
        let addr_str_id = chunk.strings.get_or_insert(commit_hash_hex.as_bytes());
        push(
            commit_s_id,
            p_address,
            ObjKind::LEX_ID,
            ObjKey::encode_u32_id(addr_str_id),
            DatatypeDictId::STRING.as_u16(),
        );

        // ledger:time (LONG) -- epoch milliseconds
        if let Some(time_str) = &envelope.time {
            if let Some(epoch_ms) = iso_to_epoch_ms(time_str) {
                push(
                    commit_s_id,
                    p_time,
                    ObjKind::NUM_INT,
                    ObjKey::encode_i64(epoch_ms),
                    DatatypeDictId::LONG.as_u16(),
                );
            }
        }

        // ledger:t (INTEGER)
        push(
            commit_s_id,
            p_t,
            ObjKind::NUM_INT,
            ObjKey::encode_i64(t as i64),
            DatatypeDictId::INTEGER.as_u16(),
        );

        // ledger:size (LONG)
        push(
            commit_s_id,
            p_size,
            ObjKind::NUM_INT,
            ObjKey::encode_i64(commit_size as i64),
            DatatypeDictId::LONG.as_u16(),
        );

        // ledger:asserts (INTEGER)
        push(
            commit_s_id,
            p_asserts,
            ObjKind::NUM_INT,
            ObjKey::encode_i64(asserts as i64),
            DatatypeDictId::INTEGER.as_u16(),
        );

        // ledger:retracts (INTEGER)
        push(
            commit_s_id,
            p_retracts,
            ObjKind::NUM_INT,
            ObjKey::encode_i64(retracts as i64),
            DatatypeDictId::INTEGER.as_u16(),
        );

        // ledger:previous (ID) -- ref to parent commit(s) (chunk-local subject)
        for prev_ref in &envelope.previous_refs {
            let prev_digest = prev_ref.id.digest_hex();
            let prev_s_id = chunk.subjects.get_or_insert(
                fluree_vocab::namespaces::FLUREE_COMMIT,
                prev_digest.as_bytes(),
            );
            push(
                commit_s_id,
                p_previous,
                ObjKind::REF_ID,
                ObjKey::encode_sid64(prev_s_id),
                DatatypeDictId::ID.as_u16(),
            );
        }

        // ledger:author (STRING) -- transaction signer DID
        if let Some(txn_sig) = &envelope.txn_signature {
            let p_author = self.predicates.get_or_insert_parts(fluree::DB, db::AUTHOR);
            let author_str_id = chunk.strings.get_or_insert(txn_sig.signer.as_bytes());
            push(
                commit_s_id,
                p_author,
                ObjKind::LEX_ID,
                ObjKey::encode_u32_id(author_str_id),
                DatatypeDictId::STRING.as_u16(),
            );
        }

        // ledger:txn (STRING) -- transaction CID string
        if let Some(txn_id) = &envelope.txn {
            let p_txn = self.predicates.get_or_insert_parts(fluree::DB, db::TXN);
            let txn_str = txn_id.to_string();
            let txn_str_id = chunk.strings.get_or_insert(txn_str.as_bytes());
            push(
                commit_s_id,
                p_txn,
                ObjKind::LEX_ID,
                ObjKey::encode_u32_id(txn_str_id),
                DatatypeDictId::STRING.as_u16(),
            );
        }

        // User-provided txn_meta entries
        for entry in &envelope.txn_meta {
            count += self.emit_txn_meta_entry_chunk(commit_s_id, g_id, t, entry, chunk)?;
        }

        Ok(count)
    }

    /// Emit a single user-provided txn_meta entry into the chunk.
    fn emit_txn_meta_entry_chunk(
        &mut self,
        commit_s_id: u64,
        g_id: u16,
        t: u32,
        entry: &fluree_db_novelty::TxnMetaEntry,
        chunk: &mut RebuildChunk,
    ) -> Result<u32, ResolverError> {
        debug_assert!(
            !RESERVED_PREDICATE_NAMESPACES.contains(&entry.predicate_ns),
            "TxnMetaEntry in reserved namespace {} reached resolver — extract_txn_meta guard bypassed?",
            entry.predicate_ns
        );

        let p_prefix = self
            .ns_prefixes
            .get(&entry.predicate_ns)
            .map(std::string::String::as_str)
            .unwrap_or("");
        let p_id = self
            .predicates
            .get_or_insert_parts(p_prefix, &entry.predicate_name);

        let (o_kind, o_key, dt, lang_id) =
            self.resolve_txn_meta_value_chunk(&entry.value, chunk)?;

        let record = RunRecord {
            g_id,
            s_id: SubjectId::from_u64(commit_s_id),
            p_id,
            dt,
            o_kind: o_kind.as_u8(),
            op: 1,
            o_key: o_key.as_u64(),
            t,
            lang_id,
            i: LIST_INDEX_NONE,
        };
        chunk.records.push(record);
        chunk.flake_count += 1;

        Ok(1)
    }

    /// Resolve a TxnMetaValue to (ObjKind, ObjKey, dt_id, lang_id) using chunk-local dicts.
    fn resolve_txn_meta_value_chunk(
        &mut self,
        value: &fluree_db_novelty::TxnMetaValue,
        chunk: &mut RebuildChunk,
    ) -> Result<(ObjKind, ObjKey, u16, u16), ResolverError> {
        use fluree_db_novelty::TxnMetaValue;

        match value {
            TxnMetaValue::String(s) => {
                let str_id = chunk.strings.get_or_insert(s.as_bytes());
                Ok((
                    ObjKind::LEX_ID,
                    ObjKey::encode_u32_id(str_id),
                    DatatypeDictId::STRING.as_u16(),
                    0,
                ))
            }
            TxnMetaValue::Long(n) => Ok((
                ObjKind::NUM_INT,
                ObjKey::encode_i64(*n),
                DatatypeDictId::LONG.as_u16(),
                0,
            )),
            TxnMetaValue::Double(n) => {
                if !n.is_finite() {
                    return Err(ResolverError::Resolve(
                        "txn_meta does not support non-finite double values".into(),
                    ));
                }
                let key = ObjKey::encode_f64(*n)
                    .map_err(|e| ResolverError::Resolve(format!("txn_meta double: {e}")))?;
                Ok((ObjKind::NUM_F64, key, DatatypeDictId::DOUBLE.as_u16(), 0))
            }
            TxnMetaValue::Boolean(b) => Ok((
                ObjKind::BOOL,
                ObjKey::encode_bool(*b),
                DatatypeDictId::BOOLEAN.as_u16(),
                0,
            )),
            TxnMetaValue::Ref { ns, name } => {
                // Resolve ref IRI → chunk-local subject ID
                let sid = chunk.subjects.get_or_insert(*ns, name.as_bytes());
                Ok((
                    ObjKind::REF_ID,
                    ObjKey::encode_sid64(sid),
                    DatatypeDictId::ID.as_u16(),
                    0,
                ))
            }
            TxnMetaValue::LangString { value, lang } => {
                let str_id = chunk.strings.get_or_insert(value.as_bytes());
                let lang_id = self.languages.get_or_insert(Some(lang.as_str()));
                Ok((
                    ObjKind::LEX_ID,
                    ObjKey::encode_u32_id(str_id),
                    DatatypeDictId::LANG_STRING.as_u16(),
                    lang_id,
                ))
            }
            TxnMetaValue::TypedLiteral {
                value,
                dt_ns,
                dt_name,
            } => {
                let str_id = chunk.strings.get_or_insert(value.as_bytes());
                let dt_id = self.resolve_datatype(*dt_ns, dt_name);
                if dt_id > u8::MAX as u32 {
                    return Err(ResolverError::Resolve(format!(
                        "txn_meta datatype dict overflow (dt_id={dt_id} exceeds u8 max)"
                    )));
                }
                Ok((
                    ObjKind::LEX_ID,
                    ObjKey::encode_u32_id(str_id),
                    dt_id as u16,
                    0,
                ))
            }
        }
    }
}

/// Per-chunk accumulator for the rebuild pipeline.
///
/// Holds chunk-local subject and string dictionaries plus the buffered
/// RunRecords. The caller flushes the chunk (via `sort_remap_and_write_sorted_commit`)
/// when flake_count reaches the chunk budget.
pub struct RebuildChunk {
    /// Chunk-local subject dict: (ns_code, name) → sequential u64.
    pub subjects: super::chunk_dict::ChunkSubjectDict,
    /// Chunk-local string dict: string bytes → sequential u32.
    pub strings: super::chunk_dict::ChunkStringDict,
    /// Buffered RunRecords (with chunk-local subject/string IDs).
    pub records: Vec<RunRecord>,
    /// Running count of flakes (records) in this chunk.
    pub flake_count: u64,
}

impl RebuildChunk {
    pub fn new() -> Self {
        Self {
            subjects: super::chunk_dict::ChunkSubjectDict::new(),
            strings: super::chunk_dict::ChunkStringDict::new(),
            records: Vec::new(),
            flake_count: 0,
        }
    }

    /// Current flake count in this chunk.
    pub fn flake_count(&self) -> u64 {
        self.flake_count
    }

    /// Whether the chunk has no records.
    pub fn is_empty(&self) -> bool {
        self.flake_count == 0
    }
}

impl Default for RebuildChunk {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Error type
// ============================================================================

/// Errors from the resolution pipeline.
#[derive(Debug)]
pub enum ResolverError {
    Codec(CommitCodecError),
    Io(io::Error),
    Resolve(String),
}

impl From<CommitCodecError> for ResolverError {
    fn from(e: CommitCodecError) -> Self {
        Self::Codec(e)
    }
}

impl From<io::Error> for ResolverError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

impl std::fmt::Display for ResolverError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Codec(e) => write!(f, "commit-codec: {e}"),
            Self::Io(e) => write!(f, "I/O: {e}"),
            Self::Resolve(msg) => write!(f, "resolve: {msg}"),
        }
    }
}

impl std::error::Error for ResolverError {}

// ============================================================================
// Helper functions
// ============================================================================

/// Parse ISO-8601 timestamp to epoch milliseconds.
///
/// Returns `None` if parsing fails (caller skips emission rather than
/// poisoning the index with `0`).
fn iso_to_epoch_ms(iso: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(iso)
        .ok()
        .map(|dt| dt.timestamp_millis())
}

/// Split a full IRI against known namespace prefixes and derive its ValueTypeTag.
///
/// Finds the longest matching prefix to extract (ns_code, local_name), then
/// delegates to `ValueTypeTag::from_ns_name()`. Returns `UNKNOWN` if no prefix
/// matches or the local name doesn't map to a known type.
fn split_iri_to_value_type_tag(
    iri: &str,
    prefix_to_code: &[(&str, u16)],
) -> fluree_db_core::value_id::ValueTypeTag {
    let mut best_code = None;
    let mut best_len = 0;
    for &(prefix, code) in prefix_to_code {
        if !prefix.is_empty() && iri.starts_with(prefix) && prefix.len() > best_len {
            best_code = Some(code);
            best_len = prefix.len();
        }
    }
    match best_code {
        Some(code) => fluree_db_core::value_id::ValueTypeTag::from_ns_name(code, &iri[best_len..]),
        None => fluree_db_core::value_id::ValueTypeTag::UNKNOWN,
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::run_index::runs::run_writer::RecordSink;
    use fluree_db_binary_index::format::run_record::RunRecord;
    use fluree_db_core::commit::codec::envelope::{encode_envelope_fields, CodecEnvelope};
    use fluree_db_core::commit::codec::format::{
        self, CommitFooter, CommitHeader, FOOTER_LEN, HEADER_LEN,
    };
    use fluree_db_core::commit::codec::load_commit_ops;
    use fluree_db_core::commit::codec::op_codec::{encode_op, CommitDicts};
    use fluree_db_core::{Flake, FlakeMeta, FlakeValue, Sid};

    /// In-memory V1 record collector for tests.
    ///
    /// Implements `RecordSink` to capture V1 `RunRecord` values emitted by the
    /// resolver without converting to V2 or writing to disk.
    struct RecordCollector {
        records: Vec<RunRecord>,
    }

    impl RecordCollector {
        fn new() -> Self {
            Self {
                records: Vec::new(),
            }
        }
    }

    impl RecordSink for RecordCollector {
        fn push(
            &mut self,
            record: RunRecord,
            _lang_dict: &mut crate::run_index::resolve::global_dict::LanguageTagDict,
        ) -> std::io::Result<()> {
            self.records.push(record);
            Ok(())
        }
    }

    /// Build a minimal commit blob from flakes (reused from raw_reader tests).
    fn build_test_blob(flakes: &[Flake], t: i64) -> Vec<u8> {
        let mut dicts = CommitDicts::new();
        let mut ops_buf = Vec::new();
        for f in flakes {
            encode_op(f, &mut dicts, &mut ops_buf).unwrap();
        }

        let envelope = CodecEnvelope {
            t,
            previous_refs: Vec::new(),
            namespace_delta: HashMap::new(),
            txn: None,
            time: None,
            txn_signature: None,
            txn_meta: Vec::new(),
            graph_delta: HashMap::new(),
            ns_split_mode: None,
        };
        let mut envelope_bytes = Vec::new();
        encode_envelope_fields(&envelope, &mut envelope_bytes).unwrap();

        let dict_bytes: Vec<Vec<u8>> = vec![
            dicts.graph.serialize(),
            dicts.subject.serialize(),
            dicts.predicate.serialize(),
            dicts.datatype.serialize(),
            dicts.object_ref.serialize(),
        ];

        let ops_section_len = ops_buf.len() as u32;
        let envelope_len = envelope_bytes.len() as u32;
        let dict_start = HEADER_LEN + envelope_bytes.len() + ops_buf.len();
        let mut dict_locations = [format::DictLocation::default(); 5];
        let mut offset = dict_start as u64;
        for (i, d) in dict_bytes.iter().enumerate() {
            dict_locations[i] = format::DictLocation {
                offset,
                len: d.len() as u32,
            };
            offset += d.len() as u64;
        }

        let footer = CommitFooter {
            dicts: dict_locations,
            ops_section_len,
        };
        let header = CommitHeader {
            version: format::VERSION,
            flags: 0,
            t,
            op_count: flakes.len() as u32,
            envelope_len,
            sig_block_len: 0,
        };

        // V4: no trailing hash
        let total_len = HEADER_LEN
            + envelope_bytes.len()
            + ops_buf.len()
            + dict_bytes.iter().map(std::vec::Vec::len).sum::<usize>()
            + FOOTER_LEN;
        let mut blob = vec![0u8; total_len];

        let mut pos = 0;
        header.write_to(&mut blob[pos..]);
        pos += HEADER_LEN;
        blob[pos..pos + envelope_bytes.len()].copy_from_slice(&envelope_bytes);
        pos += envelope_bytes.len();
        blob[pos..pos + ops_buf.len()].copy_from_slice(&ops_buf);
        pos += ops_buf.len();
        for d in &dict_bytes {
            blob[pos..pos + d.len()].copy_from_slice(d);
            pos += d.len();
        }
        footer.write_to(&mut blob[pos..]);
        blob
    }

    #[test]
    fn test_resolve_basic_ops() {
        let flakes = vec![
            Flake::new(
                Sid::new(101, "Alice"),
                Sid::new(101, "age"),
                FlakeValue::Long(30),
                Sid::new(2, "integer"),
                1,
                true,
                None,
            ),
            Flake::new(
                Sid::new(101, "Alice"),
                Sid::new(101, "name"),
                FlakeValue::String("Alice".into()),
                Sid::new(2, "string"),
                1,
                true,
                None,
            ),
            Flake::new(
                Sid::new(101, "Bob"),
                Sid::new(101, "age"),
                FlakeValue::Long(25),
                Sid::new(2, "integer"),
                1,
                true,
                None,
            ),
        ];

        let blob = build_test_blob(&flakes, 1);
        let commit_ops = load_commit_ops(&blob).unwrap();

        let mut dicts = GlobalDicts::new_memory("test:main");
        let mut resolver = CommitResolver::new();

        // Add user namespace prefix (code 101)
        resolver
            .ns_prefixes
            .insert(101, "http://example.org/".to_string());

        let mut collector = RecordCollector::new();

        let (asserts, retracts) = resolver
            .resolve_commit_ops(&commit_ops, &mut dicts, &mut collector)
            .unwrap();
        assert_eq!(asserts + retracts, 3);

        // Check dictionary state
        assert_eq!(dicts.subjects.len(), 2); // Alice, Bob
        assert_eq!(dicts.predicates.len(), 2); // age, name
        assert_eq!(dicts.strings.len(), 1); // "Alice" (the string value)

        // Verify records collected
        assert_eq!(collector.records.len(), 3);
    }

    #[test]
    fn test_resolve_ref_and_dedup() {
        let flakes = vec![
            // Alice knows Bob (Ref)
            Flake::new(
                Sid::new(101, "Alice"),
                Sid::new(101, "knows"),
                FlakeValue::Ref(Sid::new(101, "Bob")),
                Sid::new(1, "id"),
                1,
                true,
                None,
            ),
            // Bob's age
            Flake::new(
                Sid::new(101, "Bob"),
                Sid::new(101, "age"),
                FlakeValue::Long(25),
                Sid::new(2, "integer"),
                1,
                true,
                None,
            ),
        ];

        let blob = build_test_blob(&flakes, 1);
        let commit_ops = load_commit_ops(&blob).unwrap();

        let mut dicts = GlobalDicts::new_memory("test:main");
        let mut resolver = CommitResolver::new();
        resolver
            .ns_prefixes
            .insert(101, "http://example.org/".to_string());

        let mut collector = RecordCollector::new();

        resolver
            .resolve_commit_ops(&commit_ops, &mut dicts, &mut collector)
            .unwrap();

        assert_eq!(dicts.subjects.len(), 2); // Alice, Bob
        assert_eq!(dicts.predicates.len(), 2); // knows, age
    }

    #[test]
    fn test_resolve_datetime() {
        let flakes = vec![Flake::new(
            Sid::new(101, "x"),
            Sid::new(101, "created"),
            FlakeValue::DateTime(Box::new(DateTime::parse("2024-01-15T10:30:00Z").unwrap())),
            Sid::new(2, "dateTime"),
            1,
            true,
            None,
        )];

        let blob = build_test_blob(&flakes, 1);
        let commit_ops = load_commit_ops(&blob).unwrap();

        let mut dicts = GlobalDicts::new_memory("test:main");
        let mut resolver = CommitResolver::new();
        resolver
            .ns_prefixes
            .insert(101, "http://example.org/".to_string());

        let mut collector = RecordCollector::new();

        let (asserts, retracts) = resolver
            .resolve_commit_ops(&commit_ops, &mut dicts, &mut collector)
            .unwrap();
        assert_eq!(asserts + retracts, 1);

        assert_eq!(collector.records.len(), 1);
        // Verify the ObjKind is DATE_TIME (0x9)
        assert_eq!(collector.records[0].o_kind, ObjKind::DATE_TIME.as_u8());
        // Verify dt is DATE_TIME
        assert_eq!(collector.records[0].dt, DatatypeDictId::DATE_TIME.as_u16());
    }

    #[test]
    fn test_resolve_boolean_and_null() {
        let flakes = vec![
            Flake::new(
                Sid::new(101, "x"),
                Sid::new(101, "active"),
                FlakeValue::Boolean(true),
                Sid::new(2, "boolean"),
                1,
                true,
                None,
            ),
            Flake::new(
                Sid::new(101, "x"),
                Sid::new(101, "deleted"),
                FlakeValue::Null,
                Sid::new(2, "string"),
                1,
                true,
                None,
            ),
        ];

        let blob = build_test_blob(&flakes, 1);
        let commit_ops = load_commit_ops(&blob).unwrap();

        let mut dicts = GlobalDicts::new_memory("test:main");
        let mut resolver = CommitResolver::new();
        resolver
            .ns_prefixes
            .insert(101, "http://example.org/".to_string());

        let mut collector = RecordCollector::new();

        resolver
            .resolve_commit_ops(&commit_ops, &mut dicts, &mut collector)
            .unwrap();

        assert_eq!(collector.records[0].o_kind, ObjKind::BOOL.as_u8());
        assert_eq!(
            collector.records[0].o_key,
            ObjKey::encode_bool(true).as_u64()
        );
        assert_eq!(collector.records[1].o_kind, ObjKind::NULL.as_u8());
        assert_eq!(collector.records[1].o_key, 0);
    }

    #[test]
    fn test_resolve_with_lang_tag() {
        let flakes = vec![
            Flake::new(
                Sid::new(101, "x"),
                Sid::new(101, "label"),
                FlakeValue::String("hello".into()),
                Sid::new(3, "langString"),
                1,
                true,
                Some(FlakeMeta::with_lang("en")),
            ),
            Flake::new(
                Sid::new(101, "x"),
                Sid::new(101, "label"),
                FlakeValue::String("bonjour".into()),
                Sid::new(3, "langString"),
                1,
                true,
                Some(FlakeMeta::with_lang("fr")),
            ),
        ];

        let blob = build_test_blob(&flakes, 1);
        let commit_ops = load_commit_ops(&blob).unwrap();

        let mut dicts = GlobalDicts::new_memory("test:main");
        let mut resolver = CommitResolver::new();
        resolver
            .ns_prefixes
            .insert(101, "http://example.org/".to_string());

        let mut collector = RecordCollector::new();

        resolver
            .resolve_commit_ops(&commit_ops, &mut dicts, &mut collector)
            .unwrap();

        // Should have 2 language tags
        assert_eq!(dicts.languages.len(), 2);
        assert_eq!(dicts.languages.resolve(1), Some("en"));
        assert_eq!(dicts.languages.resolve(2), Some("fr"));
    }

    // ---- Txn-meta tests ----

    #[test]
    fn test_iso_to_epoch_ms() {
        let ms = super::iso_to_epoch_ms("2025-01-20T12:00:00Z");
        assert!(ms.is_some());
        let ms = ms.unwrap();
        assert!(ms > 1_737_000_000_000);
        assert!(ms < 1_738_000_000_000);

        assert_eq!(super::iso_to_epoch_ms("not-a-date"), None);
    }

    #[test]
    fn test_emit_txn_meta() {
        use fluree_db_core::commit::codec::envelope::CodecEnvelope;
        use fluree_db_core::{ContentId, ContentKind};
        use fluree_db_novelty::CommitRef;

        let mut dicts = GlobalDicts::new_memory("test:main");
        let mut resolver = CommitResolver::new();

        let mut collector = RecordCollector::new();

        let hex = "abc123def456abc123def456abc123def456abc123def456abc123def456abcd";
        let prev_hex = "0000000000000000000000000000000000000000000000000000000000000000";
        let prev_commit_id = format!("fluree:commit:sha256:{prev_hex}");

        let envelope = CodecEnvelope {
            t: 42,
            previous_refs: vec![CommitRef::new(ContentId::new(
                ContentKind::Commit,
                prev_commit_id.as_bytes(),
            ))],
            namespace_delta: HashMap::new(),
            txn: None,
            time: Some("2025-06-15T12:00:00Z".into()),
            txn_signature: None,
            txn_meta: Vec::new(),
            graph_delta: HashMap::new(),
            ns_split_mode: None,
        };

        let count = resolver
            .emit_txn_meta(hex, &envelope, 1024, 8, 2, &mut dicts, &mut collector)
            .unwrap();

        // 7 records on commit subject: address, time, t, size, asserts, retracts, previous
        assert_eq!(count, 7);

        // Verify g_id=1 reservation: txn-meta IRI is the first graph entry (dict_id=0)
        let txn_meta = fluree_db_core::graph_registry::txn_meta_graph_iri("test:main");
        let dict_id = dicts
            .graphs
            .get(&txn_meta)
            .expect("txn-meta must be in graph dict");
        assert_eq!(dict_id + 1, 1, "txn-meta g_id must be 1");

        // Verify subjects created
        assert!(dicts.subjects.len() >= 2);

        // Verify records collected
        let records = &collector.records;
        assert_eq!(records.len(), 7);

        for rec in records {
            assert_eq!(rec.op, 1, "txn-meta records must be asserts");
        }

        // Verify predicates were registered
        let p_address = dicts.predicates.get("https://ns.flur.ee/db#address");
        let p_time = dicts.predicates.get("https://ns.flur.ee/db#time");
        let p_t = dicts.predicates.get("https://ns.flur.ee/db#t");
        let p_previous = dicts.predicates.get("https://ns.flur.ee/db#previous");
        assert!(p_address.is_some(), "f:address predicate missing");
        assert!(p_time.is_some(), "f:time predicate missing");
        assert!(p_t.is_some(), "f:t predicate missing");
        assert!(p_previous.is_some(), "f:previous predicate missing");

        // Find the time record and verify it's NUM_INT with DatatypeDictId::LONG
        let time_pid = p_time.unwrap();
        let time_rec = records.iter().find(|r| r.p_id == time_pid).unwrap();
        assert_eq!(
            time_rec.dt,
            DatatypeDictId::LONG.as_u16(),
            "ledger:time must be DatatypeDictId::LONG"
        );
        assert_eq!(
            time_rec.o_kind,
            ObjKind::NUM_INT.as_u8(),
            "ledger:time must be NUM_INT"
        );
        // Verify epoch ms is reasonable (2025)
        let epoch_ms = ObjKey::from_u64(time_rec.o_key).decode_i64();
        assert!(epoch_ms > 1_718_000_000_000, "epoch ms should be in 2025");

        // Find the t record
        let t_pid = p_t.unwrap();
        let t_rec = records.iter().find(|r| r.p_id == t_pid).unwrap();
        assert_eq!(
            t_rec.dt,
            DatatypeDictId::INTEGER.as_u16(),
            "ledger:t must be DatatypeDictId::INTEGER"
        );
        assert_eq!(
            t_rec.o_kind,
            ObjKind::NUM_INT.as_u8(),
            "ledger:t must be NUM_INT"
        );

        // Find the previous record and verify it's REF_ID with DatatypeDictId::ID
        let prev_pid = p_previous.unwrap();
        let prev_rec = records.iter().find(|r| r.p_id == prev_pid).unwrap();
        assert_eq!(
            prev_rec.dt,
            DatatypeDictId::ID.as_u16(),
            "ledger:previous must be DatatypeDictId::ID"
        );
        assert_eq!(
            prev_rec.o_kind,
            ObjKind::REF_ID.as_u8(),
            "ledger:previous must be REF_ID"
        );
    }

    #[test]
    fn test_emit_txn_meta_minimal() {
        use fluree_db_core::commit::codec::envelope::CodecEnvelope;

        let mut dicts = GlobalDicts::new_memory("test:main");
        let mut resolver = CommitResolver::new();

        let mut collector = RecordCollector::new();

        let hex = "abc123def456abc123def456abc123def456abc123def456abc123def456abcd";

        let envelope = CodecEnvelope {
            t: 1,
            previous_refs: Vec::new(),
            namespace_delta: HashMap::new(),
            txn: None,
            time: None,
            txn_signature: None,
            txn_meta: Vec::new(),
            graph_delta: HashMap::new(),
            ns_split_mode: None,
        };

        let count = resolver
            .emit_txn_meta(hex, &envelope, 512, 4, 1, &mut dicts, &mut collector)
            .unwrap();

        // 5 records: address, t, size, asserts, retracts (no time, no previous)
        assert_eq!(count, 5);
    }

    #[test]
    fn test_emit_txn_meta_user_entries() {
        use fluree_db_core::commit::codec::envelope::CodecEnvelope;
        use fluree_db_novelty::{TxnMetaEntry, TxnMetaValue};

        let mut dicts = GlobalDicts::new_memory("test:main");
        let mut resolver = CommitResolver::new();
        // Add user namespace for txn_meta predicates
        resolver
            .ns_prefixes
            .insert(100, "http://example.org/".to_string());
        resolver
            .ns_prefixes
            .insert(101, "http://refs.example.org/".to_string());

        let mut collector = RecordCollector::new();

        let hex = "abc123def456abc123def456abc123def456abc123def456abc123def456abcd";

        let envelope = CodecEnvelope {
            t: 5,
            previous_refs: Vec::new(),
            namespace_delta: HashMap::new(),
            txn: None,
            time: None,
            txn_signature: None,
            txn_meta: vec![
                TxnMetaEntry::new(100, "jobId", TxnMetaValue::String("job-123".into())),
                TxnMetaEntry::new(100, "priority", TxnMetaValue::Long(42)),
                TxnMetaEntry::new(100, "enabled", TxnMetaValue::Boolean(true)),
                TxnMetaEntry::new(100, "score", TxnMetaValue::Double(1.23)),
                TxnMetaEntry::new(
                    100,
                    "assignee",
                    TxnMetaValue::Ref {
                        ns: 101,
                        name: "alice".into(),
                    },
                ),
                TxnMetaEntry::new(
                    100,
                    "description",
                    TxnMetaValue::LangString {
                        value: "bonjour".into(),
                        lang: "fr".into(),
                    },
                ),
                TxnMetaEntry::new(
                    100,
                    "createdAt",
                    TxnMetaValue::TypedLiteral {
                        value: "2025-06-15".into(),
                        dt_ns: 2,
                        dt_name: "date".into(),
                    },
                ),
            ],
            graph_delta: HashMap::new(),
            ns_split_mode: None,
        };

        let count = resolver
            .emit_txn_meta(hex, &envelope, 2048, 15, 5, &mut dicts, &mut collector)
            .unwrap();

        // 5 built-in records (address, t, size, asserts, retracts) + 7 user entries = 12
        assert_eq!(count, 12);

        let records = &collector.records;
        assert_eq!(records.len(), 12);

        for rec in records {
            assert_eq!(rec.op, 1, "txn-meta records must be asserts");
        }

        // Verify user predicates were registered
        let p_job_id = dicts.predicates.get("http://example.org/jobId");
        let p_priority = dicts.predicates.get("http://example.org/priority");
        let p_enabled = dicts.predicates.get("http://example.org/enabled");
        let p_score = dicts.predicates.get("http://example.org/score");
        let p_assignee = dicts.predicates.get("http://example.org/assignee");
        let p_description = dicts.predicates.get("http://example.org/description");
        let p_created_at = dicts.predicates.get("http://example.org/createdAt");

        assert!(p_job_id.is_some(), "jobId predicate missing");
        assert!(p_priority.is_some(), "priority predicate missing");
        assert!(p_enabled.is_some(), "enabled predicate missing");
        assert!(p_score.is_some(), "score predicate missing");
        assert!(p_assignee.is_some(), "assignee predicate missing");
        assert!(p_description.is_some(), "description predicate missing");
        assert!(p_created_at.is_some(), "createdAt predicate missing");

        // Verify priority record is NUM_INT with LONG datatype
        let priority_pid = p_priority.unwrap();
        let priority_rec = records.iter().find(|r| r.p_id == priority_pid).unwrap();
        assert_eq!(priority_rec.o_kind, ObjKind::NUM_INT.as_u8());
        assert_eq!(priority_rec.dt, DatatypeDictId::LONG.as_u16());
        assert_eq!(ObjKey::from_u64(priority_rec.o_key).decode_i64(), 42);

        // Verify enabled record is BOOL with BOOLEAN datatype
        let enabled_pid = p_enabled.unwrap();
        let enabled_rec = records.iter().find(|r| r.p_id == enabled_pid).unwrap();
        assert_eq!(enabled_rec.o_kind, ObjKind::BOOL.as_u8());
        assert_eq!(enabled_rec.dt, DatatypeDictId::BOOLEAN.as_u16());

        // Verify score record is NUM_F64 with DOUBLE datatype (no integer fast path for txn-meta)
        let score_pid = p_score.unwrap();
        let score_rec = records.iter().find(|r| r.p_id == score_pid).unwrap();
        assert_eq!(
            score_rec.o_kind,
            ObjKind::NUM_F64.as_u8(),
            "score must be NUM_F64"
        );
        assert_eq!(
            score_rec.dt,
            DatatypeDictId::DOUBLE.as_u16(),
            "score must be DOUBLE datatype"
        );

        // Verify assignee record is REF_ID with ID datatype
        let assignee_pid = p_assignee.unwrap();
        let assignee_rec = records.iter().find(|r| r.p_id == assignee_pid).unwrap();
        assert_eq!(assignee_rec.o_kind, ObjKind::REF_ID.as_u8());
        assert_eq!(assignee_rec.dt, DatatypeDictId::ID.as_u16());

        // Verify description has language tag
        let desc_pid = p_description.unwrap();
        let desc_rec = records.iter().find(|r| r.p_id == desc_pid).unwrap();
        assert_eq!(desc_rec.dt, DatatypeDictId::LANG_STRING.as_u16());
        assert!(desc_rec.lang_id > 0, "description should have lang_id");
        // Use the language dict from the resolver's dicts
        assert_eq!(dicts.languages.resolve(desc_rec.lang_id), Some("fr"));
    }

    #[test]
    fn test_global_dicts_reserves_g_id_1() {
        let dicts = GlobalDicts::new_memory("test:main");
        let txn_meta = fluree_db_core::graph_registry::txn_meta_graph_iri("test:main");
        let g_id = dicts.graphs.get(&txn_meta);
        assert_eq!(
            g_id,
            Some(0),
            "txn-meta graph must be first entry (dict id=0, g_id=0+1=1)"
        );
    }
}
