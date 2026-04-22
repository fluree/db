//! Index root descriptor (`FIR6`) for the FLI3 columnar index format.
//!
//! Contains:
//! - `o_type_table`: maps OType → decode kind, datatype IRI, dict/arena family
//! - Routing using FBR3 branch / FLI3 leaf CIDs
//! - Inline small dictionaries (graphs, datatypes, languages)
//! - Dictionary tree CID references (subjects, strings)
//! - Optional sections: stats, schema, prev_index, garbage, sketch

use fluree_db_core::o_type::{DecodeKind, OType};
use fluree_db_core::ContentId;
use fluree_db_core::GraphId;
use std::collections::BTreeMap;
use std::io;

use super::branch::LeafEntry;
use super::run_record::RunSortOrder;
use super::run_record_v2::{RunRecordV2, RECORD_V2_WIRE_SIZE};
use super::stats_wire;
use super::wire_helpers::{
    ensure_bytes, io_err, read_cid, read_dict_pack_refs, read_dict_tree_refs, read_i64_at,
    read_string, read_string_array, read_u16_at, read_u32_at, read_u64_at, read_u8_at, write_cid,
    write_dict_pack_refs, write_dict_tree_refs, write_str, write_string_array, BinaryGarbageRef,
    BinaryPrevIndexRef, DictRefs, FulltextArenaRef, GraphArenaRefs, SpatialArenaRef, VectorDictRef,
};
use fluree_db_core::index_schema::IndexSchema;
use fluree_db_core::index_stats::IndexStats;
use fluree_vocab::{fluree, geo, rdf, xsd};

// ============================================================================
// OType table entry
// ============================================================================

/// Entry in the `o_type` lookup table stored in the index root.
///
/// Maps an `OType` value to its decode routing information. For built-in
/// types this is derivable from the constant tables in `o_type.rs`, but
/// for customer-defined and dynamic types it must be looked up here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OTypeTableEntry {
    /// The `o_type` value (u16).
    pub o_type: u16,
    /// Decode routing kind.
    pub decode_kind: DecodeKind,
    /// Datatype IRI string (full IRI). Present for most literal types; absent for
    /// ref-like types where the object is a node reference (IRI/bnode).
    pub datatype_iri: Option<String>,
    /// Which dictionary/arena family `o_key` indexes into.
    /// Absent for embedded types (o_key is the value itself).
    pub dict_family: Option<DictFamily>,
}

/// Dictionary/arena family that `o_key` indexes into for dict-backed types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum DictFamily {
    /// String dictionary (xsd:string, xsd:anyURI, langString, customer types, etc.)
    StringDict = 0,
    /// Subject dictionary (IRI references)
    SubjectDict = 1,
    /// JSON arena (per-predicate)
    JsonArena = 2,
    /// Vector arena (per-predicate)
    VectorArena = 3,
    /// NumBig arena (per-predicate)
    NumBigArena = 4,
    /// Spatial arena (per-predicate)
    SpatialArena = 5,
}

impl DictFamily {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::StringDict),
            1 => Some(Self::SubjectDict),
            2 => Some(Self::JsonArena),
            3 => Some(Self::VectorArena),
            4 => Some(Self::NumBigArena),
            5 => Some(Self::SpatialArena),
            _ => None,
        }
    }
}

// ============================================================================
// V3 routing types
// ============================================================================

/// Inline leaf routing for a single sort order in the default graph (g_id=0).
///
/// Inline order routing with `LeafEntry` key types.
/// Avoids a CAS round-trip for the default graph at load time.
#[derive(Debug, Clone)]
pub struct DefaultGraphOrder {
    pub order: RunSortOrder,
    pub leaves: Vec<LeafEntry>,
}

/// Named graph routing for V3: branch CIDs per sort order.
#[derive(Debug, Clone)]
pub struct NamedGraphRouting {
    pub g_id: GraphId,
    /// `(order, branch_cid)` pairs.
    pub orders: Vec<(RunSortOrder, ContentId)>,
}

// ============================================================================
// IndexRoot
// ============================================================================

/// Magic bytes for the index root.
pub const ROOT_V6_MAGIC: &[u8; 4] = b"FIR6";

/// Format version.
///
/// Version 2 adds `lang_id: u16` to each `FulltextArenaRef` so arenas can
/// be keyed by `(g_id, p_id, lang_id)` for multi-language full-text indexing.
/// Pre-v2 roots are refused outright — operators upgrading must run a full
/// reindex before queries resume.
pub const ROOT_V6_VERSION: u8 = 2;

/// Binary index root (`FIR6`).
///
/// Contains all sections needed to load an index: dict refs, arena refs,
/// per-graph routing, o_type table, stats, schema, GC chain.
#[derive(Debug, Clone)]
pub struct IndexRoot {
    // ── Identity ───────────────────────────────────────────────────
    pub ledger_id: String,
    pub index_t: i64,
    pub base_t: i64,
    pub subject_id_encoding: fluree_db_core::SubjectIdEncoding,

    // ── Namespace / predicate metadata ─────────────────────────────
    pub namespace_codes: BTreeMap<u16, String>,
    pub predicate_sids: Vec<(u16, String)>,
    /// Ledger-fixed split mode for canonical IRI encoding.
    /// Persisted here so it survives independent of the commit chain.
    pub ns_split_mode: fluree_db_core::ns_encoding::NsSplitMode,

    // ── Inline small dictionaries ──────────────────────────────────
    pub graph_iris: Vec<String>,
    /// Datatype IRIs: index 0..RESERVED_COUNT are well-known, ≥RESERVED_COUNT are custom.
    pub datatype_iris: Vec<String>,
    /// Language tags: `lang_id → BCP 47 tag string` (index = lang_id).
    pub language_tags: Vec<String>,

    // ── Dict refs (CID trees) ──────────────────────────────────────────
    pub dict_refs: DictRefs,

    // ── Watermarks ─────────────────────────────────────────────────
    pub subject_watermarks: Vec<u64>,
    pub string_watermark: u32,

    // ── Import-only ordering invariants ────────────────────────────
    /// True if string dictionary IDs (LEX_ID / StringId) are assigned in
    /// lexicographic UTF-8 byte order of the underlying string values.
    ///
    /// This is true for ledgers created via the bulk import pipeline which
    /// assigns global string IDs via a k-way merge of per-chunk sorted vocab files.
    ///
    /// **Important**: incremental dictionary updates append new strings above the
    /// current watermark, which breaks this invariant. Indexing code must clear
    /// this flag on the first post-import write.
    pub lex_sorted_string_ids: bool,

    // ── Cumulative commit stats ────────────────────────────────────
    pub total_commit_size: u64,
    pub total_asserts: u64,
    pub total_retracts: u64,

    // ── Per-graph specialty arenas ───────────────────────────────
    pub graph_arenas: Vec<GraphArenaRefs>,

    // ── V3-specific: o_type table ──────────────────────────────────
    /// Maps OType values to decode kind, datatype IRI, and dict family.
    /// Built-in types are included for completeness; customer types
    /// and dynamic langString entries are required.
    pub o_type_table: Vec<OTypeTableEntry>,

    // ── V3-specific: default graph routing (inline, no branch fetch) ──
    /// Default graph (g_id=0) leaf entries per sort order, inline in the root.
    /// Inline order routing with leaf entries.
    pub default_graph_orders: Vec<DefaultGraphOrder>,

    // ── V3-specific: named graph routing using V3 branch CIDs ────────
    /// Named graph routing: branch CIDs per sort order (FBR3 branches).
    pub named_graphs: Vec<NamedGraphRouting>,

    // ── Optional sections ──────────────────────────────────────────────────
    pub stats: Option<IndexStats>,
    pub schema: Option<IndexSchema>,
    pub prev_index: Option<BinaryPrevIndexRef>,
    pub garbage: Option<BinaryGarbageRef>,
    pub sketch_ref: Option<ContentId>,
}

impl IndexRoot {
    /// Build the `o_type_table` from custom datatype IRIs and language tags.
    ///
    /// Includes all built-in OType constants plus customer-defined and
    /// langString entries.
    ///
    /// - `custom_datatype_iris`: IRIs for non-reserved datatypes only
    ///   (DatatypeDictId values ≥ RESERVED_COUNT). Do NOT include the
    ///   15 reserved well-known types — those are hardcoded.
    /// - `language_tags`: BCP 47 tag strings, one per `lang_id` (index = lang_id).
    pub fn build_o_type_table(
        custom_datatype_iris: &[String],
        language_tags: &[String],
    ) -> Vec<OTypeTableEntry> {
        let mut table = Vec::new();

        // Built-in embedded types (tag 00).
        let embedded_types: &[(u16, DecodeKind, Option<&str>)] = &[
            // Null isn't a standard RDF literal, but Fluree currently treats it as xsd:string
            // for fact identity consistency.
            (OType::NULL.as_u16(), DecodeKind::Null, Some(xsd::STRING)),
            (
                OType::XSD_BOOLEAN.as_u16(),
                DecodeKind::Bool,
                Some(xsd::BOOLEAN),
            ),
            (
                OType::XSD_INTEGER.as_u16(),
                DecodeKind::I64,
                Some(xsd::INTEGER),
            ),
            (OType::XSD_LONG.as_u16(), DecodeKind::I64, Some(xsd::LONG)),
            (OType::XSD_INT.as_u16(), DecodeKind::I64, Some(xsd::INT)),
            (OType::XSD_SHORT.as_u16(), DecodeKind::I64, Some(xsd::SHORT)),
            (OType::XSD_BYTE.as_u16(), DecodeKind::I64, Some(xsd::BYTE)),
            (
                OType::XSD_UNSIGNED_LONG.as_u16(),
                DecodeKind::I64,
                Some(xsd::UNSIGNED_LONG),
            ),
            (
                OType::XSD_UNSIGNED_INT.as_u16(),
                DecodeKind::I64,
                Some(xsd::UNSIGNED_INT),
            ),
            (
                OType::XSD_UNSIGNED_SHORT.as_u16(),
                DecodeKind::I64,
                Some(xsd::UNSIGNED_SHORT),
            ),
            (
                OType::XSD_UNSIGNED_BYTE.as_u16(),
                DecodeKind::I64,
                Some(xsd::UNSIGNED_BYTE),
            ),
            (
                OType::XSD_NON_NEGATIVE_INTEGER.as_u16(),
                DecodeKind::I64,
                Some(xsd::NON_NEGATIVE_INTEGER),
            ),
            (
                OType::XSD_POSITIVE_INTEGER.as_u16(),
                DecodeKind::I64,
                Some(xsd::POSITIVE_INTEGER),
            ),
            (
                OType::XSD_NON_POSITIVE_INTEGER.as_u16(),
                DecodeKind::I64,
                Some(xsd::NON_POSITIVE_INTEGER),
            ),
            (
                OType::XSD_NEGATIVE_INTEGER.as_u16(),
                DecodeKind::I64,
                Some(xsd::NEGATIVE_INTEGER),
            ),
            (
                OType::XSD_DOUBLE.as_u16(),
                DecodeKind::F64,
                Some(xsd::DOUBLE),
            ),
            (OType::XSD_FLOAT.as_u16(), DecodeKind::F64, Some(xsd::FLOAT)),
            (
                OType::XSD_DECIMAL.as_u16(),
                DecodeKind::F64,
                Some(xsd::DECIMAL),
            ),
            (OType::XSD_DATE.as_u16(), DecodeKind::Date, Some(xsd::DATE)),
            (OType::XSD_TIME.as_u16(), DecodeKind::Time, Some(xsd::TIME)),
            (
                OType::XSD_DATE_TIME.as_u16(),
                DecodeKind::DateTime,
                Some(xsd::DATE_TIME),
            ),
            (
                OType::XSD_G_YEAR.as_u16(),
                DecodeKind::GYear,
                Some(xsd::G_YEAR),
            ),
            (
                OType::XSD_G_YEAR_MONTH.as_u16(),
                DecodeKind::GYearMonth,
                Some(xsd::G_YEAR_MONTH),
            ),
            (
                OType::XSD_G_MONTH.as_u16(),
                DecodeKind::GMonth,
                Some(xsd::G_MONTH),
            ),
            (
                OType::XSD_G_DAY.as_u16(),
                DecodeKind::GDay,
                Some(xsd::G_DAY),
            ),
            (
                OType::XSD_G_MONTH_DAY.as_u16(),
                DecodeKind::GMonthDay,
                Some(xsd::G_MONTH_DAY),
            ),
            (
                OType::XSD_YEAR_MONTH_DURATION.as_u16(),
                DecodeKind::YearMonthDuration,
                Some(xsd::YEAR_MONTH_DURATION),
            ),
            (
                OType::XSD_DAY_TIME_DURATION.as_u16(),
                DecodeKind::DayTimeDuration,
                Some(xsd::DAY_TIME_DURATION),
            ),
            (
                OType::XSD_DURATION.as_u16(),
                DecodeKind::Duration,
                Some(xsd::DURATION),
            ),
            (
                OType::GEO_POINT.as_u16(),
                DecodeKind::GeoPoint,
                Some(geo::WKT_LITERAL),
            ),
            (OType::BLANK_NODE.as_u16(), DecodeKind::BlankNode, None),
        ];

        for &(o_type, decode_kind, dt_iri) in embedded_types {
            table.push(OTypeTableEntry {
                o_type,
                decode_kind,
                datatype_iri: dt_iri.map(String::from),
                dict_family: None,
            });
        }

        // Fluree-reserved dict-backed types (tag 10).
        let fluree_types: &[(u16, DecodeKind, Option<&str>, DictFamily)] = &[
            (
                OType::XSD_STRING.as_u16(),
                DecodeKind::StringDict,
                Some(xsd::STRING),
                DictFamily::StringDict,
            ),
            (
                OType::XSD_ANY_URI.as_u16(),
                DecodeKind::StringDict,
                Some(xsd::ANY_URI),
                DictFamily::StringDict,
            ),
            (
                OType::XSD_NORMALIZED_STRING.as_u16(),
                DecodeKind::StringDict,
                Some(xsd::NORMALIZED_STRING),
                DictFamily::StringDict,
            ),
            (
                OType::XSD_TOKEN.as_u16(),
                DecodeKind::StringDict,
                Some(xsd::TOKEN),
                DictFamily::StringDict,
            ),
            (
                OType::XSD_LANGUAGE.as_u16(),
                DecodeKind::StringDict,
                Some(xsd::LANGUAGE),
                DictFamily::StringDict,
            ),
            (
                OType::XSD_BASE64_BINARY.as_u16(),
                DecodeKind::StringDict,
                Some(xsd::BASE64_BINARY),
                DictFamily::StringDict,
            ),
            (
                OType::XSD_HEX_BINARY.as_u16(),
                DecodeKind::StringDict,
                Some(xsd::HEX_BINARY),
                DictFamily::StringDict,
            ),
            (
                OType::IRI_REF.as_u16(),
                DecodeKind::IriRef,
                None,
                DictFamily::SubjectDict,
            ),
            (
                OType::RDF_JSON.as_u16(),
                DecodeKind::JsonArena,
                Some(rdf::JSON),
                DictFamily::JsonArena,
            ),
            (
                OType::VECTOR.as_u16(),
                DecodeKind::VectorArena,
                Some(fluree::EMBEDDING_VECTOR),
                DictFamily::VectorArena,
            ),
            (
                OType::FULLTEXT.as_u16(),
                DecodeKind::StringDict,
                Some(fluree::FULL_TEXT),
                DictFamily::StringDict,
            ),
            (
                OType::NUM_BIG_OVERFLOW.as_u16(),
                DecodeKind::NumBigArena,
                None,
                DictFamily::NumBigArena,
            ),
            (
                OType::SPATIAL_COMPLEX.as_u16(),
                DecodeKind::SpatialArena,
                None,
                DictFamily::SpatialArena,
            ),
        ];

        for &(o_type, decode_kind, dt_iri, dict_family) in fluree_types {
            table.push(OTypeTableEntry {
                o_type,
                decode_kind,
                datatype_iri: dt_iri.map(String::from),
                dict_family: Some(dict_family),
            });
        }

        // LangString entries (tag 11).
        // LanguageTagDict is 1-based: lang_id=1 is the first tag, lang_id=0 means "no tag".
        // language_tags[0] → lang_id=1, language_tags[1] → lang_id=2, etc.
        for (i, _tag) in language_tags.iter().enumerate() {
            let lang_id = (i as u16) + 1; // 1-based
            let ot = OType::lang_string(lang_id);
            table.push(OTypeTableEntry {
                o_type: ot.as_u16(),
                decode_kind: DecodeKind::StringDict,
                datatype_iri: Some(rdf::LANG_STRING.to_string()),
                dict_family: Some(DictFamily::StringDict),
            });
        }

        // Customer-defined datatypes (tag 01).
        // Caller passes only non-reserved IRIs (DatatypeDictId ≥ RESERVED_COUNT).
        for (i, iri) in custom_datatype_iris.iter().enumerate() {
            let dt_id = fluree_db_core::DatatypeDictId::RESERVED_COUNT + i as u16;
            let ot = OType::customer_datatype(dt_id);
            table.push(OTypeTableEntry {
                o_type: ot.as_u16(),
                decode_kind: DecodeKind::StringDict,
                datatype_iri: Some(iri.clone()),
                dict_family: Some(DictFamily::StringDict),
            });
        }

        table
    }

    // ====================================================================
    // Encode / Decode
    // ====================================================================

    /// Header size in bytes: magic(4) + version(1) + flags(1) + pad(2) + index_t(8) + base_t(8).
    const HEADER_LEN: usize = 24;

    /// Flag bits for optional sections.
    const FLAG_HAS_STATS: u8 = 1 << 0;
    const FLAG_HAS_SCHEMA: u8 = 1 << 1;
    const FLAG_HAS_PREV_INDEX: u8 = 1 << 2;
    const FLAG_HAS_GARBAGE: u8 = 1 << 3;
    const FLAG_HAS_SKETCH: u8 = 1 << 4;
    const FLAG_LEX_SORTED_STRING_IDS: u8 = 1 << 5;

    /// Encode to the binary FIR6 wire format.
    ///
    /// Determinism: namespaces sorted by ns_code, named graphs by g_id,
    /// orders by order_id, numbig/vectors/spatial/fulltext by p_id.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(8192);

        // ---- Header (24 bytes) ----
        buf.extend_from_slice(ROOT_V6_MAGIC);
        buf.push(ROOT_V6_VERSION);
        let flags = (if self.stats.is_some() {
            Self::FLAG_HAS_STATS
        } else {
            0
        }) | (if self.schema.is_some() {
            Self::FLAG_HAS_SCHEMA
        } else {
            0
        }) | (if self.prev_index.is_some() {
            Self::FLAG_HAS_PREV_INDEX
        } else {
            0
        }) | (if self.garbage.is_some() {
            Self::FLAG_HAS_GARBAGE
        } else {
            0
        }) | (if self.sketch_ref.is_some() {
            Self::FLAG_HAS_SKETCH
        } else {
            0
        }) | (if self.lex_sorted_string_ids {
            Self::FLAG_LEX_SORTED_STRING_IDS
        } else {
            0
        });
        buf.push(flags);
        buf.extend_from_slice(&0u16.to_le_bytes()); // pad
        buf.extend_from_slice(&self.index_t.to_le_bytes());
        buf.extend_from_slice(&self.base_t.to_le_bytes());

        // ---- Ledger ID ----
        write_str(&mut buf, &self.ledger_id);

        // ---- Subject ID encoding ----
        buf.push(match self.subject_id_encoding {
            fluree_db_core::SubjectIdEncoding::Narrow => 0,
            fluree_db_core::SubjectIdEncoding::Wide => 1,
        });

        // ---- ns_split_mode (1 byte) ----
        buf.push(
            self.ns_split_mode
                .to_byte()
                .expect("ns_split_mode must be persistable"),
        );

        // ---- Namespace codes (sorted by ns_code) ----
        buf.extend_from_slice(&(self.namespace_codes.len() as u16).to_le_bytes());
        for (&ns_code, prefix) in &self.namespace_codes {
            buf.extend_from_slice(&ns_code.to_le_bytes());
            write_str(&mut buf, prefix);
        }

        // ---- Predicate SIDs ----
        buf.extend_from_slice(&(self.predicate_sids.len() as u32).to_le_bytes());
        for (ns_code, suffix) in &self.predicate_sids {
            buf.extend_from_slice(&ns_code.to_le_bytes());
            write_str(&mut buf, suffix);
        }

        // ---- Small dict inlines ----
        write_string_array(&mut buf, &self.graph_iris);
        write_string_array(&mut buf, &self.datatype_iris);
        write_string_array(&mut buf, &self.language_tags);

        // ---- Dict refs ----
        write_dict_pack_refs(&mut buf, &self.dict_refs.forward_packs);
        write_dict_tree_refs(&mut buf, &self.dict_refs.subject_reverse);
        write_dict_tree_refs(&mut buf, &self.dict_refs.string_reverse);

        // ---- Per-graph specialty arenas ----
        let mut sorted_arenas = self.graph_arenas.clone();
        sorted_arenas.sort_by_key(|ga| ga.g_id);
        buf.extend_from_slice(&(sorted_arenas.len() as u16).to_le_bytes());
        for ga in &sorted_arenas {
            buf.extend_from_slice(&ga.g_id.to_le_bytes());
            write_graph_arenas_v5(&mut buf, ga);
        }

        // ---- Watermarks ----
        buf.extend_from_slice(&(self.subject_watermarks.len() as u16).to_le_bytes());
        for &wm in &self.subject_watermarks {
            buf.extend_from_slice(&wm.to_le_bytes());
        }
        buf.extend_from_slice(&self.string_watermark.to_le_bytes());

        // ---- Cumulative commit stats ----
        buf.extend_from_slice(&self.total_commit_size.to_le_bytes());
        buf.extend_from_slice(&self.total_asserts.to_le_bytes());
        buf.extend_from_slice(&self.total_retracts.to_le_bytes());

        // ---- o_type table ----
        buf.extend_from_slice(&(self.o_type_table.len() as u32).to_le_bytes());
        for entry in &self.o_type_table {
            buf.extend_from_slice(&entry.o_type.to_le_bytes());
            buf.push(entry.decode_kind as u8);
            // flags: bit 0 = has_datatype_iri, bit 1 = has_dict_family
            let entry_flags = u8::from(entry.datatype_iri.is_some())
                | (if entry.dict_family.is_some() { 2u8 } else { 0 });
            buf.push(entry_flags);
            if let Some(ref iri) = entry.datatype_iri {
                write_str(&mut buf, iri);
            }
            if let Some(df) = entry.dict_family {
                buf.push(df as u8);
            }
        }

        // ---- Default graph routing (inline, V3 key types) ----
        let mut sorted_default = self.default_graph_orders.clone();
        sorted_default.sort_by_key(|o| o.order.to_wire_id());
        buf.push(sorted_default.len() as u8);
        let mut rec_buf = [0u8; RECORD_V2_WIRE_SIZE];
        for dgo in &sorted_default {
            buf.push(dgo.order.to_wire_id());
            buf.extend_from_slice(&(dgo.leaves.len() as u32).to_le_bytes());
            for leaf in &dgo.leaves {
                leaf.first_key.write_run_le(&mut rec_buf);
                buf.extend_from_slice(&rec_buf);
                leaf.last_key.write_run_le(&mut rec_buf);
                buf.extend_from_slice(&rec_buf);
                buf.extend_from_slice(&leaf.row_count.to_le_bytes());
                write_cid(&mut buf, &leaf.leaf_cid);
                if let Some(ref sidecar) = leaf.sidecar_cid {
                    buf.push(1);
                    write_cid(&mut buf, sidecar);
                } else {
                    buf.push(0);
                }
            }
        }

        // ---- Named graph routing (V3 branch CIDs) ----
        let mut sorted_named = self.named_graphs.clone();
        sorted_named.sort_by_key(|ng| ng.g_id);
        buf.extend_from_slice(&(sorted_named.len() as u16).to_le_bytes());
        for ng in &sorted_named {
            buf.extend_from_slice(&ng.g_id.to_le_bytes());
            let mut sorted_orders = ng.orders.clone();
            sorted_orders.sort_by_key(|(order, _)| order.to_wire_id());
            buf.push(sorted_orders.len() as u8);
            for (order, branch_cid) in &sorted_orders {
                buf.push(order.to_wire_id());
                write_cid(&mut buf, branch_cid);
            }
        }

        // ---- Optional: stats ----
        if let Some(ref stats) = self.stats {
            let stats_bytes = stats_wire::encode_stats(stats);
            buf.extend_from_slice(&(stats_bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(&stats_bytes);
        }

        // ---- Optional: schema ----
        if let Some(ref schema) = self.schema {
            let schema_bytes = stats_wire::encode_schema(schema);
            buf.extend_from_slice(&(schema_bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(&schema_bytes);
        }

        // ---- Optional: prev_index ----
        if let Some(ref prev) = self.prev_index {
            buf.extend_from_slice(&prev.t.to_le_bytes());
            write_cid(&mut buf, &prev.id);
        }

        // ---- Optional: garbage ----
        if let Some(ref garbage) = self.garbage {
            write_cid(&mut buf, &garbage.id);
        }

        // ---- Optional: sketch_ref ----
        if let Some(ref sketch) = self.sketch_ref {
            write_cid(&mut buf, sketch);
        }

        buf
    }

    /// Decode from FIR6 binary bytes.
    pub fn decode(data: &[u8]) -> io::Result<Self> {
        if data.len() < Self::HEADER_LEN {
            return Err(io_err("root v6: too small for header"));
        }
        if data[0..4] != *ROOT_V6_MAGIC {
            return Err(io_err(&format!(
                "root v6: expected magic FIR6, got {:?}",
                &data[0..4]
            )));
        }
        let version = data[4];
        if version != ROOT_V6_VERSION {
            return Err(io_err(&format!("root v6: unsupported version {version}")));
        }

        let flags = data[5];
        let mut pos = 8; // skip pad(2)
        let index_t = read_i64_at(data, &mut pos)?;
        let base_t = read_i64_at(data, &mut pos)?;
        let lex_sorted_string_ids = (flags & Self::FLAG_LEX_SORTED_STRING_IDS) != 0;

        // Ledger ID
        let ledger_id = read_string(data, &mut pos)?;

        // Subject ID encoding
        let enc_byte = read_u8_at(data, &mut pos)?;
        let subject_id_encoding = match enc_byte {
            0 => fluree_db_core::SubjectIdEncoding::Narrow,
            1 => fluree_db_core::SubjectIdEncoding::Wide,
            other => return Err(io_err(&format!("root v6: unknown encoding {other}"))),
        };

        // ns_split_mode
        let ns_split_mode_byte = read_u8_at(data, &mut pos)?;
        let ns_split_mode = fluree_db_core::ns_encoding::NsSplitMode::from_byte(ns_split_mode_byte);

        // Namespace codes
        let ns_count = read_u16_at(data, &mut pos)? as usize;
        let mut namespace_codes = BTreeMap::new();
        for _ in 0..ns_count {
            let ns_code = read_u16_at(data, &mut pos)?;
            let prefix = read_string(data, &mut pos)?;
            namespace_codes.insert(ns_code, prefix);
        }

        // Predicate SIDs
        let pred_count = read_u32_at(data, &mut pos)? as usize;
        let mut predicate_sids = Vec::with_capacity(pred_count);
        for _ in 0..pred_count {
            let ns_code = read_u16_at(data, &mut pos)?;
            let suffix = read_string(data, &mut pos)?;
            predicate_sids.push((ns_code, suffix));
        }

        // Small dict inlines
        let graph_iris = read_string_array(data, &mut pos)?;
        let datatype_iris = read_string_array(data, &mut pos)?;
        let language_tags = read_string_array(data, &mut pos)?;

        // Dict refs
        let forward_packs = read_dict_pack_refs(data, &mut pos)?;
        let subject_reverse = read_dict_tree_refs(data, &mut pos)?;
        let string_reverse = read_dict_tree_refs(data, &mut pos)?;
        let dict_refs = DictRefs {
            forward_packs,
            subject_reverse,
            string_reverse,
        };

        // Per-graph specialty arenas
        let arena_count = read_u16_at(data, &mut pos)? as usize;
        let mut graph_arenas = Vec::with_capacity(arena_count);
        for _ in 0..arena_count {
            graph_arenas.push(read_graph_arenas_v5(data, &mut pos)?);
        }

        // Watermarks
        let wm_count = read_u16_at(data, &mut pos)? as usize;
        let mut subject_watermarks = Vec::with_capacity(wm_count);
        for _ in 0..wm_count {
            subject_watermarks.push(read_u64_at(data, &mut pos)?);
        }
        let string_watermark = read_u32_at(data, &mut pos)?;

        // Cumulative commit stats
        let total_commit_size = read_u64_at(data, &mut pos)?;
        let total_asserts = read_u64_at(data, &mut pos)?;
        let total_retracts = read_u64_at(data, &mut pos)?;

        // o_type table
        let otype_count = read_u32_at(data, &mut pos)? as usize;
        let mut o_type_table = Vec::with_capacity(otype_count);
        for _ in 0..otype_count {
            let o_type = read_u16_at(data, &mut pos)?;
            let dk_byte = read_u8_at(data, &mut pos)?;
            let decode_kind = DecodeKind::from_u8(dk_byte)
                .ok_or_else(|| io_err(&format!("root v6: unknown decode_kind {dk_byte}")))?;
            let entry_flags = read_u8_at(data, &mut pos)?;
            let datatype_iri = if entry_flags & 1 != 0 {
                Some(read_string(data, &mut pos)?)
            } else {
                None
            };
            let dict_family =
                if entry_flags & 2 != 0 {
                    let df_byte = read_u8_at(data, &mut pos)?;
                    Some(DictFamily::from_u8(df_byte).ok_or_else(|| {
                        io_err(&format!("root v6: unknown dict_family {df_byte}"))
                    })?)
                } else {
                    None
                };
            o_type_table.push(OTypeTableEntry {
                o_type,
                decode_kind,
                datatype_iri,
                dict_family,
            });
        }

        // Default graph routing (V3 keys)
        let order_count = read_u8_at(data, &mut pos)? as usize;
        let mut default_graph_orders = Vec::with_capacity(order_count);
        for _ in 0..order_count {
            let order_id = read_u8_at(data, &mut pos)?;
            let order = RunSortOrder::from_wire_id(order_id)
                .ok_or_else(|| io_err(&format!("root v6: invalid order_id {order_id}")))?;
            let leaf_count = read_u32_at(data, &mut pos)? as usize;
            let mut leaves = Vec::with_capacity(leaf_count);
            for _ in 0..leaf_count {
                let first_key = read_run_record_v2(data, &mut pos)?;
                let last_key = read_run_record_v2(data, &mut pos)?;
                let row_count = read_u64_at(data, &mut pos)?;
                let leaf_cid = read_cid(data, &mut pos)?;
                let has_sidecar = read_u8_at(data, &mut pos)?;
                let sidecar_cid = if has_sidecar != 0 {
                    Some(read_cid(data, &mut pos)?)
                } else {
                    None
                };
                leaves.push(LeafEntry {
                    first_key,
                    last_key,
                    row_count,
                    leaf_cid,
                    sidecar_cid,
                });
            }
            default_graph_orders.push(DefaultGraphOrder { order, leaves });
        }

        // Named graph routing
        let named_count = read_u16_at(data, &mut pos)? as usize;
        let mut named_graphs = Vec::with_capacity(named_count);
        for _ in 0..named_count {
            let g_id = read_u16_at(data, &mut pos)?;
            let ng_order_count = read_u8_at(data, &mut pos)? as usize;
            let mut orders = Vec::with_capacity(ng_order_count);
            for _ in 0..ng_order_count {
                let oid = read_u8_at(data, &mut pos)?;
                let order = RunSortOrder::from_wire_id(oid).ok_or_else(|| {
                    io_err(&format!("root v6: invalid named graph order_id {oid}"))
                })?;
                let branch_cid = read_cid(data, &mut pos)?;
                orders.push((order, branch_cid));
            }
            named_graphs.push(NamedGraphRouting { g_id, orders });
        }

        // Optional sections
        let stats = if flags & Self::FLAG_HAS_STATS != 0 {
            let stats_len = read_u32_at(data, &mut pos)? as usize;
            ensure_bytes(data, pos, stats_len, "stats section")?;
            let (stats, _) = stats_wire::decode_stats_with_len(&data[pos..pos + stats_len])?;
            pos += stats_len;
            Some(stats)
        } else {
            None
        };

        let schema = if flags & Self::FLAG_HAS_SCHEMA != 0 {
            let schema_len = read_u32_at(data, &mut pos)? as usize;
            ensure_bytes(data, pos, schema_len, "schema section")?;
            let (schema, _) = stats_wire::decode_schema_with_len(&data[pos..pos + schema_len])?;
            pos += schema_len;
            Some(schema)
        } else {
            None
        };

        let prev_index = if flags & Self::FLAG_HAS_PREV_INDEX != 0 {
            let t = read_i64_at(data, &mut pos)?;
            let id = read_cid(data, &mut pos)?;
            Some(BinaryPrevIndexRef { t, id })
        } else {
            None
        };

        let garbage = if flags & Self::FLAG_HAS_GARBAGE != 0 {
            let id = read_cid(data, &mut pos)?;
            Some(BinaryGarbageRef { id })
        } else {
            None
        };

        let sketch_ref = if flags & Self::FLAG_HAS_SKETCH != 0 {
            Some(read_cid(data, &mut pos)?)
        } else {
            None
        };

        Ok(IndexRoot {
            ledger_id,
            index_t,
            base_t,
            subject_id_encoding,
            namespace_codes,
            predicate_sids,
            ns_split_mode,
            graph_iris,
            datatype_iris,
            language_tags,
            dict_refs,
            subject_watermarks,
            string_watermark,
            lex_sorted_string_ids,
            total_commit_size,
            total_asserts,
            total_retracts,
            graph_arenas,
            o_type_table,
            default_graph_orders,
            named_graphs,
            stats,
            schema,
            prev_index,
            garbage,
            sketch_ref,
        })
    }
    /// Collect all CAS content-artifact CIDs referenced by this root.
    ///
    /// Includes: dict artifacts, leaf CIDs + sidecar CIDs (default graph inline),
    /// branch CIDs (named graphs), numbig, vectors, spatial, fulltext, sketch.
    /// Does NOT include the root's own CID or the garbage manifest CID.
    pub fn all_cas_ids(&self) -> Vec<ContentId> {
        let mut ids = Vec::new();

        // Forward dict pack CIDs
        for entry in &self.dict_refs.forward_packs.string_fwd_packs {
            ids.push(entry.pack_cid.clone());
        }
        for (_, ns_packs) in &self.dict_refs.forward_packs.subject_fwd_ns_packs {
            for entry in ns_packs {
                ids.push(entry.pack_cid.clone());
            }
        }

        // Reverse dict tree CIDs
        for tree in [
            &self.dict_refs.subject_reverse,
            &self.dict_refs.string_reverse,
        ] {
            ids.push(tree.branch.clone());
            ids.extend(tree.leaves.iter().cloned());
        }

        // Per-graph arenas (numbig, vectors, spatial, fulltext)
        for ga in &self.graph_arenas {
            for (_, cid) in &ga.numbig {
                ids.push(cid.clone());
            }
            for vdr in &ga.vectors {
                ids.push(vdr.manifest.clone());
                ids.extend(vdr.shards.iter().cloned());
            }
            for sar in &ga.spatial {
                ids.push(sar.root_cid.clone());
                ids.push(sar.manifest.clone());
                ids.push(sar.arena.clone());
                ids.extend(sar.leaflets.iter().cloned());
            }
            for ftr in &ga.fulltext {
                ids.push(ftr.arena_cid.clone());
            }
        }

        // Default graph inline leaves + sidecar CIDs
        for dgo in &self.default_graph_orders {
            for leaf in &dgo.leaves {
                ids.push(leaf.leaf_cid.clone());
                if let Some(ref sidecar) = leaf.sidecar_cid {
                    ids.push(sidecar.clone());
                }
            }
        }

        // Named graph branch CIDs
        for ng in &self.named_graphs {
            for (_, branch_cid) in &ng.orders {
                ids.push(branch_cid.clone());
            }
        }

        // Sketch
        if let Some(ref sketch) = self.sketch_ref {
            ids.push(sketch.clone());
        }

        ids.sort();
        ids.dedup();
        ids
    }
}

// ============================================================================
// Shared arena encode/decode
// ============================================================================

fn write_graph_arenas_v5(buf: &mut Vec<u8>, ga: &GraphArenaRefs) {
    // numbig
    let mut sorted_nb = ga.numbig.clone();
    sorted_nb.sort_by_key(|(p_id, _)| *p_id);
    buf.extend_from_slice(&(sorted_nb.len() as u16).to_le_bytes());
    for (p_id, cid) in &sorted_nb {
        buf.extend_from_slice(&p_id.to_le_bytes());
        write_cid(buf, cid);
    }
    // vectors
    let mut sorted_v = ga.vectors.clone();
    sorted_v.sort_by_key(|v| v.p_id);
    buf.extend_from_slice(&(sorted_v.len() as u16).to_le_bytes());
    for vdr in &sorted_v {
        buf.extend_from_slice(&vdr.p_id.to_le_bytes());
        write_cid(buf, &vdr.manifest);
        buf.extend_from_slice(&(vdr.shards.len() as u16).to_le_bytes());
        for shard_cid in &vdr.shards {
            write_cid(buf, shard_cid);
        }
    }
    // spatial
    let mut sorted_s = ga.spatial.clone();
    sorted_s.sort_by_key(|s| s.p_id);
    buf.extend_from_slice(&(sorted_s.len() as u16).to_le_bytes());
    for sar in &sorted_s {
        buf.extend_from_slice(&sar.p_id.to_le_bytes());
        write_cid(buf, &sar.root_cid);
        write_cid(buf, &sar.manifest);
        write_cid(buf, &sar.arena);
        buf.extend_from_slice(&(sar.leaflets.len() as u16).to_le_bytes());
        for leaf_cid in &sar.leaflets {
            write_cid(buf, leaf_cid);
        }
    }
    // fulltext (v6 v2: adds lang_id per entry, sorted by (p_id, lang_id))
    let mut sorted_ft = ga.fulltext.clone();
    sorted_ft.sort_by_key(|f| (f.p_id, f.lang_id));
    buf.extend_from_slice(&(sorted_ft.len() as u16).to_le_bytes());
    for ftr in &sorted_ft {
        buf.extend_from_slice(&ftr.p_id.to_le_bytes());
        buf.extend_from_slice(&ftr.lang_id.to_le_bytes());
        write_cid(buf, &ftr.arena_cid);
    }
}

fn read_graph_arenas_v5(data: &[u8], pos: &mut usize) -> io::Result<GraphArenaRefs> {
    let g_id = read_u16_at(data, pos)?;
    // numbig
    let nb_count = read_u16_at(data, pos)? as usize;
    let mut numbig = Vec::with_capacity(nb_count);
    for _ in 0..nb_count {
        let p_id = read_u32_at(data, pos)?;
        let cid = read_cid(data, pos)?;
        numbig.push((p_id, cid));
    }
    // vectors
    let vec_count = read_u16_at(data, pos)? as usize;
    let mut vectors = Vec::with_capacity(vec_count);
    for _ in 0..vec_count {
        let p_id = read_u32_at(data, pos)?;
        let manifest = read_cid(data, pos)?;
        let shard_count = read_u16_at(data, pos)? as usize;
        let mut shards = Vec::with_capacity(shard_count);
        for _ in 0..shard_count {
            shards.push(read_cid(data, pos)?);
        }
        vectors.push(VectorDictRef {
            p_id,
            manifest,
            shards,
        });
    }
    // spatial
    let sp_count = read_u16_at(data, pos)? as usize;
    let mut spatial = Vec::with_capacity(sp_count);
    for _ in 0..sp_count {
        let p_id = read_u32_at(data, pos)?;
        let root_cid = read_cid(data, pos)?;
        let manifest = read_cid(data, pos)?;
        let arena = read_cid(data, pos)?;
        let leaf_count = read_u16_at(data, pos)? as usize;
        let mut leaflets = Vec::with_capacity(leaf_count);
        for _ in 0..leaf_count {
            leaflets.push(read_cid(data, pos)?);
        }
        spatial.push(SpatialArenaRef {
            p_id,
            root_cid,
            manifest,
            arena,
            leaflets,
        });
    }
    // fulltext (v6 v2: includes lang_id)
    let ft_count = read_u16_at(data, pos)? as usize;
    let mut fulltext = Vec::with_capacity(ft_count);
    for _ in 0..ft_count {
        let p_id = read_u32_at(data, pos)?;
        let lang_id = read_u16_at(data, pos)?;
        let arena_cid = read_cid(data, pos)?;
        fulltext.push(FulltextArenaRef {
            p_id,
            lang_id,
            arena_cid,
        });
    }
    Ok(GraphArenaRefs {
        g_id,
        numbig,
        vectors,
        spatial,
        fulltext,
    })
}

/// Read a V2 run record (30 bytes) at the current position.
fn read_run_record_v2(data: &[u8], pos: &mut usize) -> io::Result<RunRecordV2> {
    ensure_bytes(data, *pos, RECORD_V2_WIRE_SIZE, "RunRecordV2")?;
    let rec = RunRecordV2::read_run_le(data[*pos..*pos + RECORD_V2_WIRE_SIZE].try_into().unwrap());
    *pos += RECORD_V2_WIRE_SIZE;
    Ok(rec)
}

#[cfg(test)]
mod tests {
    use super::super::wire_helpers::{DictPackRefs, DictRefs, DictTreeRefs, PackBranchEntry};
    use super::*;
    use fluree_db_core::subject_id::SubjectId;
    use fluree_vocab::{rdf, xsd};

    /// Build a minimal IndexRoot for testing.
    fn minimal_root_v6() -> IndexRoot {
        // Create a dummy CID for dict refs
        let dummy_cid = ContentId::from_hex_digest(
            fluree_db_core::content_kind::CODEC_FLUREE_DICT_BLOB,
            &fluree_db_core::sha256_hex(b"dummy"),
        )
        .unwrap();

        IndexRoot {
            ledger_id: "test:main".to_string(),
            index_t: 42,
            base_t: 0,
            subject_id_encoding: fluree_db_core::SubjectIdEncoding::Narrow,
            namespace_codes: {
                let mut m = BTreeMap::new();
                m.insert(0, "http://example.org/".to_string());
                m
            },
            predicate_sids: vec![(0, "name".to_string()), (0, "age".to_string())],
            graph_iris: vec![],
            datatype_iris: vec![xsd::STRING.to_string(), xsd::INTEGER.to_string()],
            language_tags: vec!["en".to_string()],
            dict_refs: DictRefs {
                forward_packs: DictPackRefs {
                    string_fwd_packs: vec![PackBranchEntry {
                        first_id: 0,
                        last_id: 100,
                        pack_cid: dummy_cid.clone(),
                    }],
                    subject_fwd_ns_packs: vec![(
                        0u16,
                        vec![PackBranchEntry {
                            first_id: 0,
                            last_id: 50,
                            pack_cid: dummy_cid.clone(),
                        }],
                    )],
                },
                subject_reverse: DictTreeRefs {
                    branch: dummy_cid.clone(),
                    leaves: vec![dummy_cid.clone()],
                },
                string_reverse: DictTreeRefs {
                    branch: dummy_cid.clone(),
                    leaves: vec![dummy_cid.clone()],
                },
            },
            subject_watermarks: vec![50],
            string_watermark: 100,
            lex_sorted_string_ids: false,
            total_commit_size: 1024,
            total_asserts: 10,
            total_retracts: 0,
            graph_arenas: vec![],
            o_type_table: IndexRoot::build_o_type_table(&[], &["en".to_string()]),
            default_graph_orders: vec![],
            named_graphs: vec![],
            stats: None,
            schema: None,
            prev_index: None,
            garbage: None,
            sketch_ref: None,
            ns_split_mode: fluree_db_core::ns_encoding::NsSplitMode::default(),
        }
    }

    #[test]
    fn fir6_round_trip_minimal() {
        let root = minimal_root_v6();
        let bytes = root.encode();

        assert_eq!(&bytes[0..4], b"FIR6");
        assert_eq!(bytes[4], ROOT_V6_VERSION);
        assert_eq!(bytes[5], 0); // no optional sections

        let decoded = IndexRoot::decode(&bytes).unwrap();
        assert_eq!(decoded.ledger_id, "test:main");
        assert_eq!(decoded.index_t, 42);
        assert_eq!(decoded.base_t, 0);
        assert_eq!(decoded.namespace_codes.len(), 1);
        assert_eq!(decoded.predicate_sids.len(), 2);
        assert_eq!(decoded.language_tags, vec!["en"]);
        assert_eq!(decoded.o_type_table.len(), root.o_type_table.len());
        assert_eq!(decoded.default_graph_orders.len(), 0);
        assert_eq!(decoded.named_graphs.len(), 0);
        assert!(decoded.stats.is_none());
    }

    #[test]
    fn fir6_round_trip_with_default_graph() {
        let mut root = minimal_root_v6();

        // Create dummy leaf entries for the default graph
        let dummy_cid = ContentId::from_hex_digest(
            fluree_db_core::content_kind::CODEC_FLUREE_INDEX_LEAF,
            &fluree_db_core::sha256_hex(b"leaf1"),
        )
        .unwrap();
        let sidecar_cid = ContentId::from_hex_digest(
            fluree_db_core::content_kind::CODEC_FLUREE_HISTORY_SIDECAR,
            &fluree_db_core::sha256_hex(b"sidecar1"),
        )
        .unwrap();

        let leaf = LeafEntry {
            first_key: RunRecordV2 {
                s_id: SubjectId(1),
                o_key: 100,
                p_id: 2,
                t: 1,
                o_i: u32::MAX,
                o_type: OType::XSD_INTEGER.as_u16(),
                g_id: 0,
            },
            last_key: RunRecordV2 {
                s_id: SubjectId(999),
                o_key: 500,
                p_id: 5,
                t: 1,
                o_i: u32::MAX,
                o_type: OType::XSD_STRING.as_u16(),
                g_id: 0,
            },
            row_count: 25000,
            leaf_cid: dummy_cid,
            sidecar_cid: Some(sidecar_cid),
        };

        root.default_graph_orders = vec![
            DefaultGraphOrder {
                order: RunSortOrder::Spot,
                leaves: vec![leaf.clone()],
            },
            DefaultGraphOrder {
                order: RunSortOrder::Post,
                leaves: vec![leaf],
            },
        ];

        let bytes = root.encode();
        let decoded = IndexRoot::decode(&bytes).unwrap();

        assert_eq!(decoded.default_graph_orders.len(), 2);
        // Orders should be sorted by wire ID (SPOT=0, POST=2)
        assert_eq!(decoded.default_graph_orders[0].order, RunSortOrder::Spot);
        assert_eq!(decoded.default_graph_orders[1].order, RunSortOrder::Post);
        assert_eq!(decoded.default_graph_orders[0].leaves.len(), 1);
        assert_eq!(decoded.default_graph_orders[0].leaves[0].row_count, 25000);
        assert_eq!(
            decoded.default_graph_orders[0].leaves[0].first_key.s_id,
            SubjectId(1)
        );
        assert!(decoded.default_graph_orders[0].leaves[0]
            .sidecar_cid
            .is_some());
    }

    #[test]
    fn fir6_round_trip_with_named_graphs() {
        let mut root = minimal_root_v6();

        let branch_cid = ContentId::from_hex_digest(
            fluree_db_core::content_kind::CODEC_FLUREE_INDEX_BRANCH,
            &fluree_db_core::sha256_hex(b"branch1"),
        )
        .unwrap();

        root.named_graphs = vec![NamedGraphRouting {
            g_id: 1,
            orders: vec![
                (RunSortOrder::Spot, branch_cid.clone()),
                (RunSortOrder::Post, branch_cid.clone()),
            ],
        }];

        let bytes = root.encode();
        let decoded = IndexRoot::decode(&bytes).unwrap();

        assert_eq!(decoded.named_graphs.len(), 1);
        assert_eq!(decoded.named_graphs[0].g_id, 1);
        assert_eq!(decoded.named_graphs[0].orders.len(), 2);
    }

    #[test]
    fn fir6_o_type_table_round_trip() {
        let root = minimal_root_v6();
        let bytes = root.encode();
        let decoded = IndexRoot::decode(&bytes).unwrap();

        // Verify o_type_table entries match
        assert_eq!(decoded.o_type_table.len(), root.o_type_table.len());
        for (orig, dec) in root.o_type_table.iter().zip(decoded.o_type_table.iter()) {
            assert_eq!(orig.o_type, dec.o_type);
            assert_eq!(orig.decode_kind, dec.decode_kind);
            assert_eq!(orig.datatype_iri, dec.datatype_iri);
            assert_eq!(orig.dict_family, dec.dict_family);
        }
    }

    #[test]
    fn fir6_bad_magic_rejected() {
        let root = minimal_root_v6();
        let mut bytes = root.encode();
        bytes[0..4].copy_from_slice(b"BAD!");
        let err = IndexRoot::decode(&bytes).unwrap_err();
        assert!(err.to_string().contains("FIR6"));
    }

    #[test]
    fn o_type_table_built_in() {
        let table = IndexRoot::build_o_type_table(&[], &[]);
        // Should contain all 31 embedded + 13 Fluree = 44 entries.
        assert_eq!(table.len(), 44);

        // Spot-check a few entries.
        let int_entry = table
            .iter()
            .find(|e| e.o_type == OType::XSD_INTEGER.as_u16())
            .unwrap();
        assert_eq!(int_entry.decode_kind, DecodeKind::I64);
        assert_eq!(int_entry.datatype_iri.as_deref(), Some(xsd::INTEGER));
        assert!(int_entry.dict_family.is_none());

        let string_entry = table
            .iter()
            .find(|e| e.o_type == OType::XSD_STRING.as_u16())
            .unwrap();
        assert_eq!(string_entry.decode_kind, DecodeKind::StringDict);
        assert_eq!(string_entry.dict_family, Some(DictFamily::StringDict));

        let ref_entry = table
            .iter()
            .find(|e| e.o_type == OType::IRI_REF.as_u16())
            .unwrap();
        assert_eq!(ref_entry.decode_kind, DecodeKind::IriRef);
        assert_eq!(ref_entry.dict_family, Some(DictFamily::SubjectDict));
    }

    #[test]
    fn o_type_table_with_langs() {
        let table = IndexRoot::build_o_type_table(&[], &["en".to_string(), "fr".to_string()]);
        // 44 built-in + 2 langString = 46.
        assert_eq!(table.len(), 46);

        // lang_id is 1-based: first tag "en" gets lang_id=1
        let en_entry = table
            .iter()
            .find(|e| e.o_type == OType::lang_string(1).as_u16())
            .unwrap();
        assert_eq!(en_entry.decode_kind, DecodeKind::StringDict);
        assert_eq!(en_entry.datatype_iri.as_deref(), Some(rdf::LANG_STRING));
    }

    #[test]
    fn o_type_table_with_custom_types() {
        let table = IndexRoot::build_o_type_table(&["http://example.org/myType".to_string()], &[]);
        // 44 built-in + 1 customer = 45.
        assert_eq!(table.len(), 45);

        let custom = table.last().unwrap();
        assert!(OType::from_u16(custom.o_type).is_customer_datatype());
        assert_eq!(
            custom.datatype_iri.as_deref(),
            Some("http://example.org/myType")
        );
    }

    #[test]
    fn all_cas_ids_includes_leaves_and_sidecars() {
        use crate::format::branch::LeafEntry;

        let leaf_cid = ContentId::new(fluree_db_core::ContentKind::IndexLeaf, b"leaf1");
        let sidecar_cid = ContentId::new(fluree_db_core::ContentKind::HistorySidecar, b"sc1");
        let leaf_no_sc = ContentId::new(fluree_db_core::ContentKind::IndexLeaf, b"leaf2");
        let branch_cid = ContentId::new(fluree_db_core::ContentKind::IndexBranch, b"br1");
        let sketch_cid = ContentId::new(fluree_db_core::ContentKind::Commit, b"sketch1");

        let zero_key = RunRecordV2 {
            s_id: SubjectId::from(0u64),
            o_key: 0,
            p_id: 0,
            t: 0,
            o_i: 0,
            o_type: 0,
            g_id: 0,
        };

        let mut root = minimal_root_v6();
        root.default_graph_orders = vec![DefaultGraphOrder {
            order: RunSortOrder::Spot,
            leaves: vec![
                LeafEntry {
                    first_key: zero_key,
                    last_key: zero_key,
                    row_count: 10,
                    leaf_cid: leaf_cid.clone(),
                    sidecar_cid: Some(sidecar_cid.clone()),
                },
                LeafEntry {
                    first_key: zero_key,
                    last_key: zero_key,
                    row_count: 5,
                    leaf_cid: leaf_no_sc.clone(),
                    sidecar_cid: None,
                },
            ],
        }];
        root.named_graphs = vec![NamedGraphRouting {
            g_id: 1,
            orders: vec![(RunSortOrder::Spot, branch_cid.clone())],
        }];
        root.sketch_ref = Some(sketch_cid.clone());

        let ids = root.all_cas_ids();

        // Leaf CIDs present
        assert!(ids.contains(&leaf_cid), "missing leaf_cid");
        assert!(ids.contains(&leaf_no_sc), "missing leaf without sidecar");
        // Sidecar CID present
        assert!(ids.contains(&sidecar_cid), "missing sidecar_cid");
        // Branch CID present
        assert!(ids.contains(&branch_cid), "missing branch_cid");
        // Sketch present
        assert!(ids.contains(&sketch_cid), "missing sketch_cid");
        // Dict CIDs present (from minimal_root_v6)
        assert!(
            ids.len() >= 5,
            "expected at least 5 CIDs, got {}",
            ids.len()
        );
        // No duplicates (sorted + deduped)
        let mut sorted = ids.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(ids.len(), sorted.len(), "all_cas_ids has duplicates");
    }
}
