//! Streaming RDF export from the binary SPOT index.
//!
//! Writes N-Triples, Turtle, N-Quads, or TriG directly to a `Write` sink,
//! one leaflet-batch at a time.  Memory usage is O(leaflet_size), not O(dataset).

use fluree_db_binary_index::read::types::sort_overlay_ops;
use fluree_db_binary_index::{
    BinaryCursor, BinaryFilter, BinaryIndexStore, ColumnBatch, ColumnProjection, ColumnSet,
    RunSortOrder,
};
use fluree_db_core::dict_novelty::DictNovelty;
use fluree_db_core::value::FlakeValue;
use fluree_db_core::{DecodeKind, GraphId, OType, OverlayProvider, Sid};
use fluree_db_query::binary_scan::{translate_overlay_flakes, EphemeralPredicateMap};
use fluree_vocab::xsd;
use std::collections::{BTreeMap, HashMap};
use std::io::{self, Write};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Configuration for a single-graph streaming export.
pub struct ExportConfig<'a> {
    /// Target graph ID (0 = default graph).
    pub g_id: GraphId,
    /// If `Some`, emit as N-Quads with this graph IRI as the 4th term.
    pub graph_iri: Option<String>,
    /// Time bound for the export. Rows with `t > to_t` are excluded.
    pub to_t: i64,
    /// Novelty overlay provider (committed-but-not-yet-indexed transactions).
    pub overlay: Option<&'a dyn OverlayProvider>,
    /// Dictionary novelty for resolving IDs from committed-but-not-yet-indexed transactions.
    pub dict_novelty: Option<&'a Arc<DictNovelty>>,
}

/// Counters returned after export completes.
#[derive(Debug, Default)]
pub struct ExportStats {
    pub triples_written: u64,
    pub rows_skipped: u64,
}

/// Output format for streaming export.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExportFormat {
    /// N-Triples — one triple per line, full IRIs.
    NTriples,
    /// Turtle — prefixed names, subject grouping with `;`.
    Turtle,
    /// N-Quads — N-Triples with optional 4th graph term.
    NQuads,
    /// TriG — Turtle with `GRAPH <iri> { }` blocks.
    TriG,
    /// JSON-LD — streaming from binary index with `@context` + `@graph`.
    JsonLd,
}

/// System graph IDs excluded from dataset exports.
pub const SYSTEM_GRAPH_TXN_META: GraphId = 1;
pub const SYSTEM_GRAPH_CONFIG: GraphId = 2;

/// Returns `true` if `g_id` is a system-internal graph.
pub fn is_system_graph(g_id: GraphId) -> bool {
    g_id == SYSTEM_GRAPH_TXN_META || g_id == SYSTEM_GRAPH_CONFIG
}

/// Configure a `BinaryCursor` with time-travel bounds and novelty overlay.
///
/// Returns the ephemeral predicate map for novelty-only predicates.
fn apply_time_travel(
    cursor: &mut BinaryCursor,
    config: &ExportConfig,
    store: &BinaryIndexStore,
) -> EphemeralPredicateMap {
    cursor.set_to_t(config.to_t);
    if let Some(overlay) = config.overlay {
        let (mut ops, ephemeral_preds) = translate_overlay_flakes(
            overlay,
            store,
            config.dict_novelty,
            None, // no runtime_small_dicts during export
            config.to_t,
            config.g_id,
        );
        if !ops.is_empty() {
            sort_overlay_ops(&mut ops, RunSortOrder::Spot);
            cursor.set_overlay_ops(ops);
            cursor.set_epoch(overlay.epoch());
        }
        ephemeral_preds
    } else {
        HashMap::new()
    }
}

// ---------------------------------------------------------------------------
// ExportResolver — novelty-aware ID resolution for export
// ---------------------------------------------------------------------------

/// Wraps `BinaryIndexStore` with fallback to `DictNovelty` and an ephemeral
/// predicate map, so that export can resolve IDs for data that has been
/// committed but not yet persisted to the binary index.
struct ExportResolver<'a> {
    store: &'a Arc<BinaryIndexStore>,
    dict_novelty: Option<&'a Arc<DictNovelty>>,
    /// Reverse map: ephemeral p_id → Sid (inverted from translate_overlay_flakes).
    ephemeral_preds_reverse: HashMap<u32, Sid>,
}

impl<'a> ExportResolver<'a> {
    fn new(
        store: &'a Arc<BinaryIndexStore>,
        dict_novelty: Option<&'a Arc<DictNovelty>>,
        ephemeral_preds: &EphemeralPredicateMap,
    ) -> Self {
        // Invert the Sid→p_id map to p_id→Sid for O(1) reverse lookup.
        let ephemeral_preds_reverse: HashMap<u32, Sid> = ephemeral_preds
            .iter()
            .map(|(sid, &pid)| (pid, sid.clone()))
            .collect();
        Self {
            store,
            dict_novelty,
            ephemeral_preds_reverse,
        }
    }

    /// Resolve a subject ID to an IRI string.
    ///
    /// Falls back to `DictNovelty` for IDs above the persisted watermark.
    fn resolve_subject_iri(&self, s_id: u64) -> io::Result<String> {
        match self.store.resolve_subject_iri(s_id) {
            Ok(iri) => Ok(iri),
            Err(_) => {
                if let Some(dn) = self.dict_novelty {
                    if dn.is_initialized() {
                        if let Some((ns_code, suffix)) = dn.subjects.resolve_subject(s_id) {
                            // NS_OVERFLOW (0xFFFF): suffix is the full IRI, no prefix lookup.
                            if ns_code == 0xFFFF {
                                return Ok(suffix.to_string());
                            }
                            let prefix = self.store.namespace_prefix(ns_code)?;
                            return Ok(format!("{prefix}{suffix}"));
                        }
                    }
                }
                Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("subject s_id {s_id} not found in store or DictNovelty"),
                ))
            }
        }
    }

    /// Resolve a predicate ID to an IRI string.
    ///
    /// Falls back to the ephemeral predicate map for novelty-only predicates.
    fn resolve_predicate_iri(&self, p_id: u32) -> Option<String> {
        if let Some(iri) = self.store.resolve_predicate_iri(p_id) {
            return Some(iri.to_string());
        }
        self.ephemeral_preds_reverse
            .get(&p_id)
            .and_then(|sid| self.store.sid_to_iri(sid))
    }

    /// Decode an object value from its binary representation.
    ///
    /// Falls back to `DictNovelty` for string-dict and IRI-ref values
    /// that are not in the persisted index.
    fn decode_value(
        &self,
        o_type: u16,
        o_key: u64,
        p_id: u32,
        g_id: GraphId,
    ) -> io::Result<FlakeValue> {
        match self.store.decode_value_v3(o_type, o_key, p_id, g_id) {
            Ok(val) => Ok(val),
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                // The persisted store couldn't find a dict entry. Try DictNovelty
                // for the decode kinds that use subject/string dictionaries.
                let ot = OType::from_u16(o_type);
                match ot.decode_kind() {
                    DecodeKind::StringDict | DecodeKind::JsonArena => {
                        self.decode_string_novelty(ot.decode_kind(), o_key)
                    }
                    DecodeKind::IriRef => self.decode_iri_ref_novelty(o_key),
                    _ => Err(e),
                }
            }
            Err(e) => Err(e),
        }
    }

    /// Fallback for StringDict / JsonArena: resolve via DictNovelty.
    fn decode_string_novelty(&self, kind: DecodeKind, o_key: u64) -> io::Result<FlakeValue> {
        let str_id = o_key as u32;
        if let Some(dn) = self.dict_novelty {
            if dn.is_initialized() {
                if let Some(value) = dn.strings.resolve_string(str_id) {
                    return Ok(match kind {
                        DecodeKind::JsonArena => FlakeValue::Json(value.to_string()),
                        _ => FlakeValue::String(value.to_string()),
                    });
                }
            }
        }
        Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("string id {str_id} not found in store or DictNovelty"),
        ))
    }

    /// Fallback for IriRef: resolve subject ID via DictNovelty.
    fn decode_iri_ref_novelty(&self, o_key: u64) -> io::Result<FlakeValue> {
        let iri = self.resolve_subject_iri(o_key)?;
        let sid = self.store.encode_iri(&iri);
        Ok(FlakeValue::Ref(sid))
    }
}

// ---------------------------------------------------------------------------
// Prefix map — IRI → prefixed name compression for Turtle/TriG
// ---------------------------------------------------------------------------

/// A sorted prefix map for compressing IRIs into prefixed names.
///
/// Prefixes are sorted by IRI length descending so that longest-prefix-first
/// matching produces the most specific result.
#[derive(Debug, Clone)]
pub struct PrefixMap {
    /// (prefix, namespace_iri) sorted by namespace IRI length descending.
    entries: Vec<(String, String)>,
}

impl PrefixMap {
    /// Build a prefix map from a JSON-LD `@context` object.
    ///
    /// Expects `{"prefix": "iri", ...}` — ignores entries where the value
    /// is not a string or the key starts with `@`.
    pub fn from_context(ctx: &serde_json::Value) -> Self {
        let mut entries = Vec::new();
        if let Some(obj) = ctx.as_object() {
            for (key, val) in obj {
                if key.starts_with('@') {
                    continue;
                }
                if let Some(iri) = val.as_str() {
                    entries.push((key.clone(), iri.to_string()));
                }
            }
        }
        // Sort by IRI length descending for longest-prefix-first matching
        entries.sort_by_key(|b| std::cmp::Reverse(b.1.len()));
        PrefixMap { entries }
    }

    /// Build from an explicit map of prefix → IRI.
    pub fn from_map(map: BTreeMap<String, String>) -> Self {
        let mut entries: Vec<(String, String)> = map.into_iter().collect();
        entries.sort_by_key(|b| std::cmp::Reverse(b.1.len()));
        PrefixMap { entries }
    }

    /// Try to compress a full IRI into a prefixed name (e.g., `ex:alice`).
    ///
    /// Returns `None` if no prefix matches or the local name contains
    /// characters that are invalid in a Turtle prefixed name.
    pub fn compact(&self, iri: &str) -> Option<String> {
        for (prefix, ns) in &self.entries {
            if let Some(local) = iri.strip_prefix(ns.as_str()) {
                if is_valid_pname_local(local) {
                    return Some(format!("{prefix}:{local}"));
                }
            }
        }
        None
    }

    /// Returns `true` if the map has any entries.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Iterate `(prefix, namespace_iri)` pairs.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.entries.iter().map(|(p, n)| (p.as_str(), n.as_str()))
    }
}

/// Check if `local` is a valid Turtle PN_LOCAL (simplified).
///
/// We allow ASCII alphanumeric, `-`, `_`, and `.` (but not leading/trailing `.`).
/// This is conservative — the full Turtle grammar allows more, but this covers
/// the vast majority of real-world local names without risking invalid output.
fn is_valid_pname_local(local: &str) -> bool {
    if local.is_empty() {
        return true; // bare prefix like `ex:` is valid
    }
    if local.starts_with('.') || local.ends_with('.') {
        return false;
    }
    local
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.')
}

/// Write `@prefix` declarations to a Turtle/TriG writer.
pub fn write_prefix_declarations<W: Write>(prefixes: &PrefixMap, writer: &mut W) -> io::Result<()> {
    // Sort alphabetically for deterministic, readable output
    let mut sorted: Vec<(&str, &str)> = prefixes.iter().collect();
    sorted.sort_by_key(|(p, _)| *p);
    for (prefix, ns) in sorted {
        write!(writer, "@prefix {prefix}: <")?;
        write_escaped_iri(writer, ns)?;
        writeln!(writer, "> .")?;
    }
    if !prefixes.is_empty() {
        writeln!(writer)?; // blank line after prefixes
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Turtle streaming export
// ---------------------------------------------------------------------------

/// Stream triples from the SPOT index of one graph as Turtle to `writer`.
///
/// Uses subject grouping (`;` between predicates of the same subject)
/// and prefixed names where possible.
pub fn export_graph_turtle<W: Write>(
    store: &Arc<BinaryIndexStore>,
    config: &ExportConfig<'_>,
    prefixes: &PrefixMap,
    writer: &mut W,
) -> io::Result<ExportStats> {
    let branch_ref = match store.branch_for_order(config.g_id, RunSortOrder::Spot) {
        Some(b) => b,
        None => return Ok(ExportStats::default()),
    };
    let branch = Arc::clone(branch_ref);

    let filter = BinaryFilter::default();
    let projection = ColumnProjection {
        output: ColumnSet::CORE,
        internal: ColumnSet::EMPTY,
    };

    let mut cursor = BinaryCursor::scan_all(
        Arc::clone(store),
        RunSortOrder::Spot,
        branch,
        filter,
        projection,
    );
    let ephemeral_preds = apply_time_travel(&mut cursor, config, store);
    let resolver = ExportResolver::new(store, config.dict_novelty, &ephemeral_preds);

    let mut stats = ExportStats::default();
    let mut prev_subject: Option<String> = None;

    while let Some(batch) = cursor.next_batch()? {
        write_turtle_batch(
            &resolver,
            &batch,
            config.g_id,
            prefixes,
            &mut prev_subject,
            &mut stats,
            writer,
        )?;
    }

    // Close last subject if any
    if prev_subject.is_some() {
        writeln!(writer, " .")?;
    }

    Ok(stats)
}

/// Write a batch of rows as Turtle, grouping by subject.
fn write_turtle_batch<W: Write>(
    resolver: &ExportResolver,
    batch: &ColumnBatch,
    g_id: GraphId,
    prefixes: &PrefixMap,
    prev_subject: &mut Option<String>,
    stats: &mut ExportStats,
    writer: &mut W,
) -> io::Result<()> {
    for row in 0..batch.row_count {
        let s_id = batch.s_id.get(row);
        let p_id = batch.p_id.get_or(row, 0);
        let o_type = batch.o_type.get_or(row, 0);
        let o_key = batch.o_key.get(row);

        let s_iri = resolver.resolve_subject_iri(s_id)?;
        let p_iri = match resolver.resolve_predicate_iri(p_id) {
            Some(p) => p,
            None => {
                stats.rows_skipped += 1;
                continue;
            }
        };
        let value = resolver.decode_value(o_type, o_key, p_id, g_id)?;
        if matches!(value, FlakeValue::Null) {
            stats.rows_skipped += 1;
            continue;
        }

        let same_subject = prev_subject.as_deref() == Some(&s_iri);

        if same_subject {
            // Continue same subject — semicolon separator
            write!(writer, " ;\n    ")?;
        } else {
            // New subject — close previous if any
            if prev_subject.is_some() {
                writeln!(writer, " .")?;
            }
            // Write subject
            write_turtle_iri_or_bnode(writer, &s_iri, prefixes)?;
            write!(writer, "\n    ")?;
            *prev_subject = Some(s_iri);
        }

        // Write predicate
        if p_iri == "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" {
            writer.write_all(b"a")?;
        } else {
            write_turtle_iri(writer, &p_iri, prefixes)?;
        }
        writer.write_all(b" ")?;

        // Write object
        write_turtle_object(writer, &value, resolver.store, o_type, prefixes)?;

        stats.triples_written += 1;
    }
    Ok(())
}

/// Write an IRI as a Turtle prefixed name or `<full-iri>`.
pub fn write_turtle_iri<W: Write>(w: &mut W, iri: &str, prefixes: &PrefixMap) -> io::Result<()> {
    if let Some(pname) = prefixes.compact(iri) {
        w.write_all(pname.as_bytes())
    } else {
        w.write_all(b"<")?;
        write_escaped_iri(w, iri)?;
        w.write_all(b">")
    }
}

/// Write a subject term as Turtle (prefixed name, `<iri>`, or `_:bnode`).
fn write_turtle_iri_or_bnode<W: Write>(
    w: &mut W,
    iri: &str,
    prefixes: &PrefixMap,
) -> io::Result<()> {
    if iri.starts_with("_:") {
        w.write_all(iri.as_bytes())
    } else {
        write_turtle_iri(w, iri, prefixes)
    }
}

/// Write a Turtle object term (with prefix compression for IRI refs).
fn write_turtle_object<W: Write>(
    w: &mut W,
    value: &FlakeValue,
    store: &BinaryIndexStore,
    o_type: u16,
    prefixes: &PrefixMap,
) -> io::Result<()> {
    match value {
        FlakeValue::Ref(sid) => {
            let iri = store
                .sid_to_iri(sid)
                .unwrap_or_else(|| format!("_:unknown_{sid}"));
            write_turtle_iri_or_bnode(w, &iri, prefixes)
        }
        // For all literal types, reuse the N-Triples formatting
        // (Turtle literal syntax is a superset of N-Triples)
        _ => write_object(w, value, store, o_type),
    }
}

// ---------------------------------------------------------------------------
// JSON-LD streaming export
// ---------------------------------------------------------------------------

/// Stream triples from the SPOT index of one graph as JSON-LD to `writer`.
///
/// Produces a JSON-LD document with `@context` and `@graph`.  Streams one
/// subject at a time — memory is O(largest subject), not O(dataset).
///
/// Value rules:
/// - `xsd:string` → plain JSON string (no `@type`)
/// - `xsd:boolean` → native JSON boolean
/// - `xsd:integer`/`xsd:long`/etc. → `{"@value": n, "@type": "xsd:integer"}`
/// - `xsd:decimal` → `{"@value": "...", "@type": "xsd:decimal"}`
/// - `xsd:double` → `{"@value": n, "@type": "xsd:double"}`
/// - Language strings → `{"@value": "...", "@language": "..."}`
/// - Other typed literals → `{"@value": "...", "@type": "..."}`
/// - Refs → `{"@id": "iri"}`
/// - Single-cardinality properties are unwrapped (not in `[]`)
pub fn export_graph_jsonld<W: Write>(
    store: &Arc<BinaryIndexStore>,
    config: &ExportConfig<'_>,
    prefixes: &PrefixMap,
    writer: &mut W,
) -> io::Result<ExportStats> {
    let branch_ref = match store.branch_for_order(config.g_id, RunSortOrder::Spot) {
        Some(b) => b,
        None => return Ok(ExportStats::default()),
    };
    let branch = Arc::clone(branch_ref);

    let filter = BinaryFilter::default();
    let projection = ColumnProjection {
        output: ColumnSet::CORE,
        internal: ColumnSet::EMPTY,
    };

    let mut cursor = BinaryCursor::scan_all(
        Arc::clone(store),
        RunSortOrder::Spot,
        branch,
        filter,
        projection,
    );
    let ephemeral_preds = apply_time_travel(&mut cursor, config, store);
    let resolver = ExportResolver::new(store, config.dict_novelty, &ephemeral_preds);

    let mut stats = ExportStats::default();

    // Accumulate properties for the current subject.
    // Key = predicate IRI, Value = list of JSON-LD values.
    let mut current_subject: Option<String> = None;
    let mut current_props: Vec<(String, Vec<serde_json::Value>)> = Vec::new();
    let mut first_node = true;

    while let Some(batch) = cursor.next_batch()? {
        for row in 0..batch.row_count {
            let s_id = batch.s_id.get(row);
            let p_id = batch.p_id.get_or(row, 0);
            let o_type = batch.o_type.get_or(row, 0);
            let o_key = batch.o_key.get(row);

            let s_iri = resolver.resolve_subject_iri(s_id)?;
            let p_iri = match resolver.resolve_predicate_iri(p_id) {
                Some(p) => p.to_string(),
                None => {
                    stats.rows_skipped += 1;
                    continue;
                }
            };
            let value = resolver.decode_value(o_type, o_key, p_id, config.g_id)?;
            if matches!(value, FlakeValue::Null) {
                stats.rows_skipped += 1;
                continue;
            }

            // Convert to JSON-LD value
            let jval = flake_to_jsonld(&value, store, o_type, prefixes);

            // Check if we've moved to a new subject
            let same_subject = current_subject.as_deref() == Some(&s_iri);
            if !same_subject {
                // Flush previous subject
                if let Some(ref subj_iri) = current_subject {
                    write_jsonld_node(writer, subj_iri, &current_props, prefixes, first_node)?;
                    first_node = false;
                }
                current_subject = Some(s_iri);
                current_props.clear();
            }

            // Append value to the right predicate bucket
            let compact_p = compact_iri(&p_iri, prefixes);
            if let Some(entry) = current_props.iter_mut().find(|(k, _)| *k == compact_p) {
                entry.1.push(jval);
            } else {
                current_props.push((compact_p, vec![jval]));
            }

            stats.triples_written += 1;
        }
    }

    // Flush last subject
    if let Some(ref subj_iri) = current_subject {
        write_jsonld_node(writer, subj_iri, &current_props, prefixes, first_node)?;
    }

    Ok(stats)
}

/// Write the JSON-LD document header: `{"@context": {...}, "@graph": [`
pub fn write_jsonld_header<W: Write>(prefixes: &PrefixMap, writer: &mut W) -> io::Result<()> {
    writer.write_all(b"{\n  \"@context\": ")?;

    // Build context object
    let mut ctx = serde_json::Map::new();
    // Sort alphabetically for deterministic output
    let mut sorted: Vec<(&str, &str)> = prefixes.iter().collect();
    sorted.sort_by_key(|(p, _)| *p);
    for (prefix, ns) in sorted {
        ctx.insert(
            prefix.to_string(),
            serde_json::Value::String(ns.to_string()),
        );
    }

    let ctx_json =
        serde_json::to_string_pretty(&serde_json::Value::Object(ctx)).unwrap_or_default();
    // Indent context to match nesting
    for (i, line) in ctx_json.lines().enumerate() {
        if i > 0 {
            writer.write_all(b"\n  ")?;
        }
        writer.write_all(line.as_bytes())?;
    }

    writer.write_all(b",\n  \"@graph\": [")?;
    Ok(())
}

/// Write the JSON-LD document footer: `]}`
pub fn write_jsonld_footer<W: Write>(writer: &mut W) -> io::Result<()> {
    writer.write_all(b"\n  ]\n}\n")
}

/// Write a single JSON-LD node object for a subject.
fn write_jsonld_node<W: Write>(
    writer: &mut W,
    subject_iri: &str,
    props: &[(String, Vec<serde_json::Value>)],
    prefixes: &PrefixMap,
    first: bool,
) -> io::Result<()> {
    if !first {
        writer.write_all(b",")?;
    }
    writer.write_all(b"\n    {")?;

    // @id
    let compact_id = compact_iri(subject_iri, prefixes);
    write!(writer, "\"@id\": \"{}\"", escape_json_string(&compact_id))?;

    // Separate @type from other properties
    let rdf_type = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
    let rdf_type_compact = compact_iri(rdf_type, prefixes);

    for (pred, values) in props {
        // rdf:type gets special @type treatment
        if *pred == rdf_type_compact || *pred == rdf_type {
            writer.write_all(b", \"@type\": ")?;
            // Extract @id values for types
            let type_iris: Vec<&str> = values
                .iter()
                .filter_map(|v| {
                    v.as_object()
                        .and_then(|o| o.get("@id"))
                        .and_then(|id| id.as_str())
                })
                .collect();
            if type_iris.len() == 1 {
                write!(writer, "\"{}\"", escape_json_string(type_iris[0]))?;
            } else {
                writer.write_all(b"[")?;
                for (i, t) in type_iris.iter().enumerate() {
                    if i > 0 {
                        writer.write_all(b", ")?;
                    }
                    write!(writer, "\"{}\"", escape_json_string(t))?;
                }
                writer.write_all(b"]")?;
            }
            continue;
        }

        write!(writer, ", \"{}\": ", escape_json_string(pred))?;

        if values.len() == 1 {
            // Single cardinality — unwrap
            let json_str = serde_json::to_string(&values[0]).unwrap_or_default();
            writer.write_all(json_str.as_bytes())?;
        } else {
            // Multi cardinality — array
            writer.write_all(b"[")?;
            for (i, v) in values.iter().enumerate() {
                if i > 0 {
                    writer.write_all(b", ")?;
                }
                let json_str = serde_json::to_string(v).unwrap_or_default();
                writer.write_all(json_str.as_bytes())?;
            }
            writer.write_all(b"]")?;
        }
    }

    writer.write_all(b"}")
}

/// Convert a FlakeValue to a JSON-LD value representation.
fn flake_to_jsonld(
    value: &FlakeValue,
    store: &BinaryIndexStore,
    o_type: u16,
    prefixes: &PrefixMap,
) -> serde_json::Value {
    match value {
        FlakeValue::Ref(sid) => {
            let iri = store
                .sid_to_iri(sid)
                .unwrap_or_else(|| format!("_:unknown_{sid}"));
            let compact = compact_iri(&iri, prefixes);
            serde_json::json!({ "@id": compact })
        }

        FlakeValue::String(s) => {
            // Language-tagged string
            if let Some(lang) = store.resolve_lang_tag(o_type) {
                return serde_json::json!({ "@value": s, "@language": lang });
            }

            // Resolve datatype
            let dt_iri = resolve_datatype_iri(store, o_type);
            match dt_iri.as_deref() {
                None | Some(xsd::STRING) => {
                    // Plain string — no @type needed
                    serde_json::Value::String(s.clone())
                }
                Some(dt) => {
                    let compact_dt = compact_iri(dt, prefixes);
                    serde_json::json!({ "@value": s, "@type": compact_dt })
                }
            }
        }

        FlakeValue::Boolean(b) => {
            // Native JSON boolean
            serde_json::Value::Bool(*b)
        }

        FlakeValue::Long(n) => {
            let dt = resolve_datatype_iri(store, o_type).unwrap_or_else(|| xsd::LONG.to_string());
            let compact_dt = compact_iri(&dt, prefixes);
            serde_json::json!({ "@value": n, "@type": compact_dt })
        }

        FlakeValue::Double(f) => {
            let dt = resolve_datatype_iri(store, o_type).unwrap_or_else(|| xsd::DOUBLE.to_string());
            let compact_dt = compact_iri(&dt, prefixes);
            if f.is_finite() {
                serde_json::json!({ "@value": f, "@type": compact_dt })
            } else {
                // NaN/Infinity must be string-encoded
                let lexical = if f.is_nan() {
                    "NaN"
                } else if f.is_sign_positive() {
                    "INF"
                } else {
                    "-INF"
                };
                serde_json::json!({ "@value": lexical, "@type": compact_dt })
            }
        }

        FlakeValue::BigInt(n) => {
            let dt =
                resolve_datatype_iri(store, o_type).unwrap_or_else(|| xsd::INTEGER.to_string());
            let compact_dt = compact_iri(&dt, prefixes);
            // Try to fit in i64 for native JSON number
            let s = n.to_string();
            if let Ok(i) = s.parse::<i64>() {
                serde_json::json!({ "@value": i, "@type": compact_dt })
            } else {
                serde_json::json!({ "@value": s, "@type": compact_dt })
            }
        }

        FlakeValue::Decimal(d) => {
            let dt =
                resolve_datatype_iri(store, o_type).unwrap_or_else(|| xsd::DECIMAL.to_string());
            let compact_dt = compact_iri(&dt, prefixes);
            serde_json::json!({ "@value": d.to_string(), "@type": compact_dt })
        }

        // Temporal types
        FlakeValue::DateTime(v) => {
            typed_value_display(v.as_ref(), store, o_type, xsd::DATE_TIME, prefixes)
        }
        FlakeValue::Date(v) => typed_value_display(v.as_ref(), store, o_type, xsd::DATE, prefixes),
        FlakeValue::Time(v) => typed_value_display(v.as_ref(), store, o_type, xsd::TIME, prefixes),
        FlakeValue::GYear(v) => {
            typed_value_display(v.as_ref(), store, o_type, xsd::G_YEAR, prefixes)
        }
        FlakeValue::GYearMonth(v) => {
            typed_value_display(v.as_ref(), store, o_type, xsd::G_YEAR_MONTH, prefixes)
        }
        FlakeValue::GMonth(v) => {
            typed_value_display(v.as_ref(), store, o_type, xsd::G_MONTH, prefixes)
        }
        FlakeValue::GDay(v) => typed_value_display(v.as_ref(), store, o_type, xsd::G_DAY, prefixes),
        FlakeValue::GMonthDay(v) => typed_value_display(
            v.as_ref(),
            store,
            o_type,
            "http://www.w3.org/2001/XMLSchema#gMonthDay",
            prefixes,
        ),
        FlakeValue::YearMonthDuration(v) => typed_value_display(
            v.as_ref(),
            store,
            o_type,
            "http://www.w3.org/2001/XMLSchema#yearMonthDuration",
            prefixes,
        ),
        FlakeValue::DayTimeDuration(v) => typed_value_display(
            v.as_ref(),
            store,
            o_type,
            "http://www.w3.org/2001/XMLSchema#dayTimeDuration",
            prefixes,
        ),
        FlakeValue::Duration(v) => typed_value_display(
            v.as_ref(),
            store,
            o_type,
            "http://www.w3.org/2001/XMLSchema#duration",
            prefixes,
        ),

        // Extension types
        FlakeValue::Json(s) => {
            let dt = resolve_datatype_iri(store, o_type)
                .unwrap_or_else(|| "http://www.w3.org/1999/02/22-rdf-syntax-ns#JSON".to_string());
            let compact_dt = compact_iri(&dt, prefixes);
            // Try to parse the JSON string into a native JSON value
            if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(s) {
                serde_json::json!({ "@value": parsed, "@type": compact_dt })
            } else {
                serde_json::json!({ "@value": s, "@type": compact_dt })
            }
        }

        FlakeValue::Vector(v) => {
            let dt = resolve_datatype_iri(store, o_type)
                .unwrap_or_else(|| "https://ns.flur.ee/db#vector".to_string());
            let compact_dt = compact_iri(&dt, prefixes);
            serde_json::json!({ "@value": v, "@type": compact_dt })
        }

        FlakeValue::GeoPoint(bits) => {
            let dt = resolve_datatype_iri(store, o_type)
                .unwrap_or_else(|| "http://www.opengis.net/ont/geosparql#wktLiteral".to_string());
            let compact_dt = compact_iri(&dt, prefixes);
            serde_json::json!({ "@value": bits.to_string(), "@type": compact_dt })
        }

        FlakeValue::Null => serde_json::Value::Null,
    }
}

/// Helper: create a `{"@value": "display", "@type": "dt"}` JSON-LD value.
fn typed_value_display<T: std::fmt::Display>(
    value: &T,
    store: &BinaryIndexStore,
    o_type: u16,
    fallback_dt: &str,
    prefixes: &PrefixMap,
) -> serde_json::Value {
    let dt = resolve_datatype_iri(store, o_type).unwrap_or_else(|| fallback_dt.to_string());
    let compact_dt = compact_iri(&dt, prefixes);
    serde_json::json!({ "@value": value.to_string(), "@type": compact_dt })
}

/// Compact an IRI using the prefix map, falling back to the full IRI.
fn compact_iri(iri: &str, prefixes: &PrefixMap) -> String {
    // Blank nodes pass through
    if iri.starts_with("_:") {
        return iri.to_string();
    }
    prefixes.compact(iri).unwrap_or_else(|| iri.to_string())
}

/// Escape a string for use as a JSON string value (without outer quotes).
fn escape_json_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c.is_control() => {
                let cp = c as u32;
                out.push_str(&format!("\\u{cp:04X}"));
            }
            c => out.push(c),
        }
    }
    out
}

/// Stream triples/quads from the SPOT index of one graph to `writer`.
///
/// Includes novelty overlay and respects `to_t` for time-travel export.
pub fn export_graph_ntriples<W: Write>(
    store: &Arc<BinaryIndexStore>,
    config: &ExportConfig<'_>,
    writer: &mut W,
) -> io::Result<ExportStats> {
    let branch_ref = match store.branch_for_order(config.g_id, RunSortOrder::Spot) {
        Some(b) => b,
        None => return Ok(ExportStats::default()), // no data for this graph
    };
    let branch = Arc::clone(branch_ref);

    let filter = BinaryFilter::default();
    let projection = ColumnProjection {
        output: ColumnSet::CORE,
        internal: ColumnSet::EMPTY,
    };

    let mut cursor = BinaryCursor::scan_all(
        Arc::clone(store),
        RunSortOrder::Spot,
        branch,
        filter,
        projection,
    );
    let ephemeral_preds = apply_time_travel(&mut cursor, config, store);
    let resolver = ExportResolver::new(store, config.dict_novelty, &ephemeral_preds);

    let mut stats = ExportStats::default();
    let graph_term = config.graph_iri.as_deref().map(|iri| {
        let mut buf = String::with_capacity(iri.len() + 2);
        buf.push('<');
        escape_iri_into(&mut buf, iri);
        buf.push('>');
        buf
    });

    while let Some(batch) = cursor.next_batch()? {
        write_batch(
            &resolver,
            &batch,
            config.g_id,
            graph_term.as_deref(),
            &mut stats,
            writer,
        )?;
    }

    Ok(stats)
}

// ---------------------------------------------------------------------------
// Batch → N-Triples / N-Quads
// ---------------------------------------------------------------------------

fn write_batch<W: Write>(
    resolver: &ExportResolver,
    batch: &ColumnBatch,
    g_id: GraphId,
    graph_term: Option<&str>,
    stats: &mut ExportStats,
    writer: &mut W,
) -> io::Result<()> {
    for row in 0..batch.row_count {
        let s_id = batch.s_id.get(row);
        let p_id = batch.p_id.get_or(row, 0);
        let o_type = batch.o_type.get_or(row, 0);
        let o_key = batch.o_key.get(row);

        // Resolve subject
        let s_iri = resolver.resolve_subject_iri(s_id)?;

        // Resolve predicate
        let p_iri = match resolver.resolve_predicate_iri(p_id) {
            Some(p) => p,
            None => {
                stats.rows_skipped += 1;
                continue;
            }
        };

        // Resolve object value
        let value = resolver.decode_value(o_type, o_key, p_id, g_id)?;
        if matches!(value, FlakeValue::Null) {
            stats.rows_skipped += 1;
            continue;
        }

        // Write subject
        write_iri_or_bnode(writer, &s_iri)?;
        writer.write_all(b" ")?;

        // Write predicate (always an IRI)
        writer.write_all(b"<")?;
        write_escaped_iri(writer, &p_iri)?;
        writer.write_all(b"> ")?;

        // Write object
        write_object(writer, &value, resolver.store, o_type)?;

        // Write optional graph term (N-Quads)
        if let Some(g) = graph_term {
            writer.write_all(b" ")?;
            writer.write_all(g.as_bytes())?;
        }

        writer.write_all(b" .\n")?;
        stats.triples_written += 1;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Term formatting
// ---------------------------------------------------------------------------

/// Write a subject term: `<iri>` or `_:bnode`.
fn write_iri_or_bnode<W: Write>(w: &mut W, iri: &str) -> io::Result<()> {
    if iri.starts_with("_:") {
        // Blank node — emit as-is (no angle brackets)
        w.write_all(iri.as_bytes())
    } else {
        w.write_all(b"<")?;
        write_escaped_iri(w, iri)?;
        w.write_all(b">")
    }
}

/// Write an object value as an N-Triples term.
fn write_object<W: Write>(
    w: &mut W,
    value: &FlakeValue,
    store: &BinaryIndexStore,
    o_type: u16,
) -> io::Result<()> {
    match value {
        FlakeValue::Ref(sid) => {
            let iri = store
                .sid_to_iri(sid)
                .unwrap_or_else(|| format!("_:unknown_{sid}"));
            write_iri_or_bnode(w, &iri)
        }

        FlakeValue::String(s) => {
            // Check for language tag first (takes precedence over datatype)
            if let Some(lang) = store.resolve_lang_tag(o_type) {
                w.write_all(b"\"")?;
                write_escaped_ntriples_string(w, s)?;
                w.write_all(b"\"@")?;
                w.write_all(lang.as_bytes())?;
                return Ok(());
            }

            // Resolve datatype; omit ^^<xsd:string> (implicit)
            let dt_iri = resolve_datatype_iri(store, o_type);
            w.write_all(b"\"")?;
            write_escaped_ntriples_string(w, s)?;
            w.write_all(b"\"")?;
            if let Some(dt) = &dt_iri {
                if *dt != xsd::STRING {
                    w.write_all(b"^^<")?;
                    write_escaped_iri(w, dt)?;
                    w.write_all(b">")?;
                }
            }
            Ok(())
        }

        FlakeValue::Boolean(b) => {
            write_typed_literal(w, if *b { "true" } else { "false" }, xsd::BOOLEAN)
        }
        FlakeValue::Long(n) => write_typed_literal(
            w,
            &n.to_string(),
            &resolve_datatype_iri(store, o_type).unwrap_or_else(|| xsd::LONG.to_string()),
        ),
        FlakeValue::Double(f) => {
            // N-Triples canonical form for double
            let lexical = if f.is_infinite() {
                if f.is_sign_positive() {
                    "INF".to_string()
                } else {
                    "-INF".to_string()
                }
            } else if f.is_nan() {
                "NaN".to_string()
            } else {
                format!("{f:E}")
            };
            write_typed_literal(w, &lexical, xsd::DOUBLE)
        }
        FlakeValue::BigInt(n) => write_typed_literal(
            w,
            &n.to_string(),
            &resolve_datatype_iri(store, o_type).unwrap_or_else(|| xsd::INTEGER.to_string()),
        ),
        FlakeValue::Decimal(d) => write_typed_literal(
            w,
            &d.to_string(),
            &resolve_datatype_iri(store, o_type)
                .unwrap_or_else(|| "http://www.w3.org/2001/XMLSchema#decimal".to_string()),
        ),

        // Temporal types — use Display for canonical lexical form
        FlakeValue::DateTime(v) => write_typed_literal_display(
            w,
            v.as_ref(),
            store,
            o_type,
            "http://www.w3.org/2001/XMLSchema#dateTime",
        ),
        FlakeValue::Date(v) => write_typed_literal_display(
            w,
            v.as_ref(),
            store,
            o_type,
            "http://www.w3.org/2001/XMLSchema#date",
        ),
        FlakeValue::Time(v) => write_typed_literal_display(
            w,
            v.as_ref(),
            store,
            o_type,
            "http://www.w3.org/2001/XMLSchema#time",
        ),
        FlakeValue::GYear(v) => write_typed_literal_display(
            w,
            v.as_ref(),
            store,
            o_type,
            "http://www.w3.org/2001/XMLSchema#gYear",
        ),
        FlakeValue::GYearMonth(v) => write_typed_literal_display(
            w,
            v.as_ref(),
            store,
            o_type,
            "http://www.w3.org/2001/XMLSchema#gYearMonth",
        ),
        FlakeValue::GMonth(v) => write_typed_literal_display(
            w,
            v.as_ref(),
            store,
            o_type,
            "http://www.w3.org/2001/XMLSchema#gMonth",
        ),
        FlakeValue::GDay(v) => write_typed_literal_display(
            w,
            v.as_ref(),
            store,
            o_type,
            "http://www.w3.org/2001/XMLSchema#gDay",
        ),
        FlakeValue::GMonthDay(v) => write_typed_literal_display(
            w,
            v.as_ref(),
            store,
            o_type,
            "http://www.w3.org/2001/XMLSchema#gMonthDay",
        ),
        FlakeValue::YearMonthDuration(v) => write_typed_literal_display(
            w,
            v.as_ref(),
            store,
            o_type,
            "http://www.w3.org/2001/XMLSchema#yearMonthDuration",
        ),
        FlakeValue::DayTimeDuration(v) => write_typed_literal_display(
            w,
            v.as_ref(),
            store,
            o_type,
            "http://www.w3.org/2001/XMLSchema#dayTimeDuration",
        ),
        FlakeValue::Duration(v) => write_typed_literal_display(
            w,
            v.as_ref(),
            store,
            o_type,
            "http://www.w3.org/2001/XMLSchema#duration",
        ),

        // Extension types
        FlakeValue::Json(s) => {
            let dt = resolve_datatype_iri(store, o_type)
                .unwrap_or_else(|| "http://www.w3.org/1999/02/22-rdf-syntax-ns#JSON".to_string());
            w.write_all(b"\"")?;
            write_escaped_ntriples_string(w, s)?;
            w.write_all(b"\"^^<")?;
            write_escaped_iri(w, &dt)?;
            w.write_all(b">")
        }
        FlakeValue::Vector(v) => {
            let dt = resolve_datatype_iri(store, o_type)
                .unwrap_or_else(|| "https://ns.flur.ee/db#vector".to_string());
            // Serialize as JSON array string
            let json = serde_json::to_string(v).unwrap_or_else(|_| "[]".to_string());
            w.write_all(b"\"")?;
            write_escaped_ntriples_string(w, &json)?;
            w.write_all(b"\"^^<")?;
            write_escaped_iri(w, &dt)?;
            w.write_all(b">")
        }
        FlakeValue::GeoPoint(bits) => {
            let dt = resolve_datatype_iri(store, o_type)
                .unwrap_or_else(|| "http://www.opengis.net/ont/geosparql#wktLiteral".to_string());
            let wkt = bits.to_string(); // "POINT(lng lat)"
            w.write_all(b"\"")?;
            write_escaped_ntriples_string(w, &wkt)?;
            w.write_all(b"\"^^<")?;
            write_escaped_iri(w, &dt)?;
            w.write_all(b">")
        }

        FlakeValue::Null => Ok(()), // should have been filtered above
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Resolve the datatype IRI for an `o_type` code.
fn resolve_datatype_iri(store: &BinaryIndexStore, o_type: u16) -> Option<String> {
    store
        .resolve_datatype_sid(o_type)
        .and_then(|sid| store.sid_to_iri(&sid))
}

/// Write `"lexical"^^<datatype_iri>`.
fn write_typed_literal<W: Write>(w: &mut W, lexical: &str, datatype_iri: &str) -> io::Result<()> {
    w.write_all(b"\"")?;
    write_escaped_ntriples_string(w, lexical)?;
    w.write_all(b"\"^^<")?;
    write_escaped_iri(w, datatype_iri)?;
    w.write_all(b">")
}

/// Write a typed literal using the Display impl for the lexical form.
fn write_typed_literal_display<W: Write, T: std::fmt::Display>(
    w: &mut W,
    value: &T,
    store: &BinaryIndexStore,
    o_type: u16,
    fallback_dt: &str,
) -> io::Result<()> {
    let lexical = value.to_string();
    let dt = resolve_datatype_iri(store, o_type).unwrap_or_else(|| fallback_dt.to_string());
    write_typed_literal(w, &lexical, &dt)
}

// ---------------------------------------------------------------------------
// N-Triples escaping (W3C compliant)
// ---------------------------------------------------------------------------

/// Write an N-Triples-escaped string to `w`.
///
/// Escapes: `\` `"` `\n` `\r` `\t` and control chars (U+0000..U+001F, U+007F..U+009F)
/// via `\uXXXX`.
fn write_escaped_ntriples_string<W: Write>(w: &mut W, s: &str) -> io::Result<()> {
    for ch in s.chars() {
        match ch {
            '\\' => w.write_all(b"\\\\")?,
            '"' => w.write_all(b"\\\"")?,
            '\n' => w.write_all(b"\\n")?,
            '\r' => w.write_all(b"\\r")?,
            '\t' => w.write_all(b"\\t")?,
            c if c.is_control() => {
                let cp = c as u32;
                if cp <= 0xFFFF {
                    write!(w, "\\u{cp:04X}")?;
                } else {
                    write!(w, "\\U{cp:08X}")?;
                }
            }
            c => {
                let mut buf = [0u8; 4];
                let encoded = c.encode_utf8(&mut buf);
                w.write_all(encoded.as_bytes())?;
            }
        }
    }
    Ok(())
}

/// Write an IRI with escaping per N-Triples/Turtle `IRIREF` grammar.
///
/// Disallowed characters in `IRIREF`:
/// - ASCII control and space: U+0000..=U+0020
/// - DEL + C1 controls: U+007F..=U+009F
/// - Punctuation: `<`, `>`, `"`, `{`, `}`, `|`, `^`, `` ` ``, `\`
///
/// We percent-encode the UTF-8 bytes of these characters to ensure the output
/// remains syntactically valid RDF, even if the stored IRI contains invalid
/// characters.
pub fn write_escaped_iri<W: Write>(w: &mut W, iri: &str) -> io::Result<()> {
    for ch in iri.chars() {
        let cp = ch as u32;
        let is_forbidden_range = cp <= 0x20 || (0x7F..=0x9F).contains(&cp);
        let is_forbidden_punct = matches!(ch, '<' | '>' | '"' | '{' | '}' | '|' | '^' | '`' | '\\');

        if is_forbidden_range || is_forbidden_punct {
            let mut buf = [0u8; 4];
            let encoded = ch.encode_utf8(&mut buf);
            for &b in encoded.as_bytes() {
                write!(w, "%{b:02X}")?;
            }
        } else {
            let mut buf = [0u8; 4];
            let encoded = ch.encode_utf8(&mut buf);
            w.write_all(encoded.as_bytes())?;
        }
    }
    Ok(())
}

/// Escape an IRI into a pre-allocated String (for graph term caching).
fn escape_iri_into(out: &mut String, iri: &str) {
    for ch in iri.chars() {
        let cp = ch as u32;
        let is_forbidden_range = cp <= 0x20 || (0x7F..=0x9F).contains(&cp);
        let is_forbidden_punct = matches!(ch, '<' | '>' | '"' | '{' | '}' | '|' | '^' | '`' | '\\');

        if is_forbidden_range || is_forbidden_punct {
            let mut buf = [0u8; 4];
            let encoded = ch.encode_utf8(&mut buf);
            for &b in encoded.as_bytes() {
                out.push('%');
                out.push_str(&format!("{b:02X}"));
            }
        } else {
            out.push(ch);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_escape_ntriples_string() {
        let mut buf = Vec::new();
        write_escaped_ntriples_string(&mut buf, "hello \"world\"\nline2\\end").unwrap();
        assert_eq!(
            String::from_utf8(buf).unwrap(),
            "hello \\\"world\\\"\\nline2\\\\end"
        );
    }

    #[test]
    fn test_escape_ntriples_control_chars() {
        let mut buf = Vec::new();
        write_escaped_ntriples_string(&mut buf, "a\x00b\x1Fc").unwrap();
        assert_eq!(String::from_utf8(buf).unwrap(), "a\\u0000b\\u001Fc");
    }

    #[test]
    fn test_escape_iri() {
        let mut buf = Vec::new();
        write_escaped_iri(&mut buf, "http://example.org/foo>bar").unwrap();
        assert_eq!(
            String::from_utf8(buf).unwrap(),
            "http://example.org/foo%3Ebar"
        );

        let mut buf = Vec::new();
        write_escaped_iri(&mut buf, "http://example.org/a\\b<c\"d").unwrap();
        assert_eq!(
            String::from_utf8(buf).unwrap(),
            "http://example.org/a%5Cb%3Cc%22d"
        );

        let mut buf = Vec::new();
        write_escaped_iri(&mut buf, "http://example.org/a b\tc").unwrap();
        assert_eq!(
            String::from_utf8(buf).unwrap(),
            "http://example.org/a%20b%09c"
        );
    }

    #[test]
    fn test_write_iri_or_bnode() {
        let mut buf = Vec::new();
        write_iri_or_bnode(&mut buf, "http://example.org/alice").unwrap();
        assert_eq!(
            String::from_utf8(buf).unwrap(),
            "<http://example.org/alice>"
        );

        let mut buf = Vec::new();
        write_iri_or_bnode(&mut buf, "_:b123").unwrap();
        assert_eq!(String::from_utf8(buf).unwrap(), "_:b123");
    }

    #[test]
    fn test_prefix_map_from_context() {
        let ctx = serde_json::json!({
            "ex": "http://example.org/",
            "schema": "http://schema.org/",
            "@base": "http://ignored.org/"
        });
        let pm = PrefixMap::from_context(&ctx);
        assert_eq!(
            pm.compact("http://example.org/alice"),
            Some("ex:alice".to_string())
        );
        assert_eq!(
            pm.compact("http://schema.org/name"),
            Some("schema:name".to_string())
        );
        assert_eq!(pm.compact("http://other.org/foo"), None);
    }

    #[test]
    fn test_prefix_map_longest_match() {
        let ctx = serde_json::json!({
            "ex": "http://example.org/",
            "exn": "http://example.org/ns/"
        });
        let pm = PrefixMap::from_context(&ctx);
        // Should match the longer prefix
        assert_eq!(
            pm.compact("http://example.org/ns/thing"),
            Some("exn:thing".to_string())
        );
        assert_eq!(
            pm.compact("http://example.org/other"),
            Some("ex:other".to_string())
        );
    }

    #[test]
    fn test_prefix_map_invalid_local_name() {
        let ctx = serde_json::json!({
            "ex": "http://example.org/"
        });
        let pm = PrefixMap::from_context(&ctx);
        // Spaces and special chars → falls back to full IRI
        assert_eq!(pm.compact("http://example.org/has space"), None);
        assert_eq!(pm.compact("http://example.org/has:colon"), None);
        // Leading/trailing dots invalid
        assert_eq!(pm.compact("http://example.org/.hidden"), None);
    }

    #[test]
    fn test_write_prefix_declarations() {
        let ctx = serde_json::json!({
            "ex": "http://example.org/",
            "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
        });
        let pm = PrefixMap::from_context(&ctx);
        let mut buf = Vec::new();
        write_prefix_declarations(&pm, &mut buf).unwrap();
        let output = String::from_utf8(buf).unwrap();
        assert!(output.contains("@prefix ex: <http://example.org/> ."));
        assert!(output.contains("@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> ."));
    }

    #[test]
    fn test_write_turtle_iri() {
        let ctx = serde_json::json!({"ex": "http://example.org/"});
        let pm = PrefixMap::from_context(&ctx);

        let mut buf = Vec::new();
        write_turtle_iri(&mut buf, "http://example.org/alice", &pm).unwrap();
        assert_eq!(String::from_utf8(buf).unwrap(), "ex:alice");

        let mut buf = Vec::new();
        write_turtle_iri(&mut buf, "http://other.org/bob", &pm).unwrap();
        assert_eq!(String::from_utf8(buf).unwrap(), "<http://other.org/bob>");
    }

    #[test]
    fn test_compact_iri() {
        let ctx = serde_json::json!({"ex": "http://example.org/"});
        let pm = PrefixMap::from_context(&ctx);
        assert_eq!(compact_iri("http://example.org/alice", &pm), "ex:alice");
        assert_eq!(
            compact_iri("http://other.org/bob", &pm),
            "http://other.org/bob"
        );
        // Blank nodes pass through
        assert_eq!(compact_iri("_:b42", &pm), "_:b42");
    }

    #[test]
    fn test_escape_json_string() {
        assert_eq!(escape_json_string("hello"), "hello");
        assert_eq!(escape_json_string("a\"b"), "a\\\"b");
        assert_eq!(escape_json_string("a\\b"), "a\\\\b");
        assert_eq!(escape_json_string("a\nb\tc"), "a\\nb\\tc");
    }

    #[test]
    fn test_write_jsonld_header_footer() {
        let ctx = serde_json::json!({"ex": "http://example.org/"});
        let pm = PrefixMap::from_context(&ctx);

        let mut buf = Vec::new();
        write_jsonld_header(&pm, &mut buf).unwrap();
        write_jsonld_footer(&mut buf).unwrap();
        let output = String::from_utf8(buf).unwrap();

        assert!(output.contains("\"@context\""));
        assert!(output.contains("\"ex\": \"http://example.org/\""));
        assert!(output.contains("\"@graph\": ["));
        assert!(output.ends_with("  ]\n}\n"));
    }

    #[test]
    fn test_write_jsonld_node_single_value() {
        let ctx = serde_json::json!({"ex": "http://example.org/"});
        let pm = PrefixMap::from_context(&ctx);

        let mut buf = Vec::new();
        let props = vec![("ex:name".to_string(), vec![serde_json::json!("Alice")])];
        write_jsonld_node(&mut buf, "http://example.org/alice", &props, &pm, true).unwrap();
        let output = String::from_utf8(buf).unwrap();

        assert!(output.contains("\"@id\": \"ex:alice\""));
        // Single value should NOT be wrapped in array
        assert!(output.contains("\"ex:name\": \"Alice\""));
        assert!(!output.contains("[\"Alice\"]"));
    }

    #[test]
    fn test_write_jsonld_node_multi_value() {
        let ctx = serde_json::json!({"ex": "http://example.org/"});
        let pm = PrefixMap::from_context(&ctx);

        let mut buf = Vec::new();
        let props = vec![(
            "ex:tag".to_string(),
            vec![serde_json::json!("a"), serde_json::json!("b")],
        )];
        write_jsonld_node(&mut buf, "http://example.org/thing", &props, &pm, true).unwrap();
        let output = String::from_utf8(buf).unwrap();

        // Multi-value SHOULD be wrapped in array
        assert!(output.contains("\"ex:tag\": [\"a\", \"b\"]"));
    }

    #[test]
    fn test_write_jsonld_node_rdf_type() {
        let ctx = serde_json::json!({"ex": "http://example.org/"});
        let pm = PrefixMap::from_context(&ctx);

        let rdf_type = compact_iri("http://www.w3.org/1999/02/22-rdf-syntax-ns#type", &pm);
        let mut buf = Vec::new();
        let props = vec![(rdf_type, vec![serde_json::json!({"@id": "ex:Person"})])];
        write_jsonld_node(&mut buf, "http://example.org/alice", &props, &pm, true).unwrap();
        let output = String::from_utf8(buf).unwrap();

        // rdf:type should become @type with unwrapped single value
        assert!(output.contains("\"@type\": \"ex:Person\""));
        assert!(!output.contains("rdf:type"));
    }
}
