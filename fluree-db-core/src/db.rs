//! Database struct
//!
//! The `LedgerSnapshot` struct represents a database value at a specific point in time.
//! It is a pure value type — no storage backend reference.

use crate::content_id::ContentId;
use crate::content_kind::ContentKind;
use crate::error::{Error, Result};
use crate::graph_registry::GraphRegistry;
use crate::ids::GraphId;
use crate::index_schema::IndexSchema;
use crate::index_stats::IndexStats;
use crate::namespaces::default_namespace_codes;
use crate::ns_encoding::{canonical_split, NsSplitMode};
use crate::range_provider::RangeProvider;
use crate::schema_hierarchy::SchemaHierarchy;
use crate::sid::Sid;
use crate::storage::StorageRead;
use fluree_vocab::namespaces::{EMPTY, OVERFLOW};
use once_cell::sync::OnceCell;
use std::collections::HashMap;
use std::sync::Arc;

/// Metadata for creating a database from its index root.
///
/// Bundles all the metadata fields extracted from a BinaryIndexRoot
/// for constructing a metadata-only `LedgerSnapshot`.
pub struct LedgerSnapshotMetadata {
    /// Ledger ID (e.g., "mydb:main")
    pub ledger_id: String,
    /// Current transaction time (upper bound of index coverage).
    pub t: i64,
    /// Earliest transaction time covered by this index.
    ///
    /// The index answers queries for any `target_t` in `base_t..=t` directly
    /// from persisted history (FIR6 Region 3). `target_t < base_t` is outside
    /// coverage and must fall back to overlay-only replay.
    ///
    /// `0` for full-history indexes and genesis snapshots.
    pub base_t: i64,
    /// Namespace code -> IRI prefix mapping
    pub namespace_codes: HashMap<u16, String>,
    /// Ledger-fixed split mode from the index root.
    pub ns_split_mode: NsSplitMode,
    /// Index statistics (flakes count, total size)
    pub stats: Option<IndexStats>,
    /// Schema (class/property hierarchy)
    pub schema: Option<IndexSchema>,
    /// Per-namespace max local_id watermarks from the index root
    pub subject_watermarks: Vec<u64>,
    /// Max assigned string_id from the index root
    pub string_watermark: u32,
    /// Graph IRIs from index root in raw root format.
    /// Index 0 = g_id 1 (txn-meta), index 1 = g_id 2 (config),
    /// index 2 = g_id 3 (first user graph), etc.
    /// Matches `IndexRoot.graph_iris` encoding.
    pub graph_iris: Vec<String>,
}

/// Database value at a specific point in time.
///
/// A pure value type — no storage backend reference. All I/O (loading,
/// writing) happens at the call site, not inside `Db`.
pub struct LedgerSnapshot {
    /// Ledger ID (e.g., "mydb:main")
    pub ledger_id: String,
    /// Current transaction time
    pub t: i64,
    /// Earliest transaction time covered by the underlying index.
    ///
    /// For snapshots loaded from an index root, the index answers queries
    /// for any `target_t` in `base_t..=t`. For genesis snapshots, both
    /// `base_t` and `t` are `0`.
    pub base_t: i64,
    /// Index version
    pub version: i32,

    /// Namespace code -> IRI prefix mapping.
    ///
    /// Private to enforce bimap invariant with `namespace_reverse`.
    /// Use `namespaces()` for read access, `insert_namespace_code()` for mutation.
    namespace_codes: HashMap<u16, String>,

    /// Reverse: IRI prefix -> namespace code (for O(1) canonical encode lookup).
    /// Kept in sync with `namespace_codes` by all mutation paths.
    namespace_reverse: HashMap<String, u16>,

    /// Ledger-fixed split mode for canonical IRI encoding.
    ///
    /// Determines how IRIs are split into `(prefix, suffix)` for SID encoding.
    /// Defaults to `MostGranular`. Immutable once the ledger allocates user
    /// namespace codes.
    ///
    /// Use `ns_split_mode()` for read access and `set_ns_split_mode()` for mutation.
    ns_split_mode: NsSplitMode,

    /// Index statistics (flakes count, total size)
    pub stats: Option<IndexStats>,
    /// Schema (class/property hierarchy)
    pub schema: Option<IndexSchema>,

    /// Cached schema hierarchy for reasoning (lazily computed)
    schema_hierarchy_cache: OnceCell<SchemaHierarchy>,

    /// Per-namespace max local_id watermarks from the index root.
    ///
    /// Used by `DictNovelty` to route forward lookups between the
    /// persisted dictionary tree and the novel overlay. Empty vec
    /// means "everything is novel" (genesis or old root without watermarks).
    pub subject_watermarks: Vec<u64>,

    /// Max assigned string_id from the index root. 0 = no strings indexed.
    pub string_watermark: u32,

    /// Binary range provider.
    ///
    /// When set, `range_with_overlay()` delegates to this provider.
    /// All existing range callers (reasoner, API, policy, SHACL) use this
    /// automatically.
    pub range_provider: Option<Arc<dyn RangeProvider>>,

    /// Ledger-wide graph IRI → GraphId registry.
    ///
    /// Populated from index root (via `seed_from_root_iris`) or ledger creation
    /// (via `new_for_ledger`), and updated during novelty replay
    /// (via `apply_envelope_deltas`). Provides IRI→GraphId resolution
    /// without requiring a binary index.
    pub graph_registry: GraphRegistry,
}

impl Clone for LedgerSnapshot {
    fn clone(&self) -> Self {
        Self {
            ledger_id: self.ledger_id.clone(),
            t: self.t,
            base_t: self.base_t,
            version: self.version,
            namespace_codes: self.namespace_codes.clone(),
            namespace_reverse: self.namespace_reverse.clone(),
            ns_split_mode: self.ns_split_mode,
            stats: self.stats.clone(),
            schema: self.schema.clone(),
            schema_hierarchy_cache: self.schema_hierarchy_cache.clone(),
            subject_watermarks: self.subject_watermarks.clone(),
            string_watermark: self.string_watermark,
            range_provider: self.range_provider.clone(),
            graph_registry: self.graph_registry.clone(),
        }
    }
}

impl std::fmt::Debug for LedgerSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LedgerSnapshot")
            .field("ledger_id", &self.ledger_id)
            .field("t", &self.t)
            .field("version", &self.version)
            .field(
                "range_provider",
                &self.range_provider.as_ref().map(|_| "..."),
            )
            .finish_non_exhaustive()
    }
}

/// Build reverse map from code→prefix forward map.
fn build_namespace_reverse(codes: &HashMap<u16, String>) -> HashMap<String, u16> {
    codes
        .iter()
        .map(|(&code, prefix)| (prefix.clone(), code))
        .collect()
}

impl LedgerSnapshot {
    /// Create a genesis (empty) database for a new ledger.
    ///
    /// Used when a nameservice has a commit but no index yet.
    /// The database starts at t=0 with no base data.  Queries against
    /// a genesis LedgerSnapshot return overlay (novelty) flakes only.
    pub fn genesis(ledger_id: &str) -> Self {
        let namespace_codes = default_namespace_codes();
        let namespace_reverse = build_namespace_reverse(&namespace_codes);
        Self {
            ledger_id: ledger_id.to_string(),
            t: 0,
            base_t: 0,
            version: 3,
            namespace_codes,
            namespace_reverse,
            ns_split_mode: NsSplitMode::default(),
            stats: None,
            schema: None,
            schema_hierarchy_cache: OnceCell::new(),
            subject_watermarks: Vec::new(),
            string_watermark: 0,
            range_provider: None,
            graph_registry: GraphRegistry::new_for_ledger(ledger_id),
        }
    }

    /// Create a Db from metadata only (no index roots).
    ///
    /// Used when the binary columnar index is the source of truth.
    /// The Db carries namespace codes, stats, and schema for callers
    /// that need ledger metadata, while all actual range queries go
    /// through `BinaryIndexStore` / `BinaryScanOperator`.
    pub fn new_meta(meta: LedgerSnapshotMetadata) -> Result<Self> {
        // Seed graph registry from index root graph IRIs.
        // If graph_iris is empty (old index or genesis), seed with ledger-scoped txn-meta.
        let graph_registry = if meta.graph_iris.is_empty() {
            GraphRegistry::new_for_ledger(&meta.ledger_id)
        } else {
            GraphRegistry::seed_from_root_iris(&meta.graph_iris)
                .map_err(|e| Error::invalid_index(format!("graph registry seed from root: {e}")))?
        };

        let namespace_reverse = build_namespace_reverse(&meta.namespace_codes);
        Ok(Self {
            ledger_id: meta.ledger_id,
            t: meta.t,
            base_t: meta.base_t,
            version: 3,
            namespace_codes: meta.namespace_codes,
            namespace_reverse,
            ns_split_mode: meta.ns_split_mode,
            stats: meta.stats,
            schema: meta.schema,
            schema_hierarchy_cache: OnceCell::new(),
            subject_watermarks: meta.subject_watermarks,
            string_watermark: meta.string_watermark,
            range_provider: None,
            graph_registry,
        })
    }

    /// Extract metadata from raw index root bytes (FIR6 binary format).
    pub fn from_root_bytes(bytes: &[u8]) -> Result<Self> {
        let meta = decode_fir6_metadata(bytes).map_err(|e| {
            let hint = if bytes.len() >= 4 && &bytes[0..4] == b"IRB1" {
                " (found IRB1 magic — this ledger uses an older index format; re-indexing is required)"
            } else {
                ""
            };
            Error::invalid_index(format!("index root: FIR6 decode: {e}{hint}"))
        })?;
        Self::new_meta(meta)
    }

    // FIR6 decoding lives in `decode_fir6_metadata` below.

    /// Attach a range provider for binary index queries.
    pub fn with_range_provider(mut self, provider: Arc<dyn RangeProvider>) -> Self {
        self.range_provider = Some(provider);
        self
    }

    /// Encode an IRI to a SID using this db's namespace codes.
    ///
    /// If no registered namespace prefix matches the IRI, falls back to the
    /// EMPTY namespace (code 0), storing the full IRI as the local name.
    /// This matches the transaction layer's behavior for bare-string
    /// identifiers and ensures queries return empty results (rather than
    /// erroring) for truly unknown IRIs.
    ///
    /// Use [`encode_iri_strict`](Self::encode_iri_strict) when unknown IRIs
    /// should produce an error (e.g., validating datatype IRIs in BIND
    /// expressions or resolving policy identity IRIs).
    pub fn encode_iri(&self, iri: &str) -> Option<Sid> {
        Some(self.encode_iri_inner(iri))
    }

    /// Encode an IRI to a SID, returning `None` if no registered namespace
    /// prefix matches.
    ///
    /// Unlike [`encode_iri`](Self::encode_iri), this does NOT fall back to
    /// the EMPTY namespace for unknown IRIs. Use this when the caller needs
    /// to distinguish between known and unknown namespaces (e.g., validating
    /// user-supplied IRIs in BIND expressions or policy identity resolution).
    ///
    /// Note: bare strings with no scheme (e.g., `"just-a-string"`) canonically
    /// split to the EMPTY prefix `""` and are intentionally rejected here, even
    /// though EMPTY is a registered code. This is correct — such strings are not
    /// valid IRIs in any known namespace.
    pub fn encode_iri_strict(&self, iri: &str) -> Option<Sid> {
        let sid = self.encode_iri_inner(iri);
        if sid.namespace_code == fluree_vocab::namespaces::EMPTY {
            None
        } else {
            Some(sid)
        }
    }

    /// Shared IRI → SID encoding using canonical splitting and exact-prefix lookup.
    ///
    /// Uses `canonical_split(iri, mode)` + exact-prefix lookup. If the
    /// canonical prefix is not registered, falls back to `Sid(EMPTY, iri)`.
    /// No longest-prefix-match — canonical encoding prohibits `starts_with` matching.
    fn encode_iri_inner(&self, iri: &str) -> Sid {
        let (canonical_prefix, canonical_suffix) = canonical_split(iri, self.ns_split_mode);
        if let Some(&code) = self.namespace_reverse.get(canonical_prefix) {
            return Sid::new(code, canonical_suffix);
        }
        Sid::new(EMPTY, iri)
    }

    /// Build a reverse lookup from graph Sid → GraphId.
    ///
    /// Returns `Err` if any registered graph IRI cannot be encoded (missing
    /// namespace prefix) or if two graph IRIs collide to the same Sid.
    ///
    /// **Prerequisite**: `apply_envelope_deltas()` must be called first so that
    /// namespace codes are available for `encode_iri()`.
    pub fn build_reverse_graph(&self) -> Result<HashMap<Sid, GraphId>> {
        let mut map = HashMap::new();
        for (g_id, iri) in self.graph_registry.iter_entries() {
            if g_id == 0 {
                continue;
            } // default graph has no Sid
            let sid = self.encode_iri(iri).ok_or_else(|| {
                Error::invalid_index(format!(
                    "graph IRI '{iri}' (g_id={g_id}) has no matching namespace prefix \
                     — namespace_codes may be incomplete"
                ))
            })?;
            if let Some(prev_g_id) = map.insert(sid.clone(), g_id) {
                return Err(Error::invalid_index(format!(
                    "Sid collision: graph IRIs for g_id={prev_g_id} and g_id={g_id} encode to same Sid '{sid}'"
                )));
            }
        }
        Ok(map)
    }

    /// Decode a SID to an IRI using this db's namespace codes.
    ///
    /// - `EMPTY (0)`: returns `Some(sid.name)` — name is the full IRI
    /// - `OVERFLOW (0xFFFE)`: returns `Some(sid.name)` — full IRI stored as name
    /// - Registered code: returns `Some(prefix + name)`
    /// - Unknown code: returns `None` (corruption/bug)
    pub fn decode_sid(&self, sid: &Sid) -> Option<String> {
        if sid.namespace_code == EMPTY || sid.namespace_code == OVERFLOW {
            return Some(sid.name.to_string());
        }
        self.namespace_codes
            .get(&sid.namespace_code)
            .map(|prefix| format!("{}{}", prefix, sid.name))
    }

    /// Get all registered namespace codes (code → prefix).
    pub fn namespaces(&self) -> &HashMap<u16, String> {
        &self.namespace_codes
    }

    /// Get the reverse namespace map (prefix → code) for conflict checking.
    pub fn namespace_reverse(&self) -> &HashMap<String, u16> {
        &self.namespace_reverse
    }

    /// Get the ledger's split mode for canonical IRI encoding.
    #[inline]
    pub fn ns_split_mode(&self) -> NsSplitMode {
        self.ns_split_mode
    }

    /// Set the ledger's split mode (immutable after user namespace allocation).
    ///
    /// Validates that the mode is acceptable: if user namespaces have already
    /// been allocated under a different mode, returns `Err`. Otherwise sets the mode.
    ///
    /// `commit_t` is included in error messages for diagnostics.
    pub fn set_ns_split_mode(&mut self, mode: NsSplitMode, commit_t: i64) -> Result<()> {
        if self.has_user_namespace_codes() && self.ns_split_mode != mode {
            return Err(Error::invalid_index(format!(
                "ns_split_mode conflict: commit t={} declares {:?} \
                 but ledger already has user namespaces under {:?}",
                commit_t, mode, self.ns_split_mode
            )));
        }
        self.ns_split_mode = mode;
        Ok(())
    }

    /// Insert a namespace code if not already present.
    ///
    /// Keeps the reverse map in sync. Returns `Ok(true)` if the entry was new,
    /// `Ok(false)` if the code was already registered (no-op), or `Err` if the
    /// prefix is already mapped to a different code (bimap conflict).
    pub fn insert_namespace_code(&mut self, code: u16, prefix: String) -> Result<bool> {
        if let Some(existing) = self.namespace_codes.get(&code) {
            if existing != &prefix {
                return Err(Error::invalid_index(format!(
                    "namespace bimap conflict: code {code} already maps to {existing:?} \
                     but {prefix:?} was requested"
                )));
            }
            return Ok(false);
        }
        if let Some(&existing_code) = self.namespace_reverse.get(&prefix) {
            if existing_code != code {
                return Err(Error::invalid_index(format!(
                    "namespace bimap conflict: prefix {prefix:?} already maps to code {existing_code} \
                     but code {code} was requested"
                )));
            }
            return Ok(false);
        }
        self.namespace_reverse.insert(prefix.clone(), code);
        self.namespace_codes.insert(code, prefix);
        Ok(true)
    }

    /// Apply commit envelope deltas (namespace + graph) to this snapshot.
    ///
    /// Called at commit-apply time only. Centralizes all snapshot mutations
    /// so callers don't need to know which fields to update.
    ///
    /// **Call order invariant**: namespace deltas are applied first so that
    /// `encode_iri()` can decompose graph IRIs when building reverse maps.
    ///
    /// **Cross-commit namespace conflict check**: each namespace delta entry is validated against
    /// the current table. A code that already maps to a different prefix, or
    /// a prefix that already maps to a different code, is a conflict that
    /// indicates an invalid commit chain. Returns `Err` so the caller can
    /// reject the commit rather than crashing the process.
    pub fn apply_envelope_deltas(
        &mut self,
        ns_delta: &HashMap<u16, String>,
        graph_iris: impl IntoIterator<Item = impl AsRef<str>>,
    ) -> Result<()> {
        // Validate delta against both directions of the bimap inline,
        // using the already-maintained namespace_reverse map.
        for (&code, prefix) in ns_delta {
            // code → prefix direction
            if let Some(existing) = self.namespace_codes.get(&code) {
                if existing != prefix {
                    return Err(Error::invalid_index(format!(
                        "namespace conflict: code {code} maps to {existing:?} but delta has {prefix:?}"
                    )));
                }
                continue;
            }
            // prefix → code direction
            if let Some(&existing_code) = self.namespace_reverse.get(prefix.as_str()) {
                if existing_code != code {
                    return Err(Error::invalid_index(format!(
                        "namespace conflict: prefix {prefix:?} has code {existing_code} but delta has code {code}"
                    )));
                }
                continue;
            }
            // New mapping — insert into both maps.
            self.namespace_codes.insert(code, prefix.clone());
            self.namespace_reverse.insert(prefix.clone(), code);
        }
        self.graph_registry.apply_delta(graph_iris);
        Ok(())
    }

    /// Whether user namespace codes have been allocated (any code >= USER_START).
    ///
    /// Once user codes exist, the `ns_split_mode` is locked — changing it would
    /// re-split existing IRIs differently, breaking decode consistency.
    pub fn has_user_namespace_codes(&self) -> bool {
        self.namespace_codes
            .keys()
            .any(|&code| (fluree_vocab::namespaces::USER_START..OVERFLOW).contains(&code))
    }

    /// Get the schema hierarchy for RDFS reasoning.
    pub fn schema_hierarchy(&self) -> Option<SchemaHierarchy> {
        self.schema.as_ref().map(|schema| {
            self.schema_hierarchy_cache
                .get_or_init(|| SchemaHierarchy::from_db_root_schema(schema))
                .clone()
        })
    }

    /// Get the schema epoch (transaction ID when schema was last updated).
    pub fn schema_epoch(&self) -> Option<u64> {
        self.schema.as_ref().map(|s| s.t as u64)
    }
}

// =============================================================================
// FIR6 root metadata decode (core-only, no binary-index dependency)
// =============================================================================

fn decode_fir6_metadata(bytes: &[u8]) -> std::io::Result<LedgerSnapshotMetadata> {
    use crate::stats_wire;

    // Mirror `fluree-db-binary-index` wire format:
    // [magic(4)][version(1)][flags(1)][pad(2)][index_t(8)][base_t(8)] ...
    if bytes.len() < 24 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            // Keep legacy substring "too short" for tests and callers that
            // match error text, while preserving FIR6 decode context.
            "FIR6: too short (truncated root < 24 bytes)",
        ));
    }
    if &bytes[0..4] != b"FIR6" {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "FIR6: expected magic bytes",
        ));
    }
    let version = bytes[4];
    // FIR6 version 2 adds `lang_id` to each `FulltextArenaRef` so fulltext
    // arenas can be keyed by `(g_id, p_id, lang_id)`. This helper doesn't
    // parse arena refs — it only consumes the header bits it needs — so
    // both versions are accepted here. The authoritative parser
    // (`IndexRoot::decode` in `fluree-db-binary-index`) enforces version
    // matching for the full-root deserialization path.
    if version != 1 && version != 2 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("FIR6: unsupported version {version}"),
        ));
    }
    let flags = bytes[5];

    // Optional-section flags (must match binary-index IndexRoot).
    const FLAG_HAS_STATS: u8 = 1 << 0;
    const FLAG_HAS_SCHEMA: u8 = 1 << 1;
    const FLAG_HAS_PREV_INDEX: u8 = 1 << 2;
    const FLAG_HAS_GARBAGE: u8 = 1 << 3;
    const FLAG_HAS_SKETCH: u8 = 1 << 4;

    #[inline]
    fn ensure(bytes: &[u8], pos: usize, need: usize, ctx: &str) -> std::io::Result<()> {
        if pos + need > bytes.len() {
            Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "FIR6: truncated at {ctx} (need {need} at offset {pos}, have {})",
                    bytes.len()
                ),
            ))
        } else {
            Ok(())
        }
    }

    #[inline]
    fn read_u8(bytes: &[u8], pos: &mut usize) -> std::io::Result<u8> {
        ensure(bytes, *pos, 1, "u8")?;
        let v = bytes[*pos];
        *pos += 1;
        Ok(v)
    }
    #[inline]
    fn read_u16(bytes: &[u8], pos: &mut usize) -> std::io::Result<u16> {
        ensure(bytes, *pos, 2, "u16")?;
        let v = u16::from_le_bytes(bytes[*pos..*pos + 2].try_into().unwrap());
        *pos += 2;
        Ok(v)
    }
    #[inline]
    fn read_u32(bytes: &[u8], pos: &mut usize) -> std::io::Result<u32> {
        ensure(bytes, *pos, 4, "u32")?;
        let v = u32::from_le_bytes(bytes[*pos..*pos + 4].try_into().unwrap());
        *pos += 4;
        Ok(v)
    }
    #[inline]
    fn read_u64(bytes: &[u8], pos: &mut usize) -> std::io::Result<u64> {
        ensure(bytes, *pos, 8, "u64")?;
        let v = u64::from_le_bytes(bytes[*pos..*pos + 8].try_into().unwrap());
        *pos += 8;
        Ok(v)
    }
    #[inline]
    fn read_i64(bytes: &[u8], pos: &mut usize) -> std::io::Result<i64> {
        ensure(bytes, *pos, 8, "i64")?;
        let v = i64::from_le_bytes(bytes[*pos..*pos + 8].try_into().unwrap());
        *pos += 8;
        Ok(v)
    }

    #[inline]
    fn read_string(bytes: &[u8], pos: &mut usize) -> std::io::Result<String> {
        let len = read_u16(bytes, pos)? as usize;
        ensure(bytes, *pos, len, "string bytes")?;
        let s = std::str::from_utf8(&bytes[*pos..*pos + len]).map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, format!("utf8: {e}"))
        })?;
        *pos += len;
        Ok(s.to_string())
    }

    #[inline]
    fn read_string_array(bytes: &[u8], pos: &mut usize) -> std::io::Result<Vec<String>> {
        let count = read_u16(bytes, pos)? as usize;
        let mut out = Vec::with_capacity(count);
        for _ in 0..count {
            out.push(read_string(bytes, pos)?);
        }
        Ok(out)
    }

    #[inline]
    fn skip_cid(bytes: &[u8], pos: &mut usize) -> std::io::Result<()> {
        let len = read_u16(bytes, pos)? as usize;
        ensure(bytes, *pos, len, "cid bytes")?;
        *pos += len;
        Ok(())
    }

    fn skip_dict_pack_refs(bytes: &[u8], pos: &mut usize) -> std::io::Result<()> {
        // string forward packs
        let sp_count = read_u16(bytes, pos)? as usize;
        for _ in 0..sp_count {
            let _first = read_u64(bytes, pos)?;
            let _last = read_u64(bytes, pos)?;
            skip_cid(bytes, pos)?;
        }
        // subject forward packs per namespace
        let ns_count = read_u16(bytes, pos)? as usize;
        for _ in 0..ns_count {
            let _ns_code = read_u16(bytes, pos)?;
            let pack_count = read_u16(bytes, pos)? as usize;
            for _ in 0..pack_count {
                let _first = read_u64(bytes, pos)?;
                let _last = read_u64(bytes, pos)?;
                skip_cid(bytes, pos)?;
            }
        }
        Ok(())
    }

    fn skip_dict_tree_refs(bytes: &[u8], pos: &mut usize) -> std::io::Result<()> {
        skip_cid(bytes, pos)?; // branch CID
        let leaf_count = read_u32(bytes, pos)? as usize;
        for _ in 0..leaf_count {
            skip_cid(bytes, pos)?;
        }
        Ok(())
    }

    fn skip_graph_arenas(bytes: &[u8], pos: &mut usize, version: u8) -> std::io::Result<()> {
        // Matches `write_graph_arenas_v5` in binary-index.
        let _g_id = read_u16(bytes, pos)?;

        // numbig
        let nb_count = read_u16(bytes, pos)? as usize;
        for _ in 0..nb_count {
            let _p_id = read_u32(bytes, pos)?;
            skip_cid(bytes, pos)?;
        }
        // vectors
        let vec_count = read_u16(bytes, pos)? as usize;
        for _ in 0..vec_count {
            let _p_id = read_u32(bytes, pos)?;
            skip_cid(bytes, pos)?; // manifest
            let shard_count = read_u16(bytes, pos)? as usize;
            for _ in 0..shard_count {
                skip_cid(bytes, pos)?;
            }
        }
        // spatial
        let sp_count = read_u16(bytes, pos)? as usize;
        for _ in 0..sp_count {
            let _p_id = read_u32(bytes, pos)?;
            skip_cid(bytes, pos)?; // root_cid
            skip_cid(bytes, pos)?; // manifest
            skip_cid(bytes, pos)?; // arena
            let leaflet_count = read_u16(bytes, pos)? as usize;
            for _ in 0..leaflet_count {
                skip_cid(bytes, pos)?;
            }
        }
        // fulltext (v2 adds `lang_id: u16` per entry)
        let ft_count = read_u16(bytes, pos)? as usize;
        for _ in 0..ft_count {
            let _p_id = read_u32(bytes, pos)?;
            if version >= 2 {
                let _lang_id = read_u16(bytes, pos)?;
            }
            skip_cid(bytes, pos)?;
        }

        Ok(())
    }

    let mut pos = 8; // skip pad(2)
    let index_t = read_i64(bytes, &mut pos)?;
    let base_t = read_i64(bytes, &mut pos)?;

    // Ledger ID
    let ledger_id = read_string(bytes, &mut pos)?;

    // Subject ID encoding (skip; stored on FIR6 root only)
    let _subject_id_encoding = read_u8(bytes, &mut pos)?;

    // ns_split_mode
    let ns_split_mode_byte = read_u8(bytes, &mut pos)?;
    let ns_split_mode = crate::ns_encoding::NsSplitMode::from_byte(ns_split_mode_byte);

    // Namespace codes
    let ns_count = read_u16(bytes, &mut pos)? as usize;
    let mut namespace_codes: HashMap<u16, String> = HashMap::with_capacity(ns_count);
    for _ in 0..ns_count {
        let ns_code = read_u16(bytes, &mut pos)?;
        let prefix = read_string(bytes, &mut pos)?;
        namespace_codes.insert(ns_code, prefix);
    }

    // Predicate SIDs (skip)
    let pred_count = read_u32(bytes, &mut pos)? as usize;
    for _ in 0..pred_count {
        let _ns_code = read_u16(bytes, &mut pos)?;
        let _suffix = read_string(bytes, &mut pos)?;
    }

    // Small dict inlines
    let graph_iris = read_string_array(bytes, &mut pos)?;
    let _datatype_iris = read_string_array(bytes, &mut pos)?;
    let _language_tags = read_string_array(bytes, &mut pos)?;

    // Dict refs (skip)
    skip_dict_pack_refs(bytes, &mut pos)?;
    skip_dict_tree_refs(bytes, &mut pos)?; // subject reverse
    skip_dict_tree_refs(bytes, &mut pos)?; // string reverse

    // Per-graph specialty arenas (skip)
    let arena_count = read_u16(bytes, &mut pos)? as usize;
    for _ in 0..arena_count {
        skip_graph_arenas(bytes, &mut pos, version)?;
    }

    // Watermarks
    let wm_count = read_u16(bytes, &mut pos)? as usize;
    let mut subject_watermarks = Vec::with_capacity(wm_count);
    for _ in 0..wm_count {
        subject_watermarks.push(read_u64(bytes, &mut pos)?);
    }
    let string_watermark = read_u32(bytes, &mut pos)?;

    // Cumulative commit stats (skip)
    let _total_commit_size = read_u64(bytes, &mut pos)?;
    let _total_asserts = read_u64(bytes, &mut pos)?;
    let _total_retracts = read_u64(bytes, &mut pos)?;

    // o_type table (skip)
    let otype_count = read_u32(bytes, &mut pos)? as usize;
    for _ in 0..otype_count {
        let _o_type = read_u16(bytes, &mut pos)?;
        let _decode_kind = read_u8(bytes, &mut pos)?;
        let entry_flags = read_u8(bytes, &mut pos)?;
        if entry_flags & 1 != 0 {
            let _dt_iri = read_string(bytes, &mut pos)?;
        }
        if entry_flags & 2 != 0 {
            let _dict_family = read_u8(bytes, &mut pos)?;
        }
    }

    // Default graph routing (skip)
    let default_order_count = read_u8(bytes, &mut pos)? as usize;
    for _ in 0..default_order_count {
        let _order_id = read_u8(bytes, &mut pos)?;
        let leaf_count = read_u32(bytes, &mut pos)? as usize;
        for _ in 0..leaf_count {
            // first_key + last_key (RunRecordV2 ordered key)
            const RECORD_V2_WIRE_SIZE: usize = 30;
            ensure(bytes, pos, RECORD_V2_WIRE_SIZE, "RunRecordV2 first_key")?;
            pos += RECORD_V2_WIRE_SIZE;
            ensure(bytes, pos, RECORD_V2_WIRE_SIZE, "RunRecordV2 last_key")?;
            pos += RECORD_V2_WIRE_SIZE;
            let _row_count = read_u64(bytes, &mut pos)?;
            skip_cid(bytes, &mut pos)?;
            let has_sidecar = read_u8(bytes, &mut pos)?;
            if has_sidecar != 0 {
                skip_cid(bytes, &mut pos)?;
            }
        }
    }

    // Named graph routing (skip)
    let named_count = read_u16(bytes, &mut pos)? as usize;
    for _ in 0..named_count {
        let _g_id = read_u16(bytes, &mut pos)?;
        let order_count = read_u8(bytes, &mut pos)? as usize;
        for _ in 0..order_count {
            let _order_id = read_u8(bytes, &mut pos)?;
            skip_cid(bytes, &mut pos)?;
        }
    }

    // Optional sections
    let stats = if flags & FLAG_HAS_STATS != 0 {
        let stats_len = read_u32(bytes, &mut pos)? as usize;
        ensure(bytes, pos, stats_len, "stats section")?;
        let (s, _consumed) = stats_wire::decode_stats(&bytes[pos..pos + stats_len])?;
        pos += stats_len;
        Some(s)
    } else {
        None
    };

    let schema = if flags & FLAG_HAS_SCHEMA != 0 {
        let schema_len = read_u32(bytes, &mut pos)? as usize;
        ensure(bytes, pos, schema_len, "schema section")?;
        let (s, _consumed) = stats_wire::decode_schema(&bytes[pos..pos + schema_len])?;
        pos += schema_len;
        Some(s)
    } else {
        None
    };

    if flags & FLAG_HAS_PREV_INDEX != 0 {
        let _t = read_i64(bytes, &mut pos)?;
        skip_cid(bytes, &mut pos)?;
    }
    if flags & FLAG_HAS_GARBAGE != 0 {
        skip_cid(bytes, &mut pos)?;
    }
    if flags & FLAG_HAS_SKETCH != 0 {
        skip_cid(bytes, &mut pos)?;
    }

    Ok(LedgerSnapshotMetadata {
        ledger_id,
        t: index_t,
        base_t,
        namespace_codes,
        ns_split_mode,
        stats,
        schema,
        subject_watermarks,
        string_watermark,
        graph_iris,
    })
}

/// Load a database from an index root content ID.
///
/// The returned Db is metadata-only (`range_provider = None`). The caller
/// (typically the API layer) must load a `BinaryIndexStore` and attach a
/// `BinaryRangeProvider` before serving range queries.
///
/// The storage address is derived internally from the `ContentId` and
/// `ledger_id` using the storage backend's method identifier.
pub async fn load_ledger_snapshot(
    storage: &(impl StorageRead + crate::storage::StorageMethod),
    root_id: &ContentId,
    ledger_id: &str,
) -> Result<LedgerSnapshot> {
    let root_address = crate::content_address(
        storage.storage_method(),
        ContentKind::IndexRoot,
        ledger_id,
        &root_id.digest_hex(),
    );
    let bytes = storage.read_bytes(&root_address).await?;
    LedgerSnapshot::from_root_bytes(&bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_root_bytes_rejects_non_fir6() {
        let json = b"{\"ledger_id\": \"test:main\"}";
        let err = LedgerSnapshot::from_root_bytes(json).unwrap_err();
        assert!(err.to_string().contains("FIR6"));
    }

    #[test]
    fn test_from_root_bytes_rejects_truncated() {
        let err = LedgerSnapshot::from_root_bytes(b"FI").unwrap_err();
        assert!(err.to_string().contains("too short"));
    }

    #[test]
    fn test_encode_decode_sid() {
        let mut ns = HashMap::new();
        ns.insert(0u16, String::new());
        ns.insert(100u16, "http://example.org/".to_string());
        let db = LedgerSnapshot::new_meta(LedgerSnapshotMetadata {
            ledger_id: "test:main".into(),
            t: 1,
            base_t: 0,
            namespace_codes: ns,
            ns_split_mode: NsSplitMode::default(),
            stats: None,
            schema: None,
            subject_watermarks: vec![],
            string_watermark: 0,
            graph_iris: vec![],
        })
        .unwrap();

        let sid = db.encode_iri("http://example.org/Alice").unwrap();
        assert_eq!(sid.namespace_code, 100);
        assert_eq!(sid.name.as_ref(), "Alice");

        let iri = db.decode_sid(&sid).unwrap();
        assert_eq!(iri, "http://example.org/Alice");
    }

    #[test]
    fn test_genesis_db() {
        let db = LedgerSnapshot::genesis("test:main");
        assert_eq!(db.t, 0);
        assert_eq!(db.ledger_id, "test:main");
        assert!(db.range_provider.is_none());
    }

    #[test]
    fn test_encode_iri_txn_meta_graph() {
        let db = LedgerSnapshot::genesis("mydb:main");
        let txn_meta_iri = crate::graph_registry::txn_meta_graph_iri("mydb:main");
        assert_eq!(txn_meta_iri, "urn:fluree:mydb:main#txn-meta");

        // Canonical split (MostGranular) for this opaque IRI: prefix = "urn:fluree:mydb:main#"
        // Genesis db only has "urn:fluree:" (FLUREE_URN) — canonical prefix not registered.
        // Canonical encoding: no starts_with fallback → EMPTY.
        let sid = db
            .encode_iri(&txn_meta_iri)
            .expect("encode_iri always returns Some via EMPTY fallback");
        assert_eq!(sid.namespace_code, EMPTY);
        assert_eq!(sid.name.as_ref(), "urn:fluree:mydb:main#txn-meta");

        // Round-trip: EMPTY code decodes to sid.name (the full IRI).
        let decoded = db.decode_sid(&sid).unwrap();
        assert_eq!(decoded, txn_meta_iri);

        // Once the canonical prefix is registered (by the transact layer),
        // encode uses the exact prefix.
        let mut db2 = db.clone();
        db2.insert_namespace_code(100, "urn:fluree:mydb:main#".to_string())
            .unwrap();
        let sid2 = db2.encode_iri(&txn_meta_iri).unwrap();
        assert_eq!(sid2.namespace_code, 100);
        assert_eq!(sid2.name.as_ref(), "txn-meta");
        assert_eq!(db2.decode_sid(&sid2).unwrap(), txn_meta_iri);
    }

    // =========================================================================
    // apply_envelope_deltas — conflict rejection
    // =========================================================================

    /// Helper: build a genesis snapshot with one user namespace pre-registered.
    fn snapshot_with_ns(code: u16, prefix: &str) -> LedgerSnapshot {
        let mut db = LedgerSnapshot::genesis("test:main");
        db.insert_namespace_code(code, prefix.to_string()).unwrap();
        db
    }

    #[test]
    fn apply_envelope_deltas_accepts_identical_mapping() {
        let mut db = snapshot_with_ns(100, "http://example.org/");
        let mut delta = HashMap::new();
        delta.insert(100u16, "http://example.org/".to_string());
        // Same code, same prefix → no-op, no error.
        db.apply_envelope_deltas(&delta, std::iter::empty::<&str>())
            .expect("identical mapping should succeed");
    }

    #[test]
    fn apply_envelope_deltas_rejects_code_to_different_prefix() {
        let mut db = snapshot_with_ns(100, "http://example.org/");
        let mut delta = HashMap::new();
        // Same code 100, but different prefix.
        delta.insert(100u16, "http://other.org/".to_string());
        let err = db
            .apply_envelope_deltas(&delta, std::iter::empty::<&str>())
            .unwrap_err();
        assert!(
            err.to_string().contains("namespace conflict"),
            "expected code→prefix conflict, got: {err}"
        );
        assert!(err.to_string().contains("100"));
    }

    #[test]
    fn apply_envelope_deltas_rejects_prefix_to_different_code() {
        let mut db = snapshot_with_ns(100, "http://example.org/");
        let mut delta = HashMap::new();
        // Different code 200, but same prefix.
        delta.insert(200u16, "http://example.org/".to_string());
        let err = db
            .apply_envelope_deltas(&delta, std::iter::empty::<&str>())
            .unwrap_err();
        assert!(
            err.to_string().contains("namespace conflict"),
            "expected prefix→code conflict, got: {err}"
        );
        assert!(err.to_string().contains("200"));
    }

    #[test]
    fn apply_envelope_deltas_accepts_new_mapping() {
        let mut db = snapshot_with_ns(100, "http://example.org/");
        let mut delta = HashMap::new();
        delta.insert(200u16, "http://other.org/".to_string());
        db.apply_envelope_deltas(&delta, std::iter::empty::<&str>())
            .expect("new non-conflicting mapping should succeed");
        // Verify both directions.
        assert_eq!(
            db.namespaces().get(&200),
            Some(&"http://other.org/".to_string())
        );
        assert_eq!(
            db.namespace_reverse().get("http://other.org/"),
            Some(&200u16)
        );
    }
}
