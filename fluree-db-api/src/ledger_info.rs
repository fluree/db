//! Ledger information API
//!
//! This module provides graph-scoped ledger metadata via `build_ledger_info`.
//!
//! ## Response Shape
//!
//! ```json
//! {
//!   "ledger": { "alias", "t", "commit-t", "index-t", "flakes", "size", "named-graphs" },
//!   "graph": "urn:default",
//!   "stats": { "flakes", "size", "properties": { ... }, "classes": { ... } },
//!   "commit": { ... },
//!   "nameservice": { ... },
//!   "index": { ... }
//! }
//! ```
//!
//! The `stats` block is always scoped to a single graph (default: g_id=0).
//! Use the builder API to select a different graph via name, IRI, or g_id.

use crate::format::iri::IriCompactor;
use async_trait::async_trait;
use fluree_db_binary_index::BinaryIndexStore;
use fluree_db_core::address_path::ledger_id_to_path_prefix;
use fluree_db_core::ids::GraphId;
use fluree_db_core::ledger_id::{format_ledger_id, split_ledger_id};
use fluree_db_core::load_commit_by_id;
use fluree_db_core::value_id::ValueTypeTag;
use fluree_db_core::{
    ClassStatEntry, GraphPropertyStatEntry, GraphStatsEntry, IndexSchema, IndexStats,
    LedgerSnapshot, OverlayProvider, RuntimePredicateId, RuntimeSmallDicts, SchemaPredicateInfo,
    Sid, Storage,
};
use fluree_db_ledger::LedgerState;
use fluree_db_nameservice::{GraphSourceRecord, GraphSourceType, NsRecord};
use fluree_db_novelty::{
    assemble_fast_stats, assemble_full_stats, StatsAssemblyError, StatsLookup,
};
use fluree_db_r2rml::{CompiledR2rmlMapping, ObjectMap, TermType};
use fluree_graph_json_ld::ParsedContext;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use xxhash_rust::xxh3::xxh3_128;

// ============================================================================
// Typed ledger-info response
// ============================================================================
//
// The `/info` (ledger-info) response is assembled by TWO independent builders:
// the native committed-ledger path ([`build_ledger_info_with_options`]) and the
// virtual query-in-place R2RML/Iceberg path ([`build_virtual_ledger_info`]).
// Both now construct the SAME [`LedgerInfo`] type, so the compiler enforces
// native↔virtual parity on the shared `stats`/`classes`/`properties` core — the
// shape that previously drifted silently (a virtual class emitted only `count`
// while a native class emitted `count`+`properties`+`subclass-of`, breaking the
// instance view and the LLM data-model reader with no compile-time signal).
//
// Serde layout preserves the exact on-the-wire bytes of BOTH paths:
//   * `#[serde(skip_serializing_if = "…")]`  → OMIT the key when absent.
//   * a plain `Option<T>` with NO skip        → emit an explicit `null`.
// so null-vs-absent fidelity is exact. `#[serde(default)]` is added to optional
// fields purely so downstream consumers (solo, conformance) can `Deserialize`
// leniently; it never affects serialization. The nested types deliberately do
// NOT derive `Default`, so a builder that omits a shared field (e.g. per-class
// `properties`) fails to compile rather than silently emitting a divergent shape.

/// Typed `/info` (ledger-info) response — see module-level notes.
///
/// Native builders leave `ledger_id`/`t`/`source` unset (the server route stamps
/// `ledger_id`/`t` onto the native response as trailing keys); the virtual
/// builder sets them. `commit` and `nameservice` remain [`JsonValue`] because
/// they are genuinely dynamic JSON-LD documents whose shapes differ between the
/// two paths (native `NsRecord` vs virtual `GraphSourceRecord`; native commit
/// object / `{"error":…}` / `null`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LedgerInfo {
    /// Virtual-only: dataset id echoed at top level. Native omits it here.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ledger_id: Option<String>,
    /// Virtual-only top-level version `t` (Iceberg snapshot id; `null` when
    /// unknown). `Some(None)` → `null`; `None` → key omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub t: Option<Option<i64>>,

    /// Ledger-wide metadata (shared).
    pub ledger: Ledger,
    /// Name of the graph the `stats` block is scoped to (shared).
    pub graph: String,
    /// Graph-scoped statistics — holds the drift-prone `classes` core (shared).
    pub stats: Stats,

    /// Commit block. Native: commit JSON-LD object, `null`, or `{"error":…}`;
    /// virtual: always `null`. Kept as [`JsonValue`] (dynamic JSON-LD).
    #[serde(default)]
    pub commit: JsonValue,

    /// Native-only content id of the head commit.
    #[serde(rename = "commitId", default, skip_serializing_if = "Option::is_none")]
    pub commit_id: Option<String>,
    /// Native-only content id of the head index.
    #[serde(rename = "indexId", default, skip_serializing_if = "Option::is_none")]
    pub index_id: Option<String>,

    /// Nameservice JSON-LD (native `NsRecord` view / virtual redacted
    /// `GraphSourceRecord` view). Kept as [`JsonValue`] (dynamic JSON-LD).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nameservice: Option<JsonValue>,

    /// Native-only index pointer block.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index: Option<Index>,

    /// Virtual-only source metadata (identifying only — NEVER credentials).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<Source>,
}

impl LedgerInfo {
    /// Serialize to a [`JsonValue`] for the `JsonValue`-returning API boundary
    /// (server routes, cache, MCP markdown). Infallible: every field is a
    /// String-keyed, JSON-representable value.
    pub fn into_json(self) -> JsonValue {
        serde_json::to_value(self).expect("LedgerInfo is always JSON-serializable")
    }
}

/// Ledger-wide metadata block (`ledger`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Ledger {
    /// Ledger id / graph-source id.
    pub alias: String,
    /// Version `t`. Native: commit `t`; virtual: Iceberg snapshot id (`null`
    /// when unknown).
    #[serde(default)]
    pub t: Option<i64>,
    /// Commit `t`. `null` for virtual datasets (no commit chain).
    #[serde(rename = "commit-t", default)]
    pub commit_t: Option<i64>,
    /// Index `t`. `null` for virtual datasets (no index chain).
    #[serde(rename = "index-t", default)]
    pub index_t: Option<i64>,
    /// Total flakes (native) / total subject rows (virtual; `null` when
    /// unknown).
    #[serde(default)]
    pub flakes: Option<i64>,
    /// Byte size (native) / `0` (virtual).
    pub size: u64,
    /// Registered named graphs (always includes `urn:default`).
    #[serde(rename = "named-graphs")]
    pub named_graphs: Vec<NamedGraph>,
}

/// One entry in `ledger.named-graphs`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NamedGraph {
    /// Full graph IRI (usable in FROM / FROM NAMED).
    pub iri: String,
    /// Numeric graph id (`0` = default).
    #[serde(rename = "g-id")]
    pub g_id: GraphId,
    /// Flakes (native) / subject rows (virtual; `null` when unknown).
    #[serde(default)]
    pub flakes: Option<i64>,
    /// Byte size (native) / `0` (virtual).
    pub size: u64,
}

/// Graph-scoped statistics block (`stats`).
///
/// `classes` — and its nested per-class `properties` — is the shape native and
/// virtual builders MUST agree on.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Stats {
    /// Graph-scoped flakes (native) / total rows (virtual; `null` when unknown).
    #[serde(default)]
    pub flakes: Option<i64>,
    /// Byte size (native) / `0` (virtual).
    pub size: u64,
    /// Flat, graph/dataset-wide property stats keyed by (compacted) IRI.
    #[serde(default)]
    pub properties: BTreeMap<String, PropertyStat>,
    /// Per-class stats keyed by (compacted) class IRI.
    #[serde(default)]
    pub classes: BTreeMap<String, ClassInfo>,
}

/// Flat per-property stats (value of a `stats.properties` entry).
///
/// Native fills the rich fields (`last-modified-t`, estimates, `sub-property-of`)
/// as requested by options; virtual fills only `count` + `datatypes`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PropertyStat {
    /// Flake/row count. `null` when unknown (virtual dataset, no scan).
    #[serde(default)]
    pub count: Option<i64>,
    /// Native-only: most recent modifying `t`. Omitted for virtual datasets.
    #[serde(
        rename = "last-modified-t",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub last_modified_t: Option<i64>,
    /// Per-datatype counts (`null` value = unknown). Native emits this only when
    /// datatype detail is requested; virtual always emits it. `None` omits it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub datatypes: Option<BTreeMap<String, Option<i64>>>,
    /// Native-only index-derived estimate (present only when requested).
    #[serde(
        rename = "ndv-values",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub ndv_values: Option<u64>,
    /// Native-only index-derived estimate (present only when requested).
    #[serde(
        rename = "ndv-subjects",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub ndv_subjects: Option<u64>,
    /// Native-only index-derived estimate (present only when requested).
    #[serde(
        rename = "selectivity-value",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub selectivity_value: Option<u64>,
    /// Native-only index-derived estimate (present only when requested).
    #[serde(
        rename = "selectivity-subject",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub selectivity_subject: Option<u64>,
    /// Native-only schema hierarchy parents. Omitted when empty.
    #[serde(
        rename = "sub-property-of",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    pub sub_property_of: Vec<String>,
}

/// Per-class stats (value of a `stats.classes` entry).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClassInfo {
    /// Instance/row count. `null` when unknown (virtual dataset, no scan).
    #[serde(default)]
    pub count: Option<i64>,
    /// Native-only schema hierarchy parents. Omitted when empty.
    #[serde(rename = "subclass-of", default, skip_serializing_if = "Vec::is_empty")]
    pub subclass_of: Vec<String>,
    /// Per-class property membership — the map the instance view + data-model /
    /// LLM reader consume. BOTH builders MUST populate it; there is no `Default`,
    /// so omitting it is a compile error (the whole point of this shared type).
    #[serde(default)]
    pub properties: BTreeMap<String, PropertyInfo>,
    /// Native-only ordered list of this class's property IRIs. Virtual omits it
    /// (`None`); native always emits it (possibly empty).
    #[serde(
        rename = "property-list",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub property_list: Option<Vec<String>>,
}

/// Per-(class, property) usage detail (value of a `stats.classes[c].properties`
/// entry) — the exact shape native↔virtual previously disagreed on.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PropertyInfo {
    /// Per-datatype counts (`null` value = unknown, virtual no-scan).
    #[serde(default)]
    pub types: BTreeMap<String, Option<i64>>,
    /// Per-language-tag counts (empty for virtual R2RML datasets).
    #[serde(default)]
    pub langs: BTreeMap<String, i64>,
    /// Per-target-class reference counts (empty until FK detection lands).
    #[serde(rename = "ref-classes", default)]
    pub ref_classes: BTreeMap<String, i64>,
}

/// Native-only index pointer block (`index`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Index {
    /// Indexed `t`.
    pub t: i64,
    /// Content id of the index root, when available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
}

/// Virtual-only source metadata block (`source`). Identifying only — the auth /
/// credentials used to read metadata NEVER land here.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Source {
    /// Always `true` — marks a query-in-place (virtual) dataset.
    #[serde(rename = "virtual")]
    pub is_virtual: bool,
    /// Source type label (`"Iceberg"` / `"R2RML"`).
    #[serde(rename = "type")]
    pub source_type: String,
    /// Distinct logical tables referenced by the mapping/config.
    pub tables: Vec<String>,
    /// Current Iceberg snapshot id (the virtual dataset "version"); `null` when
    /// unknown.
    #[serde(default)]
    pub snapshot: Option<i64>,
    /// Catalog descriptor (REST or Direct-mode). Omitted when neither is known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catalog: Option<Catalog>,
    /// Authoritative per-table row counts (Iceberg manifest metadata).
    #[serde(rename = "table-row-counts", default)]
    pub table_row_counts: BTreeMap<String, i64>,
}

/// Source catalog descriptor. `type`/`warehouse` emit explicit `null` (not
/// omitted) to match the historic REST shape.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Catalog {
    /// REST catalog (Polaris / generic REST).
    Rest {
        /// Catalog type identifier (e.g. `"rest"`, `"polaris"`); `null` when
        /// unknown.
        #[serde(rename = "type", default)]
        catalog_type: Option<String>,
        /// Catalog base URI.
        uri: String,
        /// Optional warehouse identifier; `null` when unset.
        #[serde(default)]
        warehouse: Option<String>,
    },
    /// Direct-mode (S3 table location) catalog.
    Direct {
        /// S3 table location.
        table_location: String,
    },
}

/// Which graph to scope the stats section to.
#[derive(Debug, Clone, Default)]
pub enum GraphSelector {
    /// Default graph (g_id = 0).
    #[default]
    Default,
    /// Select by numeric graph ID.
    ById(GraphId),
    /// Select by graph IRI (resolved via the binary index store).
    ByIri(String),
    /// Select by well-known name ("default", "txn-meta", or "config").
    ByName(String),
}

/// Options controlling `ledger-info` stats detail and freshness.
///
/// Defaults return the full novelty-aware ledger view. Callers can opt into
/// lighter/index-derived payloads explicitly when they want cheaper planner-style
/// metadata instead of the full public ledger-info view.
#[derive(Debug, Clone)]
pub struct LedgerInfoOptions {
    /// When true, include full novelty-aware property/class detail assembly
    /// (including lookup-backed class/ref enrichment).
    ///
    /// When false, `ledger-info` uses the lighter fast novelty-aware merge that
    /// keeps counts current but skips lookup-backed enrichment.
    pub realtime_property_details: bool,

    /// When true, include `datatypes` under `stats.properties[*]`.
    pub include_property_datatypes: bool,

    /// When true, include index-derived NDV/selectivity estimates under
    /// `stats.properties[*]`.
    ///
    /// These values are only as current as the last index refresh, so they are
    /// omitted from the default ledger-info payload to keep the default response
    /// fully current with respect to novelty-aware stats.
    pub include_property_estimates: bool,

    /// Which graph to scope the stats section to.
    pub graph: GraphSelector,
}

impl Default for LedgerInfoOptions {
    fn default() -> Self {
        Self {
            realtime_property_details: true,
            include_property_datatypes: true,
            include_property_estimates: false,
            graph: GraphSelector::Default,
        }
    }
}

/// Schema index for fast SID -> hierarchy lookup
type SchemaIndex<'a> = HashMap<Sid, &'a SchemaPredicateInfo>;

/// Build a schema index for fast hierarchy lookups
fn build_schema_index(schema: &IndexSchema) -> SchemaIndex<'_> {
    schema
        .pred
        .vals
        .iter()
        .map(|info| (info.id.clone(), info))
        .collect()
}

/// Error type for ledger info operations
#[derive(Debug, thiserror::Error)]
pub enum LedgerInfoError {
    #[error("No commit ID available")]
    NoCommitId,

    #[error("Failed to load commit: {0}")]
    CommitLoad(String),

    #[error("Storage error: {0}")]
    Storage(String),

    #[error("Unknown namespace code: {0}")]
    UnknownNamespace(u16),

    #[error("Class lookup failed: {0}")]
    ClassLookup(String),

    #[error("Unknown graph: {0}")]
    UnknownGraph(String),
}

/// Result type for ledger info operations
pub type Result<T> = std::result::Result<T, LedgerInfoError>;

struct LedgerInfoStatsLookup<'a> {
    store: Option<&'a BinaryIndexStore>,
    runtime_small_dicts: Option<&'a RuntimeSmallDicts>,
}

#[async_trait]
impl StatsLookup for LedgerInfoStatsLookup<'_> {
    fn runtime_small_dicts(&self) -> Option<&RuntimeSmallDicts> {
        self.runtime_small_dicts
    }

    fn persisted_predicate_id_for_sid(&self, sid: &Sid) -> Option<RuntimePredicateId> {
        self.store
            .and_then(|store| store.sid_to_p_id(sid).map(RuntimePredicateId::from_u32))
    }

    async fn lookup_subject_classes(
        &self,
        snapshot: &LedgerSnapshot,
        overlay: &dyn OverlayProvider,
        to_t: i64,
        g_id: GraphId,
        subjects: &[Sid],
    ) -> std::result::Result<HashMap<Sid, Vec<Sid>>, StatsAssemblyError> {
        let mut db = fluree_db_core::GraphDbRef::new(snapshot, g_id, overlay, to_t);
        if let Some(runtime_small_dicts) = self.runtime_small_dicts {
            db = db.with_runtime_small_dicts(runtime_small_dicts);
        }
        fluree_db_policy::lookup_subject_classes(subjects, db)
            .await
            .map_err(|e| StatsAssemblyError::Message(e.to_string()))
    }
}

/// Build comprehensive ledger metadata.
///
/// Returns JSON containing:
/// - `ledger`: ledger-wide metadata
/// - `graph`: name of the graph being reported
/// - `stats`: graph-scoped statistics with decoded IRIs
/// - `commit`: Commit info in JSON-LD format
/// - `nameservice`: NsRecord in JSON-LD format
/// - `index`: Index metadata (if available)
pub async fn build_ledger_info<S: Storage + Clone>(
    ledger: &LedgerState,
    storage: &S,
    context: Option<&JsonValue>,
) -> Result<JsonValue> {
    build_ledger_info_with_options(ledger, storage, context, LedgerInfoOptions::default()).await
}

/// Build comprehensive ledger metadata, with optional extra/real-time stats.
pub async fn build_ledger_info_with_options<S: Storage + Clone>(
    ledger: &LedgerState,
    storage: &S,
    context: Option<&JsonValue>,
    options: LedgerInfoOptions,
) -> Result<JsonValue> {
    // Build the IRI compactor for stats decoding
    let parsed_context = context
        .map(|c| ParsedContext::parse(None, c).unwrap_or_default())
        .unwrap_or_default();
    let compactor = IriCompactor::new(ledger.snapshot.shared_namespaces(), &parsed_context);

    // Build schema index for hierarchy lookups
    let schema_index = ledger
        .snapshot
        .schema
        .as_ref()
        .map(build_schema_index)
        .unwrap_or_default();

    // Try to get the BinaryIndexStore for IRI resolution
    let binary_store: Option<Arc<BinaryIndexStore>> = ledger
        .binary_store
        .as_ref()
        .and_then(|te| Arc::clone(&te.0).downcast::<BinaryIndexStore>().ok());

    // Resolve graph selector to g_id
    let g_id = resolve_graph_selector(&options.graph, binary_store.as_deref())?;

    // Determine graph display name
    let graph_name = graph_display_name(g_id, binary_store.as_deref());

    let indexed = ledger.snapshot.stats.clone().unwrap_or_default();
    let stats_lookup = LedgerInfoStatsLookup {
        store: binary_store.as_deref(),
        runtime_small_dicts: Some(&ledger.runtime_small_dicts),
    };
    let mut stats: IndexStats = match (options.realtime_property_details, ledger.novelty.is_empty())
    {
        (_, true) => indexed.clone(),
        (true, false) => assemble_full_stats(
            &indexed,
            &ledger.snapshot,
            ledger.novelty.as_ref(),
            ledger.novelty.as_ref(),
            ledger.t(),
            &stats_lookup,
        )
        .await
        .map_err(|e| LedgerInfoError::ClassLookup(e.to_string()))?,
        _ => assemble_fast_stats(
            &indexed,
            &ledger.snapshot,
            ledger.novelty.as_ref(),
            ledger.t(),
            Some(&stats_lookup as &dyn StatsLookup),
        ),
    };

    // Pre-index fallback: if no graph stats from index, try loading the pre-index manifest
    if stats.graphs.is_none() {
        let alias_prefix = ledger_id_to_path_prefix(&ledger.snapshot.ledger_id)
            .unwrap_or_else(|_| ledger.snapshot.ledger_id.replace(':', "/"));
        let manifest_addr_primary =
            format!("fluree:file://{alias_prefix}/stats/pre-index-stats.json");
        if let Ok(bytes) = storage.read_bytes(&manifest_addr_primary).await {
            match parse_pre_index_manifest(&bytes) {
                Ok(graphs) => {
                    tracing::debug!(graphs = graphs.len(), "loaded pre-index stats manifest");
                    stats.graphs = Some(graphs);
                }
                Err(e) => {
                    tracing::warn!("failed to parse pre-index stats manifest: {}", e);
                }
            }
        }
    }

    // Build the typed response. BOTH the native path (here) and the virtual
    // path ([`build_virtual_ledger_info`]) construct the shared `LedgerInfo`, so
    // the compiler enforces stats/classes/properties parity. The native path
    // leaves the virtual-only fields (`ledger_id`/`t`/`source`) unset — the
    // server route stamps `ledger_id`/`t` on as trailing keys.

    // 1. Ledger block (ledger-wide metadata).
    let ledger_block = build_ledger_block(ledger, &stats);

    // 2 + 3. Graph name + graph-scoped stats section.
    let stats_block = build_graph_scoped_stats(
        g_id,
        &stats,
        &compactor,
        &schema_index,
        binary_store.as_deref(),
        Some(&ledger.runtime_small_dicts),
        options.include_property_datatypes,
        options.include_property_estimates,
    )?;

    // 4. Commit section (ALWAYS include, even if None). Dynamic JSON-LD document
    // (or `{"error":…}` on load failure), so kept as `JsonValue`.
    let commit = if let Some(head_cid) = &ledger.head_commit_id {
        match build_commit_jsonld(storage, head_cid, &ledger.snapshot.ledger_id).await {
            Ok(commit_json) => commit_json,
            Err(e) => json!({ "error": format!("{}", e) }),
        }
    } else {
        JsonValue::Null
    };

    // Content identifiers when available.
    let commit_id = ledger.head_commit_id.as_ref().map(ToString::to_string);
    let index_id = ledger.head_index_id.as_ref().map(ToString::to_string);

    // 5. Nameservice section (dynamic JSON-LD, kept as `JsonValue`).
    let nameservice = ledger.ns_record.as_ref().map(ns_record_to_jsonld);

    // 6. Index section (if available). `id` prefers the live head index id and
    // falls back to the nameservice record's.
    let index = ledger.ns_record.as_ref().and_then(|ns_record| {
        if ns_record.index_head_id.is_some() || ns_record.index_t > 0 {
            let id = ledger
                .head_index_id
                .as_ref()
                .or(ns_record.index_head_id.as_ref())
                .map(ToString::to_string);
            Some(Index {
                t: ns_record.index_t,
                id,
            })
        } else {
            None
        }
    });

    Ok(LedgerInfo {
        ledger_id: None,
        t: None,
        ledger: ledger_block,
        graph: graph_name,
        stats: stats_block,
        commit,
        commit_id,
        index_id,
        nameservice,
        index,
        source: None,
    }
    .into_json())
}

// ============================================================================
// Graph selector resolution
// ============================================================================

/// Resolve a `GraphSelector` to a numeric `g_id`.
fn resolve_graph_selector(
    selector: &GraphSelector,
    store: Option<&BinaryIndexStore>,
) -> Result<GraphId> {
    match selector {
        GraphSelector::Default => Ok(0),
        GraphSelector::ById(g_id) => Ok(*g_id),
        GraphSelector::ByName(name) => match name.as_str() {
            "default" | "urn:default" => Ok(0),
            "txn-meta" => Ok(1),
            "config" => Ok(2),
            other => {
                // Try as IRI
                if let Some(store) = store {
                    store
                        .graph_id_for_iri(other)
                        .ok_or_else(|| LedgerInfoError::UnknownGraph(other.to_string()))
                } else {
                    Err(LedgerInfoError::UnknownGraph(format!(
                        "no binary index store available to resolve graph name '{other}'"
                    )))
                }
            }
        },
        GraphSelector::ByIri(iri) => {
            if iri == "urn:default" {
                return Ok(0);
            }
            // Recognize well-known system graph IRIs so resolution works even
            // without a binary store (e.g. pre-index).
            if iri.ends_with("#txn-meta") {
                return Ok(1);
            }
            if iri.ends_with("#config") {
                return Ok(2);
            }
            if let Some(store) = store {
                store
                    .graph_id_for_iri(iri)
                    .ok_or_else(|| LedgerInfoError::UnknownGraph(iri.clone()))
            } else {
                Err(LedgerInfoError::UnknownGraph(format!(
                    "no binary index store available to resolve graph IRI '{iri}'"
                )))
            }
        }
    }
}

/// Determine the display name for a graph ID.
fn graph_display_name(g_id: GraphId, store: Option<&BinaryIndexStore>) -> String {
    if g_id == 0 {
        return "urn:default".to_string();
    }
    if let Some(store) = store {
        if let Some(iri) = store.graph_iri_for_id(g_id) {
            return iri.to_string();
        }
    }
    format!("g:{g_id}")
}

// ============================================================================
// Response builders
// ============================================================================

/// Build the `ledger` block with ledger-wide metadata.
fn build_ledger_block(ledger: &LedgerState, stats: &IndexStats) -> Ledger {
    let index_t = ledger
        .ns_record
        .as_ref()
        .map(|r| r.index_t)
        .unwrap_or(ledger.snapshot.t);

    let commit_t = ledger
        .ns_record
        .as_ref()
        .map(|r| r.commit_t)
        .unwrap_or(ledger.t());

    let graph_sizes = stats.graphs.as_deref().unwrap_or_default();
    let graph_totals = |g_id: GraphId| -> (u64, u64) {
        graph_sizes
            .iter()
            .find(|g| g.g_id == g_id)
            .map(|g| (g.flakes, g.size))
            .unwrap_or((0, 0))
    };

    // Build the `named-graphs` list from the **live** `GraphRegistry` on the
    // snapshot, not from the binary index store. The registry is updated at
    // commit-apply time, so it includes graphs registered since the last
    // index build (and stays correct for ledgers with no index at all).
    // `flakes` / `size` still come from `IndexStats.graphs` when available
    // and fall back to `0` when no per-graph stats are present.
    //
    // The `"iri"` value is the full graph IRI, usable directly in
    // FROM / FROM NAMED clauses.
    let mut named_graphs = Vec::new();
    // Always include the default graph (g_id=0). The registry does not
    // store an IRI for it, so we synthesize `urn:default` here.
    let (default_flakes, default_size) = graph_totals(0);
    named_graphs.push(NamedGraph {
        iri: "urn:default".to_string(),
        g_id: 0,
        flakes: Some(default_flakes as i64),
        size: default_size,
    });
    // Then every registered graph (txn-meta at g_id=1, config at g_id=2,
    // user-defined at g_id>=3). `iter_entries` is dense and ordered by
    // g_id, so the response is deterministic.
    for (g_id, iri) in ledger.snapshot.graph_registry.iter_entries() {
        let (flakes, size) = graph_totals(g_id);
        named_graphs.push(NamedGraph {
            iri: iri.to_string(),
            g_id,
            flakes: Some(flakes as i64),
            size,
        });
    }

    Ledger {
        alias: ledger.snapshot.ledger_id.clone(),
        t: Some(ledger.t()),
        commit_t: Some(commit_t),
        index_t: Some(index_t),
        flakes: Some(stats.flakes as i64),
        size: stats.size,
        named_graphs,
    }
}

/// Build the graph-scoped `stats` section.
///
/// Extracts the `GraphStatsEntry` for the requested `g_id` and renders
/// its properties and classes with IRI compaction.
///
/// All graphs (including default g_id=0) use their `GraphStatsEntry` for
/// graph-scoped properties and classes.
#[allow(clippy::too_many_arguments)]
fn build_graph_scoped_stats(
    g_id: GraphId,
    stats: &IndexStats,
    compactor: &IriCompactor,
    schema_index: &SchemaIndex,
    store: Option<&BinaryIndexStore>,
    runtime_small_dicts: Option<&RuntimeSmallDicts>,
    include_property_datatypes: bool,
    include_property_estimates: bool,
) -> Result<Stats> {
    // Find the GraphStatsEntry for the requested g_id (works for all graphs including default).
    let graph_entry = stats
        .graphs
        .as_ref()
        .and_then(|gs| gs.iter().find(|g| g.g_id == g_id));

    let (graph_flakes, graph_size) = graph_entry.map(|g| (g.flakes, g.size)).unwrap_or((0, 0));

    // Properties: always from graph-scoped GraphStatsEntry.
    let properties = if let Some(entry) = graph_entry {
        decode_graph_property_stats(
            &entry.properties,
            compactor,
            schema_index,
            store,
            runtime_small_dicts,
            include_property_datatypes,
            include_property_estimates,
        )?
    } else {
        BTreeMap::new()
    };

    // Classes: always from graph-scoped GraphStatsEntry.
    let classes = if let Some(entry) = graph_entry {
        decode_class_stats(&entry.classes, compactor, schema_index)?
    } else {
        BTreeMap::new()
    };

    Ok(Stats {
        flakes: Some(graph_flakes as i64),
        size: graph_size,
        properties,
        classes,
    })
}

// ============================================================================
// Commit / Nameservice JSON-LD helpers
// ============================================================================

/// Build commit JSON-LD block.
async fn build_commit_jsonld<S: Storage + Clone>(
    storage: &S,
    head_id: &fluree_db_core::ContentId,
    alias: &str,
) -> Result<JsonValue> {
    let store = fluree_db_core::content_store_for(storage.clone(), alias);
    let commit = load_commit_by_id(&store, head_id)
        .await
        .map_err(|e| LedgerInfoError::CommitLoad(e.to_string()))?;

    let mut obj = json!({
        "@context": "https://ns.flur.ee/db/v1",
        "type": ["Commit"],
        "id": head_id.to_string(),
        "ledger_id": alias,
    });

    if let Some(id) = &commit.id {
        obj["id"] = json!(id.to_string());
    }

    if let Some(time) = &commit.time {
        obj["time"] = json!(time);
    }

    if !commit.parents.is_empty() {
        let parents: Vec<_> = commit
            .parents
            .iter()
            .map(|r| {
                json!({
                    "type": ["Commit"],
                    "id": r.to_string(),
                })
            })
            .collect();
        obj["parents"] = json!(parents);
    }

    obj["data"] = json!({
        "type": ["DB"],
        "t": commit.t,
    });

    obj["ns"] = json!([{"id": alias}]);

    Ok(obj)
}

/// Convert NsRecord to JSON-LD format for nameservice queries.
pub fn ns_record_to_jsonld(record: &NsRecord) -> JsonValue {
    let ledger_name = split_ledger_id(&record.ledger_id)
        .map(|(ledger, _branch)| ledger)
        .unwrap_or_else(|_| record.name.clone());

    // Use f: prefix so the @id resolves through the @context below, avoiding
    // bare "name:branch" strings that look like unresolved compact IRIs.
    let canonical_id = format!("f:{}", format_ledger_id(&ledger_name, &record.branch));

    let status = if record.retracted {
        "retracted"
    } else {
        "ready"
    };

    let mut obj = json!({
        "@context": { "f": "https://ns.flur.ee/db#" },
        "@id": &canonical_id,
        "@type": ["f:LedgerSource"],
        "f:ledger": { "@id": &ledger_name },
        "f:branch": &record.branch,
        "f:t": record.commit_t,
        "f:status": status,
    });

    if let Some(ref cid) = record.commit_head_id {
        let mut commit_obj = serde_json::Map::new();
        commit_obj.insert("@id".to_string(), json!(cid.to_string()));
        obj["f:ledgerCommit"] = JsonValue::Object(commit_obj);
    }
    if let Some(ref cid) = record.index_head_id {
        let mut index_obj = serde_json::Map::new();
        index_obj.insert("@id".to_string(), json!(cid.to_string()));
        index_obj.insert("f:t".to_string(), json!(record.index_t));
        obj["f:ledgerIndex"] = JsonValue::Object(index_obj);
    }
    if let Some(ref ctx_cid) = record.default_context {
        obj["f:defaultContextCid"] = json!(ctx_cid.to_string());
    }

    obj
}

/// Convert GraphSourceRecord to JSON-LD format for nameservice queries.
pub fn gs_record_to_jsonld(record: &GraphSourceRecord) -> JsonValue {
    let canonical_id = format!("f:{}", format_ledger_id(&record.name, &record.branch));

    let status = if record.retracted {
        "retracted"
    } else {
        "ready"
    };

    let kind_type_str = match record.source_type.kind() {
        fluree_db_nameservice::GraphSourceKind::Index => "f:IndexSource",
        fluree_db_nameservice::GraphSourceKind::Mapped => "f:MappedSource",
        fluree_db_nameservice::GraphSourceKind::Ledger => "f:LedgerSource",
    };

    // Redact any auth/credential leaves before exposing the config verbatim.
    // A graph-source config may carry a resolved OAuth2 client secret / bearer
    // token (an env-var reference is safe, but a literal secret must never reach
    // a client). `redact_graph_source_config` returns the string unchanged when
    // it holds no secrets, so non-secret configs stay byte-identical.
    let redacted_config = redact_graph_source_config(&record.config);
    let mut obj = json!({
        "@context": { "f": "https://ns.flur.ee/db#" },
        "@id": &canonical_id,
        "@type": [kind_type_str, record.source_type.to_type_string()],
        "f:name": &record.name,
        "f:branch": &record.branch,
        "f:status": status,
        "f:graphSourceConfig": { "@value": &redacted_config },
        "f:graphSourceDependencies": &record.dependencies,
    });

    if let Some(ref index_id) = record.index_id {
        obj["f:graphSourceIndex"] = json!(index_id.to_string());
        obj["f:graphSourceIndexT"] = json!(record.index_t);
    }

    obj
}

// ============================================================================
// Graph-source config secret redaction (defense-in-depth)
// ============================================================================

/// Object keys whose values are secrets and must never reach a client. Matched
/// case-insensitively anywhere in a stored graph-source config JSON tree. This
/// mirrors the redacting `Debug` impls added at the config secret leaves
/// (`ConfigValue`, `OAuth2Config`, bearer/vended) for the *serialized* form.
///
/// NOTE: `client_id`, `catalog uri`, `warehouse`, `scope`, and `audience` are
/// intentionally NOT secret (they identify, not authenticate) and are preserved.
const SECRET_CONFIG_KEYS: &[&str] = &[
    "client_secret",
    "token",
    "secret",
    "password",
    "passwd",
    "access_key_id",
    "secret_access_key",
    "session_token",
    "default_val",
];

fn is_secret_config_key(key: &str) -> bool {
    let lower = key.to_ascii_lowercase();
    SECRET_CONFIG_KEYS.iter().any(|s| *s == lower)
}

/// Recursively replace secret-bearing scalar values with `"[redacted]"`.
///
/// A secret held as an env-var reference object (`{"env_var": "...",
/// "default_val": "..."}`) keeps its `env_var` name (safe, aids debugging) while
/// the inline `default_val` fallback is masked by the recursive walk. Returns
/// `true` when anything was redacted.
fn redact_json_secrets(value: &mut JsonValue) -> bool {
    let mut redacted = false;
    match value {
        JsonValue::Object(map) => {
            for (k, v) in map.iter_mut() {
                if is_secret_config_key(k) && !v.is_object() && !v.is_array() && !v.is_null() {
                    *v = JsonValue::String("[redacted]".to_string());
                    redacted = true;
                } else if redact_json_secrets(v) {
                    redacted = true;
                }
            }
        }
        JsonValue::Array(arr) => {
            for v in arr.iter_mut() {
                if redact_json_secrets(v) {
                    redacted = true;
                }
            }
        }
        _ => {}
    }
    redacted
}

/// Redact auth/credential leaves from a stored graph-source config JSON string.
///
/// Returns the original string unchanged when it parses as JSON and contains no
/// secrets, so non-secret configs (e.g. BM25/Vector) stay byte-for-byte
/// identical; otherwise returns a re-serialized, redacted JSON string.
///
/// FAILS CLOSED: input that does not parse as JSON (unreachable today — every
/// stored config is serde-serialized) is replaced wholesale rather than echoed,
/// because a redactor that cannot inspect its input must not forward it.
pub fn redact_graph_source_config(config: &str) -> String {
    match serde_json::from_str::<JsonValue>(config) {
        Ok(mut value) => {
            if redact_json_secrets(&mut value) {
                serde_json::to_string(&value)
                    .unwrap_or_else(|_| "\"[redacted:unserializable]\"".to_string())
            } else {
                config.to_string()
            }
        }
        Err(_) => "\"[redacted:unparseable]\"".to_string(),
    }
}

// ============================================================================
// Virtual (query-in-place) dataset ledger-info
// ============================================================================

/// Human-readable label for a graph-source type (e.g. `"Iceberg"`, `"R2RML"`).
fn graph_source_type_label(source_type: &GraphSourceType) -> String {
    match source_type {
        GraphSourceType::Bm25 => "BM25".to_string(),
        GraphSourceType::Vector => "Vector".to_string(),
        GraphSourceType::Geo => "Geo".to_string(),
        GraphSourceType::R2rml => "R2RML".to_string(),
        GraphSourceType::Iceberg => "Iceberg".to_string(),
        GraphSourceType::Unknown(s) => format!("Unknown({s})"),
    }
}

/// Non-secret source metadata for a virtual dataset, resolved from the
/// graph-source record + compiled mapping + Iceberg table metadata.
///
/// This deliberately carries ONLY identifying metadata — never auth/credentials
/// (no client_secret, token, or secret-ref ever lands here).
#[derive(Debug, Clone, Default)]
pub struct VirtualSourceMeta {
    /// Source type label (`"Iceberg"` / `"R2RML"`).
    pub source_type: String,
    /// Catalog type identifier (e.g. `"polaris"`, `"rest"`) for REST catalogs.
    pub catalog_type: Option<String>,
    /// Catalog base URI for REST catalogs.
    pub catalog_uri: Option<String>,
    /// S3 table location for Direct-mode catalogs.
    pub table_location: Option<String>,
    /// Optional warehouse identifier.
    pub warehouse: Option<String>,
    /// Distinct logical tables referenced by the mapping/config.
    pub tables: Vec<String>,
    /// Current Iceberg snapshot id (used as the virtual dataset "version").
    pub snapshot_id: Option<i64>,
}

/// Sum two optional row counts, treating `None` as "unknown" rather than zero:
/// a known count on either side survives, and the result is `None` only when
/// neither side is known.
fn add_row_counts(acc: Option<i64>, rows: Option<i64>) -> Option<i64> {
    match (acc, rows) {
        (Some(a), Some(r)) => Some(a + r),
        (Some(a), None) => Some(a),
        (None, Some(r)) => Some(r),
        (None, None) => None,
    }
}

/// Compact a datatype IRI to a short form where possible (`xsd:*`, `@id`).
fn compact_datatype_iri(datatype: &str) -> String {
    if let Some(local) = datatype.strip_prefix("http://www.w3.org/2001/XMLSchema#") {
        format!("xsd:{local}")
    } else if datatype == "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString" {
        "rdf:langString".to_string()
    } else {
        datatype.to_string()
    }
}

fn term_type_datatype(term_type: TermType) -> String {
    match term_type {
        TermType::Iri | TermType::BlankNode => "@id".to_string(),
        TermType::Literal => "xsd:string".to_string(),
    }
}

/// Infer the datatype label an object map produces, from its R2RML term type /
/// declared datatype (metadata only — no data is read).
fn object_map_datatype(object_map: &ObjectMap) -> String {
    use fluree_db_r2rml::mapping::ConstantValue;
    match object_map {
        ObjectMap::Column {
            datatype: Some(dt), ..
        } => compact_datatype_iri(dt),
        ObjectMap::Column { term_type, .. } => term_type_datatype(*term_type),
        ObjectMap::Constant { value } => match value {
            ConstantValue::Iri(_) => "@id".to_string(),
            ConstantValue::Literal {
                datatype: Some(dt), ..
            } => compact_datatype_iri(dt),
            ConstantValue::Literal { .. } => "xsd:string".to_string(),
        },
        ObjectMap::Template {
            datatype: Some(dt), ..
        } => compact_datatype_iri(dt),
        ObjectMap::Template { term_type, .. } => term_type_datatype(*term_type),
        ObjectMap::RefObjectMap(_) => "@id".to_string(),
    }
}

#[derive(Default)]
struct PropertyAgg {
    count: Option<i64>,
    datatypes: BTreeMap<String, Option<i64>>,
    /// FK relationship targets (`ref-classes`): for a ref (RefObjectMap)
    /// predicate, the parent triples-map's class IRI(s) → accumulated child row
    /// count. Empty for literal-column properties. Only the per-class
    /// aggregation populates this; the flat `stats.properties` view has no
    /// `ref-classes` field and leaves it empty.
    ref_classes: BTreeMap<String, Option<i64>>,
}

/// Build the native-shaped [`LedgerInfo`] for a virtual (query-in-place)
/// dataset, derived entirely from metadata: the compiled R2RML mapping supplies
/// the classes/properties/datatypes, and `table_row_counts` (authoritative
/// Iceberg manifest row counts — one row = one subject) supplies per-class /
/// per-property counts. NEVER reads data, NEVER emits auth/credentials.
///
/// This is the pure derivation; the async orchestration that resolves the
/// mapping + row counts lives in [`build_graph_source_info`].
pub fn build_virtual_ledger_info(
    record: &GraphSourceRecord,
    mapping: Option<&CompiledR2rmlMapping>,
    meta: &VirtualSourceMeta,
    table_row_counts: &HashMap<String, i64>,
) -> LedgerInfo {
    // Per-class aggregation: class IRI -> (row count, its property membership).
    // The native `/info` derives class->property membership from commit history;
    // a virtual dataset has none, so we derive it deterministically from the
    // compiled R2RML mapping — each triples map contributes its class(es) and, to
    // each, the predicates of its predicate-object maps (with datatypes). This is
    // the map the instance view and the data-model / LLM reader need to render a
    // class's properties; without it they saw only @id.
    #[derive(Default)]
    struct ClassAgg {
        count: Option<i64>,
        properties: BTreeMap<String, PropertyAgg>,
    }
    let mut classes: BTreeMap<String, ClassAgg> = BTreeMap::new();
    let mut properties: BTreeMap<String, PropertyAgg> = BTreeMap::new();

    if let Some(mapping) = mapping {
        for tm in mapping.triples_maps.values() {
            // One row -> one subject: the class/property counts of a triples map
            // are the row count of its logical table.
            let rows = tm
                .table_name()
                .and_then(|t| table_row_counts.get(t).copied());

            // The named predicates this triples map contributes (with datatypes).
            // Only constant predicates can be named without reading data.
            let poms: Vec<(String, String)> = tm
                .predicate_object_maps
                .iter()
                .filter_map(|pom| {
                    pom.predicate_map
                        .as_constant()
                        .map(|pred| (pred.to_string(), object_map_datatype(&pom.object_map)))
                })
                .collect();

            // FK relationship targets (`ref-classes`): for each predicate whose
            // object map is a RefObjectMap (`rr:parentTriplesMap`), the object is
            // the parent map's subject IRI, so the predicate's target class(es)
            // are that parent map's class(es). Resolved from the compiled mapping
            // alone (metadata only) — the same `parentTriplesMap` → parent-map
            // resolution the emitter's round-trip verifier does
            // (`fluree-db-r2rml/src/emit/mod.rs`), keyed here to the parent's
            // `classes()` rather than its `table_name()`. The ref predicate's
            // `@id` datatype already flows through `poms` above; this adds the
            // per-target-class counts the native `/info` carries.
            let ref_targets: Vec<(String, Vec<String>)> = tm
                .predicate_object_maps
                .iter()
                .filter_map(|pom| {
                    let ObjectMap::RefObjectMap(rom) = &pom.object_map else {
                        return None;
                    };
                    let pred = pom.predicate_map.as_constant()?;
                    let parent = mapping.get(&rom.parent_triples_map)?;
                    Some((pred.to_string(), parent.classes().to_vec()))
                })
                .collect();

            for class in tm.classes() {
                let cls = classes.entry(class.clone()).or_default();
                cls.count = add_row_counts(cls.count, rows);
                // Per-class property membership (mirrors the native class->property map).
                for (pred, datatype) in &poms {
                    let agg = cls.properties.entry(pred.clone()).or_default();
                    agg.count = add_row_counts(agg.count, rows);
                    let dt_entry = agg.datatypes.entry(datatype.clone()).or_insert(None);
                    *dt_entry = add_row_counts(*dt_entry, rows);
                }
                // Per-class FK relationship targets: accumulate the child row count
                // onto each ref predicate's target class(es). The predicate entry
                // already exists from the `poms` loop (its `@id` datatype); this
                // fills the `ref-classes` map the native path carries.
                for (pred, targets) in &ref_targets {
                    let agg = cls.properties.entry(pred.clone()).or_default();
                    for target_class in targets {
                        let rc_entry = agg.ref_classes.entry(target_class.clone()).or_insert(None);
                        *rc_entry = add_row_counts(*rc_entry, rows);
                    }
                }
            }

            // Flat, dataset-wide property stats (`stats.properties`), unchanged.
            for (pred, datatype) in poms {
                let agg = properties.entry(pred).or_default();
                agg.count = add_row_counts(agg.count, rows);
                let dt_entry = agg.datatypes.entry(datatype).or_insert(None);
                *dt_entry = add_row_counts(*dt_entry, rows);
            }
        }
    }

    // Per-class stats: BOTH builders construct `ClassInfo`/`PropertyInfo`, so
    // the compiler now enforces that this virtual path fills the same per-class
    // `properties` membership the native path does (the field that drifted).
    let classes: BTreeMap<String, ClassInfo> = classes
        .into_iter()
        .map(|(iri, agg)| {
            let props: BTreeMap<String, PropertyInfo> = agg
                .properties
                .into_iter()
                .map(|(piri, pagg)| {
                    // `langs` is empty (a Phase-1 R2RML mapping carries no
                    // language tags). `ref-classes` carries the FK relationship
                    // targets derived from the mapping's RefObjectMaps. The count
                    // is the child row count; when unknown it degrades to 0 so the
                    // target class (the structural signal) still renders even when
                    // the catalog is offline / row counts time out.
                    (
                        piri,
                        PropertyInfo {
                            types: pagg.datatypes,
                            langs: BTreeMap::new(),
                            ref_classes: pagg
                                .ref_classes
                                .into_iter()
                                .map(|(target_class, count)| (target_class, count.unwrap_or(0)))
                                .collect(),
                        },
                    )
                })
                .collect();
            (
                iri,
                ClassInfo {
                    count: agg.count,
                    subclass_of: Vec::new(),
                    properties: props,
                    // Native emits an ordered `property-list`; a virtual dataset
                    // omits it (the per-class `properties` map is authoritative).
                    property_list: None,
                },
            )
        })
        .collect();

    // Flat, dataset-wide property stats (`stats.properties`).
    let properties: BTreeMap<String, PropertyStat> = properties
        .into_iter()
        .map(|(iri, agg)| {
            (
                iri,
                PropertyStat {
                    count: agg.count,
                    last_modified_t: None,
                    datatypes: Some(agg.datatypes),
                    ndv_values: None,
                    ndv_subjects: None,
                    selectivity_value: None,
                    selectivity_subject: None,
                    sub_property_of: Vec::new(),
                },
            )
        })
        .collect();

    // Total subjects across the dataset (sum of distinct logical-table rows).
    let total_rows: Option<i64> = if table_row_counts.is_empty() {
        None
    } else {
        Some(table_row_counts.values().sum())
    };

    // Source metadata block (identifying only — no credentials).
    let catalog = if let Some(uri) = &meta.catalog_uri {
        Some(Catalog::Rest {
            catalog_type: meta.catalog_type.clone(),
            uri: uri.clone(),
            warehouse: meta.warehouse.clone(),
        })
    } else {
        meta.table_location
            .as_ref()
            .map(|location| Catalog::Direct {
                table_location: location.clone(),
            })
    };
    let source = Source {
        is_virtual: true,
        source_type: meta.source_type.clone(),
        tables: meta.tables.clone(),
        snapshot: meta.snapshot_id,
        catalog,
        table_row_counts: table_row_counts
            .iter()
            .map(|(t, c)| (t.clone(), *c))
            .collect(),
    };

    LedgerInfo {
        // Top-level parity with the native `/info` response (which the server
        // route stamps `ledger_id`/`t` onto for native ledgers). For a virtual
        // dataset the Iceberg snapshot id serves as the version `t`.
        ledger_id: Some(record.graph_source_id.clone()),
        t: Some(meta.snapshot_id),
        ledger: Ledger {
            alias: record.graph_source_id.clone(),
            t: meta.snapshot_id,
            // A virtual dataset has no commit/index chain.
            commit_t: None,
            index_t: None,
            flakes: total_rows,
            size: 0,
            named_graphs: vec![NamedGraph {
                iri: "urn:default".to_string(),
                g_id: 0,
                flakes: total_rows,
                size: 0,
            }],
        },
        graph: "urn:default".to_string(),
        stats: Stats {
            flakes: total_rows,
            size: 0,
            properties,
            classes,
        },
        // A virtual dataset has no commit/index chain.
        commit: JsonValue::Null,
        commit_id: None,
        index_id: None,
        // Redacted nameservice record (config secrets already masked).
        nameservice: Some(gs_record_to_jsonld(record)),
        index: None,
        source: Some(source),
    }
}

/// Thin (redacted) metadata view for a graph source that is not a virtual
/// R2RML/Iceberg dataset (BM25 / Vector / Geo / Unknown). Preserves the
/// historical `/info` stub shape but routes the config through
/// [`redact_graph_source_config`] so no secret can leak.
fn build_generic_graph_source_info(record: &GraphSourceRecord) -> JsonValue {
    let mut obj = json!({
        "name": record.name,
        "branch": record.branch,
        "type": graph_source_type_label(&record.source_type),
        "graph_source_id": record.graph_source_id,
        "retracted": record.retracted,
        "index_t": record.index_t,
    });

    if let Some(ref id) = record.index_id {
        obj["index_id"] = JsonValue::String(id.to_string());
    }
    if !record.dependencies.is_empty() {
        obj["dependencies"] = json!(record.dependencies);
    }
    if !record.config.is_empty() && record.config != "{}" {
        let redacted = redact_graph_source_config(&record.config);
        if let Ok(parsed) = serde_json::from_str::<JsonValue>(&redacted) {
            obj["config"] = parsed;
        }
    }

    obj
}

/// Build `ledger-info` for a graph source (virtual dataset), resolved from a
/// nameservice [`GraphSourceRecord`].
///
/// For `Iceberg`/`R2RML` sources this returns the SAME JSON shape a native
/// ledger's `info` returns — `classes`, `properties`, per-class counts — derived
/// entirely from the compiled R2RML mapping and Iceberg table **metadata**
/// (`loadTable` snapshot summary row counts: metadata-only, NEVER a Parquet/data
/// scan) — and NEVER includes auth/credentials. Other graph-source types get the
/// thin (redacted) record view.
///
/// This is the single shared builder behind the db-server `/info` route, the
/// `LedgerInfoBuilder::execute` path (MCP `get_data_model`), and — by extension —
/// solo's virtual-dataset panels.
pub async fn build_graph_source_info(
    fluree: &crate::Fluree,
    record: &GraphSourceRecord,
) -> crate::Result<JsonValue> {
    match record.source_type {
        GraphSourceType::Iceberg | GraphSourceType::R2rml => {
            #[cfg(feature = "iceberg")]
            {
                build_iceberg_virtual_info(fluree, record).await
            }
            #[cfg(not(feature = "iceberg"))]
            {
                let _ = fluree;
                Ok(build_generic_graph_source_info(record))
            }
        }
        _ => Ok(build_generic_graph_source_info(record)),
    }
}

/// Max in-flight per-table `loadTable` fetches for the virtual `/info` row-count
/// pass. Each is one REST round trip; sharing a single client, 8 clears a
/// 16-table dataset in two waves. Mirrors the generate path's fan-out width.
#[cfg(feature = "iceberg")]
const INFO_COUNT_FETCH_CONCURRENCY: usize = 8;

/// Default wall-clock budget (ms) for the whole virtual `/info` row-count fetch.
/// The structure (classes/properties) is derived from the mapping and needs no
/// counts, so if the catalog is slow the fetch is abandoned and counts degrade to
/// empty rather than blowing the caller's (lambda / gateway) deadline. Override
/// with `FLUREE_ICEBERG_INFO_COUNT_BUDGET_MS` (`0` disables the row-count fetch
/// entirely, returning structure-only).
#[cfg(feature = "iceberg")]
const DEFAULT_INFO_COUNT_BUDGET_MS: u64 = 10_000;

#[cfg(feature = "iceberg")]
fn info_count_budget_ms() -> u64 {
    std::env::var("FLUREE_ICEBERG_INFO_COUNT_BUDGET_MS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(DEFAULT_INFO_COUNT_BUDGET_MS)
}

/// Fetch best-effort per-table row counts (and the first table's snapshot id) for
/// a virtual Iceberg dataset, sharing ONE REST catalog client across a bounded
/// concurrent fan-out.
///
/// The prior path built a fresh client AND OAuth provider (empty token cache) per
/// table inside a SERIAL loop — a redundant token exchange + `loadTable` per
/// table, no keep-alive — which blew the `/info` deadline on a wide dataset. Here
/// the client (and thus its cached OAuth token + HTTPS connection pool) is reused
/// from / warmed into the same process-wide `R2rmlCache` the scan path uses
/// (keyed by the SAME config fingerprint), and the per-table `loadTable`s run with
/// bounded concurrency.
///
/// Metadata-only: `loadTable` returns the snapshot-summary row count; no
/// manifest-list, manifest, or Parquet/data file is read. Every step is
/// best-effort — a parse / auth / load / metadata failure drops only that table's
/// count (logged at debug), never the whole response. A Direct-catalog source (no
/// REST client) returns no counts.
#[cfg(feature = "iceberg")]
async fn fetch_virtual_table_row_counts(
    fluree: &crate::Fluree,
    record: &GraphSourceRecord,
    cfg: &fluree_db_iceberg::IcebergGsConfig,
    tables: &[String],
) -> (HashMap<String, i64>, Option<i64>) {
    use fluree_db_iceberg::catalog::{
        parse_table_identifier, RestCatalogClient, RestCatalogConfig, SendCatalogClient,
    };
    use fluree_db_iceberg::config::CatalogConfig;
    use futures::StreamExt;
    use std::sync::Arc;

    // Only a REST catalog has a client (and token) worth sharing; a Direct
    // (table-location) source has no catalog to query for counts.
    let CatalogConfig::Rest {
        uri,
        warehouse,
        auth,
        ..
    } = &cfg.catalog
    else {
        return (HashMap::new(), None);
    };

    // Reuse (or warm) the process-wide REST client under the SAME cache key the
    // scan path uses, so `/info` and query execution share one OAuth token +
    // connection pool (see graph_source::r2rml::rest_client_cache_key).
    let cache = fluree.r2rml_cache();
    let client_fp =
        crate::graph_source::rest_client_cache_key(&record.graph_source_id, &record.config);
    let catalog: Arc<RestCatalogClient> = match cache.rest_client(&client_fp) {
        Some(c) => c,
        None => {
            let auth_provider = match auth.create_provider_arc() {
                Ok(p) => p,
                Err(e) => {
                    tracing::debug!(error = %e, "virtual ledger-info: auth provider build failed; counts omitted");
                    return (HashMap::new(), None);
                }
            };
            let catalog_config = RestCatalogConfig {
                uri: uri.clone(),
                warehouse: warehouse.clone(),
                ..Default::default()
            };
            match RestCatalogClient::new(catalog_config, auth_provider) {
                Ok(client) => {
                    let client = Arc::new(client);
                    cache.put_rest_client(client_fp, Arc::clone(&client));
                    client
                }
                Err(e) => {
                    tracing::debug!(error = %e, "virtual ledger-info: catalog client build failed; counts omitted");
                    return (HashMap::new(), None);
                }
            }
        }
    };

    let vended = cfg.io.vended_credentials;

    // Fan out `loadTable` across tables with bounded concurrency, sharing the one
    // client. `.buffered` (not `buffer_unordered`) preserves request order, so the
    // snapshot pin (first successful table) is deterministic for the sorted
    // `tables`.
    let per_table: Vec<Option<(String, Option<i64>, i64)>> =
        futures::stream::iter(tables.iter().cloned())
            .map(|table| {
                let catalog = Arc::clone(&catalog);
                async move {
                    let id = parse_table_identifier(&table).ok()?;
                    let api_id = crate::graph_source::TableIdentifier::new(id.namespace, id.table);
                    let catalog_table_id = api_id.to_catalog();
                    let load = match SendCatalogClient::load_table(
                        &*catalog,
                        &catalog_table_id,
                        vended,
                    )
                    .await
                    {
                        Ok(l) => l,
                        Err(e) => {
                            tracing::debug!(table = %table, error = %e, "virtual ledger-info: loadTable failed; count omitted");
                            return None;
                        }
                    };
                    let metadata = load.metadata.as_ref()?;
                    let schema =
                        crate::graph_source::table_schema_from_metadata(&api_id, metadata).ok()?;
                    Some((table, schema.row_count, schema.snapshot.id))
                }
            })
            .buffered(INFO_COUNT_FETCH_CONCURRENCY)
            .collect()
            .await;

    let mut counts = HashMap::new();
    let mut snapshot_id = None;
    for (table, row_count, snap) in per_table.into_iter().flatten() {
        if let Some(rc) = row_count {
            counts.insert(table, rc);
        }
        if snapshot_id.is_none() {
            snapshot_id = Some(snap);
        }
    }
    (counts, snapshot_id)
}

/// Async orchestration for the Iceberg/R2RML virtual-info path: resolve the
/// compiled mapping + best-effort per-table row counts, then derive the JSON via
/// [`build_virtual_ledger_info`].
#[cfg(feature = "iceberg")]
async fn build_iceberg_virtual_info(
    fluree: &crate::Fluree,
    record: &GraphSourceRecord,
) -> crate::Result<JsonValue> {
    use fluree_db_iceberg::config::CatalogConfig;
    use fluree_db_iceberg::IcebergGsConfig;
    use fluree_db_query::r2rml::R2rmlProvider;

    // Parse the stored config for source metadata (auth is never emitted).
    let cfg = IcebergGsConfig::from_json(&record.config).ok();

    // Resolve the compiled mapping (best-effort). A plain Iceberg source with no
    // R2RML mapping yields no classes/properties but still lists its table.
    let provider = crate::graph_source::FlureeR2rmlProvider::new(fluree);
    let mapping = provider
        .compiled_mapping(&record.graph_source_id, None)
        .await
        .ok();

    let mut meta = VirtualSourceMeta {
        source_type: graph_source_type_label(&record.source_type),
        ..Default::default()
    };

    // Distinct tables: union of the mapping's tables and the config's own table.
    let mut tables: Vec<String> = mapping
        .as_deref()
        .map(|m| m.table_names().into_iter().map(str::to_string).collect())
        .unwrap_or_default();
    if let Some(cfg) = cfg.as_ref() {
        match &cfg.catalog {
            CatalogConfig::Rest {
                catalog_type,
                uri,
                warehouse,
                ..
            } => {
                meta.catalog_type = Some(catalog_type.clone());
                meta.catalog_uri = Some(uri.clone());
                meta.warehouse = warehouse.clone();
            }
            CatalogConfig::Direct { table_location } => {
                meta.table_location = Some(table_location.clone());
            }
        }
        let id = cfg.table.identifier();
        if !id.is_empty() && !tables.contains(&id) {
            tables.push(id);
        }
    }
    tables.sort();
    tables.dedup();
    meta.tables = tables.clone();

    // Best-effort per-table row counts + snapshot id via Iceberg metadata only
    // (loadTable snapshot summary — reads no manifest/Parquet data). A single
    // shared catalog client + bounded concurrency replaces the old serial,
    // client-per-table loop; the whole fetch is time-boxed so a slow / offline
    // catalog degrades counts to empty (and snapshot to `None`) while the schema
    // (classes/properties, derived from the mapping) still renders on time.
    let budget_ms = info_count_budget_ms();
    let (counts, fetched_snapshot): (HashMap<String, i64>, Option<i64>) = match cfg.as_ref() {
        Some(cfg) if budget_ms > 0 => {
            match tokio::time::timeout(
                std::time::Duration::from_millis(budget_ms),
                fetch_virtual_table_row_counts(fluree, record, cfg, &tables),
            )
            .await
            {
                Ok(res) => res,
                Err(_) => {
                    tracing::debug!(
                        graph_source = %record.graph_source_id,
                        budget_ms,
                        "virtual ledger-info: row-count fetch exceeded budget; counts omitted"
                    );
                    (HashMap::new(), None)
                }
            }
        }
        _ => (HashMap::new(), None),
    };
    if meta.snapshot_id.is_none() {
        meta.snapshot_id = fetched_snapshot;
    }

    Ok(build_virtual_ledger_info(record, mapping.as_deref(), &meta, &counts).into_json())
}

// ============================================================================
// Stats rendering helpers
// ============================================================================

/// Decode graph-scoped property stats with IRI compaction.
///
/// Uses the `BinaryIndexStore` to resolve p_id -> predicate IRI.
fn decode_graph_property_stats(
    properties: &[GraphPropertyStatEntry],
    compactor: &IriCompactor,
    schema_index: &SchemaIndex,
    store: Option<&BinaryIndexStore>,
    runtime_small_dicts: Option<&RuntimeSmallDicts>,
    include_datatypes: bool,
    include_estimates: bool,
) -> Result<BTreeMap<String, PropertyStat>> {
    let mut result = BTreeMap::new();

    for entry in properties {
        // Resolve p_id to IRI via the binary index store
        let full_iri = store
            .and_then(|store| store.resolve_predicate_iri(entry.p_id))
            .map(str::to_string)
            .or_else(|| {
                runtime_small_dicts
                    .and_then(|dicts| dicts.predicate_sid(RuntimePredicateId::from_u32(entry.p_id)))
                    .and_then(|sid| compactor.decode_sid(sid).ok())
            });
        let Some(full_iri) = full_iri else {
            tracing::debug!(
                p_id = entry.p_id,
                "skipping unknown predicate in graph stats"
            );
            continue;
        };
        let compacted = compactor.compact_vocab_iri(&full_iri);

        // Try to find the SID for schema lookups
        let sid_for_schema = compactor.try_encode_iri(&full_iri);

        // `datatypes` present only when requested (matches historic omit-vs-emit).
        let datatypes = include_datatypes.then(|| {
            entry
                .datatypes
                .iter()
                .map(|(tag, count)| (datatype_display_string(*tag), Some(*count as i64)))
                .collect::<BTreeMap<String, Option<i64>>>()
        });

        let (ndv_values, ndv_subjects, selectivity_value, selectivity_subject) =
            if include_estimates {
                (
                    Some(entry.ndv_values),
                    Some(entry.ndv_subjects),
                    Some(compute_selectivity(entry.count, entry.ndv_values)),
                    Some(compute_selectivity(entry.count, entry.ndv_subjects)),
                )
            } else {
                (None, None, None, None)
            };

        // sub-property-of from schema hierarchy (empty vec is skipped on serialize).
        let sub_property_of = sid_for_schema
            .as_ref()
            .and_then(|sid| schema_index.get(sid))
            .map(|schema_info| {
                schema_info
                    .parent_props
                    .iter()
                    .filter_map(|parent_sid| {
                        compactor
                            .decode_sid(parent_sid)
                            .ok()
                            .map(|iri| compactor.compact_vocab_iri(&iri))
                    })
                    .collect::<Vec<String>>()
            })
            .unwrap_or_default();

        result.insert(
            compacted,
            PropertyStat {
                count: Some(entry.count as i64),
                last_modified_t: Some(entry.last_modified_t),
                datatypes,
                ndv_values,
                ndv_subjects,
                selectivity_value,
                selectivity_subject,
                sub_property_of,
            },
        );
    }

    Ok(result)
}

/// Decode class statistics with IRI compaction, including types/langs/ref-classes.
fn decode_class_stats(
    classes: &Option<Vec<ClassStatEntry>>,
    compactor: &IriCompactor,
    schema_index: &SchemaIndex,
) -> Result<BTreeMap<String, ClassInfo>> {
    let mut result = BTreeMap::new();

    let Some(classes) = classes else {
        return Ok(result);
    };

    for entry in classes {
        let iri = compactor
            .decode_sid(&entry.class_sid)
            .map_err(|e| match e {
                crate::format::FormatError::UnknownNamespace(code) => {
                    LedgerInfoError::UnknownNamespace(code)
                }
                _ => LedgerInfoError::Storage(e.to_string()),
            })?;
        let compacted = compactor.compact_vocab_iri(&iri);

        // subclass-of from schema hierarchy (empty vec is skipped on serialize).
        let subclass_of = schema_index
            .get(&entry.class_sid)
            .map(|schema_info| {
                schema_info
                    .subclass_of
                    .iter()
                    .filter_map(|parent_sid| {
                        compactor
                            .decode_sid(parent_sid)
                            .ok()
                            .map(|iri| compactor.compact_vocab_iri(&iri))
                    })
                    .collect::<Vec<String>>()
            })
            .unwrap_or_default();

        // Decode class->property stats with types/langs/ref-classes.
        let mut properties: BTreeMap<String, PropertyInfo> = BTreeMap::new();
        let mut property_list: Vec<String> = Vec::new();

        for usage in &entry.properties {
            let prop_iri = compactor
                .decode_sid(&usage.property_sid)
                .map_err(|e| match e {
                    crate::format::FormatError::UnknownNamespace(code) => {
                        LedgerInfoError::UnknownNamespace(code)
                    }
                    _ => LedgerInfoError::Storage(e.to_string()),
                })?;
            let prop_compacted = compactor.compact_vocab_iri(&prop_iri);
            property_list.push(prop_compacted.clone());

            // types: per-datatype counts
            let types: BTreeMap<String, Option<i64>> = usage
                .datatypes
                .iter()
                .map(|&(tag, count)| (datatype_display_string(tag), Some(count as i64)))
                .collect();

            // langs: per-language-tag counts
            let langs: BTreeMap<String, i64> = usage
                .langs
                .iter()
                .map(|(lang, count)| (lang.clone(), *count as i64))
                .collect();

            // ref-classes: per-target-class ref counts
            let mut ref_classes: BTreeMap<String, i64> = BTreeMap::new();
            for rc in &usage.ref_classes {
                let class_iri = compactor.decode_sid(&rc.class_sid).map_err(|e| match e {
                    crate::format::FormatError::UnknownNamespace(code) => {
                        LedgerInfoError::UnknownNamespace(code)
                    }
                    _ => LedgerInfoError::Storage(e.to_string()),
                })?;
                let class_compacted = compactor.compact_vocab_iri(&class_iri);
                ref_classes.insert(class_compacted, rc.count as i64);
            }

            properties.insert(
                prop_compacted,
                PropertyInfo {
                    types,
                    langs,
                    ref_classes,
                },
            );
        }

        result.insert(
            compacted,
            ClassInfo {
                count: Some(entry.count as i64),
                subclass_of,
                properties,
                // Native always emits `property-list` (possibly empty).
                property_list: Some(property_list),
            },
        );
    }

    Ok(result)
}

// ============================================================================
// Utility helpers
// ============================================================================

/// Convert a ValueTypeTag raw u8 to a display string suitable for JSON keys.
///
/// Special-cases `JSON_LD_ID` (16) -> `"@id"`. All others use the standard
/// `ValueTypeTag::Display` implementation (e.g., `"xsd:string"`).
fn datatype_display_string(tag: u8) -> String {
    if tag == ValueTypeTag::JSON_LD_ID.as_u8() {
        "@id".to_string()
    } else if tag == ValueTypeTag::VECTOR.as_u8() {
        "@vector".to_string()
    } else if tag == ValueTypeTag::FULL_TEXT.as_u8() {
        "@fulltext".to_string()
    } else {
        ValueTypeTag::from_u8(tag).to_string()
    }
}

/// Compute selectivity: ceil(count/ndv), minimum 1, as INTEGER.
fn compute_selectivity(count: u64, ndv: u64) -> u64 {
    if ndv == 0 {
        1
    } else {
        ((count as f64 / ndv as f64).ceil() as u64).max(1)
    }
}

/// Parse a pre-index stats manifest (JSON) into `GraphStatsEntry` entries.
pub fn parse_pre_index_manifest(bytes: &[u8]) -> std::result::Result<Vec<GraphStatsEntry>, String> {
    let json: JsonValue =
        serde_json::from_slice(bytes).map_err(|e| format!("invalid JSON: {e}"))?;

    let graphs_arr = json
        .get("graphs")
        .and_then(|v| v.as_array())
        .ok_or_else(|| "missing 'graphs' array".to_string())?;

    let mut entries = Vec::with_capacity(graphs_arr.len());
    for g in graphs_arr {
        let g_id = g
            .get("g_id")
            .and_then(serde_json::Value::as_u64)
            .ok_or_else(|| "missing g_id".to_string())? as GraphId;
        let flakes = g
            .get("flakes")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0);
        let size = g
            .get("size")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0);

        let props_arr = g
            .get("properties")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();

        let mut properties = Vec::with_capacity(props_arr.len());
        for p in &props_arr {
            let p_id = p
                .get("p_id")
                .and_then(serde_json::Value::as_u64)
                .ok_or_else(|| "missing p_id".to_string())? as u32;
            let count = p
                .get("count")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0);
            let ndv_values = p
                .get("ndv_values")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0);
            let ndv_subjects = p
                .get("ndv_subjects")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0);
            let last_modified_t = p
                .get("last_modified_t")
                .and_then(serde_json::Value::as_i64)
                .unwrap_or(0);

            let dt_arr = p
                .get("datatypes")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();
            let datatypes: Vec<(u8, u64)> = dt_arr
                .iter()
                .filter_map(|pair| {
                    let arr = pair.as_array()?;
                    if arr.len() == 2 {
                        Some((arr[0].as_u64()? as u8, arr[1].as_u64()?))
                    } else {
                        None
                    }
                })
                .collect();

            properties.push(GraphPropertyStatEntry {
                p_id,
                count,
                ndv_values,
                ndv_subjects,
                last_modified_t,
                datatypes,
            });
        }

        entries.push(GraphStatsEntry {
            g_id,
            flakes,
            size,
            properties,
            classes: None,
        });
    }

    Ok(entries)
}

// ============================================================================
// LedgerInfoBuilder
// ============================================================================

use crate::{ApiError, Fluree};

/// Builder for retrieving comprehensive ledger metadata.
///
/// Created via [`Fluree::ledger_info()`]. Provides a fluent API for configuring
/// and executing ledger info requests.
///
/// # Example
///
/// ```ignore
/// let info = fluree.ledger_info("mydb:main")
///     .with_context(&context)
///     .for_graph("default")
///     .execute()
///     .await?;
/// ```
pub struct LedgerInfoBuilder<'a> {
    fluree: &'a Fluree,
    ledger_id: String,
    context: Option<&'a JsonValue>,
    options: LedgerInfoOptions,
}

impl<'a> LedgerInfoBuilder<'a> {
    /// Create a new builder (called by `Fluree::ledger_info()`).
    pub(crate) fn new(fluree: &'a Fluree, ledger_id: String) -> Self {
        Self {
            fluree,
            ledger_id,
            context: None,
            options: LedgerInfoOptions::default(),
        }
    }

    /// Set the JSON-LD context for IRI compaction in stats.
    pub fn with_context(mut self, context: &'a JsonValue) -> Self {
        self.context = Some(context);
        self
    }

    /// Include datatype breakdowns under `stats.properties[*]`.
    pub fn with_property_datatypes(mut self, enabled: bool) -> Self {
        self.options.include_property_datatypes = enabled;
        self
    }

    /// Include index-derived NDV/selectivity estimates under `stats.properties[*]`.
    ///
    /// These estimates are only as current as the last index refresh.
    pub fn with_property_estimates(mut self, enabled: bool) -> Self {
        self.options.include_property_estimates = enabled;
        self
    }

    /// Toggle the heavier full novelty-aware property/class detail path.
    ///
    /// Enabled by default for `ledger_info()`. Disable this only when you
    /// explicitly want the lighter fast novelty-aware merge.
    ///
    /// Note: this does NOT override `include_property_datatypes` — set that
    /// independently via [`with_property_datatypes`](Self::with_property_datatypes).
    pub fn with_realtime_property_details(mut self, enabled: bool) -> Self {
        self.options.realtime_property_details = enabled;
        self
    }

    /// Select which graph to scope stats to by well-known name.
    ///
    /// - `"default"` -> default graph (g_id = 0)
    /// - `"txn-meta"` -> transaction metadata graph (g_id = 1)
    /// - `"config"` -> ledger config graph (g_id = 2)
    /// - Any other string is tried as a graph IRI
    pub fn for_graph(mut self, name: &str) -> Self {
        self.options.graph = GraphSelector::ByName(name.to_string());
        self
    }

    /// Select which graph to scope stats to by IRI.
    pub fn for_graph_iri(mut self, iri: &str) -> Self {
        self.options.graph = GraphSelector::ByIri(iri.to_string());
        self
    }

    /// Select which graph to scope stats to by numeric graph ID.
    pub fn for_g_id(mut self, g_id: GraphId) -> Self {
        self.options.graph = GraphSelector::ById(g_id);
        self
    }

    /// Execute the ledger info request.
    pub async fn execute(self) -> crate::Result<JsonValue> {
        // A committed ledger takes the native path (shape/value-identical output;
        // the typed refactor re-emits nested stats-map keys in sorted order —
        // same keys/values, no consumer parses positionally). When
        // the id is not a committed ledger, fall back to a graph-source lookup so
        // a virtual (query-in-place) R2RML/Iceberg dataset returns real
        // classes/properties/counts derived from metadata rather than a stub.
        let ledger = match self.fluree.ledger(&self.ledger_id).await {
            Ok(ledger) => ledger,
            Err(e) if e.is_not_found() => {
                if let Ok(Some(gs)) = self
                    .fluree
                    .nameservice()
                    .lookup_graph_source(&self.ledger_id)
                    .await
                {
                    // NOTE(perf, PR-8 catalog floor): this virtual branch
                    // bypasses the leaflet-cache response memoization below, so
                    // every call (e.g. MCP `get_data_model`) redoes the budgeted
                    // loadTable count fan-out. Bounded + low-traffic today;
                    // fold into the response cache keyed on the graph-source
                    // fingerprint + snapshot when the catalog floor lands.
                    return build_graph_source_info(self.fluree, &gs).await;
                }
                return Err(e);
            }
            Err(e) => return Err(e),
        };

        // Optional API-level cache: when ledger caching is enabled, a global LeafletCache
        // exists with a single memory budget (TinyLFU). We store ledger-info response
        // blobs there keyed by (ledger_id, commit_t, index_t, opts, context-hash).
        if let Some(cache) = self
            .fluree
            .ledger_manager()
            .and_then(|mgr| mgr.leaflet_cache())
        {
            let commit_t = ledger.t();
            let index_t = ledger.snapshot.t;
            let index_id = ledger
                .head_index_id
                .as_ref()
                .map(std::string::ToString::to_string)
                .or_else(|| {
                    ledger
                        .ns_record
                        .as_ref()
                        .and_then(|r| r.index_head_id.as_ref())
                        .map(std::string::ToString::to_string)
                })
                .unwrap_or_default();

            let ctx_hash: u64 = match self.context {
                Some(ctx) => {
                    // Stable key across calls: hash the canonical JSON bytes.
                    // This is cheap relative to the novelty merge work we’re caching.
                    let bytes = serde_json::to_vec(ctx).unwrap_or_default();
                    let mut h = std::collections::hash_map::DefaultHasher::new();
                    use std::hash::Hasher;
                    h.write(&bytes);
                    h.finish()
                }
                None => 0,
            };

            let graph_key = match &self.options.graph {
                GraphSelector::Default => "default".to_string(),
                GraphSelector::ById(id) => format!("gid:{id}"),
                GraphSelector::ByIri(iri) => format!("iri:{iri}"),
                GraphSelector::ByName(name) => format!("name:{name}"),
            };

            let key_str = format!(
                "ledger-info:{}:{}:{}:{}:{}:{}:{}:{}:{}",
                self.ledger_id,
                commit_t,
                index_t,
                index_id,
                self.options.realtime_property_details as u8,
                self.options.include_property_datatypes as u8,
                self.options.include_property_estimates as u8,
                graph_key,
                ctx_hash
            );
            let cache_key = xxh3_128(key_str.as_bytes());

            if let Some(bytes) = cache.get_ledger_info(cache_key) {
                if let Ok(json) = serde_json::from_slice::<JsonValue>(&bytes) {
                    return Ok(json);
                }
            }

            let storage = self
                .fluree
                .backend()
                .admin_storage_cloned()
                .ok_or_else(|| {
                    ApiError::config("ledger_info requires a managed storage backend")
                })?;
            let json =
                build_ledger_info_with_options(&ledger, &storage, self.context, self.options)
                    .await
                    .map_err(|e| ApiError::internal(format!("ledger_info failed: {e}")))?;

            if let Ok(vec) = serde_json::to_vec(&json) {
                cache.insert_ledger_info(cache_key, vec.into());
            }

            return Ok(json);
        }

        let storage = self
            .fluree
            .backend()
            .admin_storage_cloned()
            .ok_or_else(|| ApiError::config("ledger_info requires a managed storage backend"))?;
        build_ledger_info_with_options(&ledger, &storage, self.context, self.options)
            .await
            .map_err(|e| ApiError::internal(format!("ledger_info failed: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_selectivity() {
        assert_eq!(compute_selectivity(100, 50), 2);
        assert_eq!(compute_selectivity(100, 100), 1);
        assert_eq!(compute_selectivity(100, 0), 1);
        assert_eq!(compute_selectivity(0, 0), 1);
        assert_eq!(compute_selectivity(3, 2), 2); // ceil(1.5) = 2
        assert_eq!(compute_selectivity(1, 100), 1); // ceil(0.01) = 1, but min is 1
    }

    #[test]
    fn test_datatype_display_string() {
        assert_eq!(datatype_display_string(0), "xsd:string");
        assert_eq!(datatype_display_string(16), "@id");
        assert_eq!(datatype_display_string(7), "xsd:double");
        assert_eq!(datatype_display_string(14), "rdf:langString");
        assert_eq!(datatype_display_string(38), "@vector");
        assert_eq!(datatype_display_string(39), "@fulltext");
    }

    #[test]
    fn test_ns_record_to_jsonld() {
        use fluree_db_core::{ContentId, ContentKind};
        let commit_cid = ContentId::new(ContentKind::Commit, b"abc");
        let index_cid = ContentId::new(ContentKind::IndexRoot, b"def");
        let record = NsRecord {
            ledger_id: "mydb:main".to_string(),
            name: "mydb:main".to_string(),
            branch: "main".to_string(),
            commit_head_id: Some(commit_cid.clone()),
            config_id: None,
            commit_t: 42,
            index_head_id: Some(index_cid),
            index_t: 40,
            default_context: None,
            retracted: false,
            source_branch: None,
            branches: 0,
        };

        let json = ns_record_to_jsonld(&record);

        assert_eq!(json["@id"], "f:mydb:main");
        assert_eq!(json["@type"], json!(["f:LedgerSource"]));
        assert_eq!(json["f:ledger"]["@id"], "mydb");
        assert_eq!(json["f:branch"], "main");
        assert_eq!(json["f:t"], 42);
        assert_eq!(json["f:status"], "ready");
        assert_eq!(json["f:ledgerCommit"]["@id"], commit_cid.to_string());
        assert_eq!(json["f:ledgerIndex"]["f:t"], 40);
    }

    #[test]
    fn test_ns_record_to_jsonld_retracted() {
        use fluree_db_core::{ContentId, ContentKind};
        let commit_cid = ContentId::new(ContentKind::Commit, b"commit-data");
        let record = NsRecord {
            ledger_id: "mydb:main".to_string(),
            name: "mydb:main".to_string(),
            branch: "main".to_string(),
            commit_head_id: Some(commit_cid),
            config_id: None,
            commit_t: 10,
            index_head_id: None,
            index_t: 0,
            default_context: None,
            retracted: true,
            source_branch: None,
            branches: 0,
        };

        let json = ns_record_to_jsonld(&record);
        assert_eq!(json["f:status"], "retracted");
    }

    #[test]
    fn test_gs_record_to_jsonld() {
        use fluree_db_core::{ContentId, ContentKind};
        let index_cid = ContentId::new(ContentKind::IndexRoot, b"snapshot-data");
        let record = GraphSourceRecord {
            graph_source_id: "my-search:main".to_string(),
            name: "my-search".to_string(),
            branch: "main".to_string(),
            source_type: fluree_db_nameservice::GraphSourceType::Bm25,
            config: r#"{"k1":1.2,"b":0.75}"#.to_string(),
            dependencies: vec!["source-ledger:main".to_string()],
            index_id: Some(index_cid.clone()),
            index_t: 42,
            retracted: false,
        };

        let json = gs_record_to_jsonld(&record);

        assert_eq!(json["@id"], "f:my-search:main");
        assert_eq!(json["@type"], json!(["f:IndexSource", "f:Bm25Index"]));
        assert_eq!(json["f:name"], "my-search");
        assert_eq!(json["f:branch"], "main");
        assert_eq!(json["f:status"], "ready");
        assert_eq!(
            json["f:graphSourceConfig"]["@value"],
            r#"{"k1":1.2,"b":0.75}"#
        );
        assert_eq!(
            json["f:graphSourceDependencies"],
            json!(["source-ledger:main"])
        );
        assert_eq!(json["f:graphSourceIndex"], index_cid.to_string());
        assert_eq!(json["f:graphSourceIndexT"], 42);
    }

    #[test]
    fn test_graph_selector_default() {
        assert_eq!(
            resolve_graph_selector(&GraphSelector::Default, None).unwrap(),
            0
        );
    }

    #[test]
    fn test_graph_selector_by_id() {
        assert_eq!(
            resolve_graph_selector(&GraphSelector::ById(3), None).unwrap(),
            3
        );
    }

    #[test]
    fn test_graph_selector_by_name_default() {
        let sel = GraphSelector::ByName("default".to_string());
        assert_eq!(resolve_graph_selector(&sel, None).unwrap(), 0);
    }

    #[test]
    fn test_graph_selector_by_name_txn_meta() {
        let sel = GraphSelector::ByName("txn-meta".to_string());
        assert_eq!(resolve_graph_selector(&sel, None).unwrap(), 1);
    }

    #[test]
    fn test_graph_selector_by_name_config() {
        let sel = GraphSelector::ByName("config".to_string());
        assert_eq!(resolve_graph_selector(&sel, None).unwrap(), 2);
    }

    // ── Virtual (R2RML/Iceberg) dataset ledger-info + secret redaction ──

    const SECRET_PAT: &str = "SUPER_SECRET_PAT_do_not_leak";

    /// An Iceberg/R2RML graph-source config carrying a literal OAuth2 secret —
    /// exactly the shape whose verbatim emission was the P0 leak.
    fn secret_bearing_config() -> String {
        format!(
            r#"{{"catalog":{{"type":"rest","uri":"https://polaris.example.com",
               "auth":{{"type":"oauth2_client_credentials",
               "token_url":"https://polaris.example.com/tokens",
               "client_id":"svc-client","client_secret":"{SECRET_PAT}"}}}},
               "table":"openflights.airlines"}}"#
        )
    }

    fn virtual_record(config: &str) -> GraphSourceRecord {
        GraphSourceRecord {
            graph_source_id: "sales:main".to_string(),
            name: "sales".to_string(),
            branch: "main".to_string(),
            source_type: GraphSourceType::Iceberg,
            config: config.to_string(),
            dependencies: vec![],
            index_id: None,
            index_t: 0,
            retracted: false,
        }
    }

    fn two_table_mapping() -> CompiledR2rmlMapping {
        use fluree_db_r2rml::{PredicateMap, PredicateObjectMap, RefObjectMap, TriplesMap};
        let airline = TriplesMap::new("#Airline", "openflights.airlines")
            .with_subject_template("http://ex.org/airline/{id}")
            .with_class("http://ex.org/Airline")
            .with_predicate_object(PredicateObjectMap {
                predicate_map: PredicateMap::constant("http://ex.org/name"),
                object_map: ObjectMap::column("name"),
            })
            .with_predicate_object(PredicateObjectMap {
                predicate_map: PredicateMap::constant("http://ex.org/founded"),
                object_map: ObjectMap::column_typed(
                    "founded",
                    "http://www.w3.org/2001/XMLSchema#integer",
                ),
            });
        let route = TriplesMap::new("#Route", "openflights.routes")
            .with_subject_template("http://ex.org/route/{id}")
            .with_class("http://ex.org/Route")
            .with_predicate_object(PredicateObjectMap {
                predicate_map: PredicateMap::constant("http://ex.org/distance"),
                object_map: ObjectMap::column("distance"),
            })
            // FK reference: each route points at an airline (parent map `#Airline`),
            // so `ex:airline`'s object is an Airline subject IRI. Drives the
            // per-class `ref-classes` derivation.
            .with_predicate_object(PredicateObjectMap {
                predicate_map: PredicateMap::constant("http://ex.org/airline"),
                object_map: ObjectMap::RefObjectMap(RefObjectMap::new(
                    "#Airline",
                    "airline_id",
                    "id",
                )),
            });
        CompiledR2rmlMapping::new(vec![airline, route])
    }

    fn iceberg_meta() -> VirtualSourceMeta {
        VirtualSourceMeta {
            source_type: "Iceberg".to_string(),
            catalog_type: Some("rest".to_string()),
            catalog_uri: Some("https://polaris.example.com".to_string()),
            table_location: None,
            warehouse: Some("wh1".to_string()),
            tables: vec![
                "openflights.airlines".to_string(),
                "openflights.routes".to_string(),
            ],
            snapshot_id: Some(42),
        }
    }

    #[test]
    fn test_build_virtual_ledger_info_classes_and_counts() {
        let record = virtual_record(&secret_bearing_config());
        let mapping = two_table_mapping();
        let mut counts = HashMap::new();
        counts.insert("openflights.airlines".to_string(), 100);
        counts.insert("openflights.routes".to_string(), 50);

        let info = serde_json::to_value(build_virtual_ledger_info(
            &record,
            Some(&mapping),
            &iceberg_meta(),
            &counts,
        ))
        .unwrap();

        // Non-empty classes with per-class counts = logical-table row counts.
        assert_eq!(
            info["stats"]["classes"]["http://ex.org/Airline"]["count"],
            100
        );
        assert_eq!(info["stats"]["classes"]["http://ex.org/Route"]["count"], 50);

        // Properties with inferred datatypes.
        assert_eq!(
            info["stats"]["properties"]["http://ex.org/name"]["count"],
            100
        );
        assert_eq!(
            info["stats"]["properties"]["http://ex.org/name"]["datatypes"]["xsd:string"],
            100
        );
        assert_eq!(
            info["stats"]["properties"]["http://ex.org/founded"]["datatypes"]["xsd:integer"],
            100
        );

        // Per-class property membership (the map the instance view + the
        // data-model / LLM reader consume; previously absent for virtual
        // datasets, which left classes showing only @id). Mirrors the native
        // class shape: classes[c].properties[p].types[<datatype>] = count.
        assert_eq!(
            info["stats"]["classes"]["http://ex.org/Airline"]["properties"]["http://ex.org/name"]
                ["types"]["xsd:string"],
            100
        );
        assert_eq!(
            info["stats"]["classes"]["http://ex.org/Airline"]["properties"]
                ["http://ex.org/founded"]["types"]["xsd:integer"],
            100
        );
        // Membership is class-scoped: Airline's `founded` is not attributed to Route.
        assert!(
            info["stats"]["classes"]["http://ex.org/Route"]["properties"]
                .get("http://ex.org/founded")
                .is_none(),
            "founded leaked onto Route: {}",
            info["stats"]["classes"]["http://ex.org/Route"]
        );

        // FK relationship targets: Route's `airline` ref property carries a
        // populated `ref-classes` mapping to the parent Airline class, with the
        // child (Route) row count. Derived from the RefObjectMap's
        // `parentTriplesMap` — the relationship the native `/info` exposes.
        assert_eq!(
            info["stats"]["classes"]["http://ex.org/Route"]["properties"]["http://ex.org/airline"]
                ["ref-classes"]["http://ex.org/Airline"],
            50
        );
        // The ref property still carries its `@id` datatype in `types` (the count
        // and the ref-classes count agree: one airline reference per route row).
        assert_eq!(
            info["stats"]["classes"]["http://ex.org/Route"]["properties"]["http://ex.org/airline"]
                ["types"]["@id"],
            50
        );
        // A literal property has NO ref-classes (empty), so the relationship
        // signal is unambiguous.
        assert_eq!(
            info["stats"]["classes"]["http://ex.org/Route"]["properties"]["http://ex.org/distance"]
                ["ref-classes"],
            json!({})
        );

        // Totals + Iceberg snapshot as the version `t`.
        assert_eq!(info["stats"]["flakes"], 150);
        assert_eq!(info["ledger"]["flakes"], 150);
        assert_eq!(info["t"], 42);
        assert_eq!(info["ledger"]["t"], 42);

        // Source metadata block (identifying only).
        assert_eq!(info["source"]["type"], "Iceberg");
        assert_eq!(info["source"]["snapshot"], 42);
        assert_eq!(
            info["source"]["catalog"]["uri"],
            "https://polaris.example.com"
        );
        assert_eq!(info["source"]["virtual"], true);
        assert_eq!(info["graph"], "urn:default");
        assert_eq!(info["commit"], JsonValue::Null);

        // The whole response — including the nameservice block that echoes the
        // stored config — must carry NO secret.
        let serialized = serde_json::to_string(&info).unwrap();
        assert!(
            !serialized.contains(SECRET_PAT),
            "virtual ledger-info leaked the secret: {serialized}"
        );
    }

    #[test]
    fn test_build_virtual_ledger_info_unknown_counts_are_null() {
        let record = virtual_record("{}");
        let mapping = two_table_mapping();
        let empty = HashMap::new();
        let info = serde_json::to_value(build_virtual_ledger_info(
            &record,
            Some(&mapping),
            &iceberg_meta(),
            &empty,
        ))
        .unwrap();

        // Schema still renders (class present) but counts are null (no scan).
        assert!(info["stats"]["classes"]
            .get("http://ex.org/Airline")
            .is_some());
        assert_eq!(
            info["stats"]["classes"]["http://ex.org/Airline"]["count"],
            JsonValue::Null
        );
        assert_eq!(info["stats"]["flakes"], JsonValue::Null);
    }

    // ── Golden byte-for-value fidelity: the typed `LedgerInfo` must serialize to
    //    EXACTLY the JsonValue the pre-refactor hand-built `json!` output did.
    //    Captured from the `482bca261` (pre-refactor) builder for the same two
    //    representative fixtures (full shape w/ counts; null-count shape).
    //    `serde_json::Value` equality is order-insensitive, so this pins the
    //    value shape (null-vs-absent, kebab keys, nested per-class properties).

    /// Golden: full virtual Iceberg dataset — classes + per-class properties +
    /// datatypes + counts + REST catalog + table-row-counts, secret redacted.
    #[test]
    fn test_virtual_ledger_info_golden_full() {
        let record = virtual_record(&secret_bearing_config());
        let mapping = two_table_mapping();
        let mut counts = HashMap::new();
        counts.insert("openflights.airlines".to_string(), 100);
        counts.insert("openflights.routes".to_string(), 50);

        let got = serde_json::to_value(build_virtual_ledger_info(
            &record,
            Some(&mapping),
            &iceberg_meta(),
            &counts,
        ))
        .unwrap();

        let golden: JsonValue = serde_json::from_str(GOLDEN_VIRTUAL_FULL).unwrap();
        assert_eq!(got, golden, "typed LedgerInfo drifted from golden (full)");
    }

    /// Golden: same mapping with NO row counts — schema still renders, every
    /// count is an explicit `null`, `table-row-counts` is `{}`.
    #[test]
    fn test_virtual_ledger_info_golden_null_counts() {
        let record = virtual_record("{}");
        let mapping = two_table_mapping();
        let empty = HashMap::new();

        let got = serde_json::to_value(build_virtual_ledger_info(
            &record,
            Some(&mapping),
            &iceberg_meta(),
            &empty,
        ))
        .unwrap();

        let golden: JsonValue = serde_json::from_str(GOLDEN_VIRTUAL_NULL).unwrap();
        assert_eq!(got, golden, "typed LedgerInfo drifted from golden (null)");
    }

    /// The typed `LedgerInfo` round-trips through JSON (so solo / conformance can
    /// `Deserialize` the wire response back into the shared type).
    #[test]
    fn test_ledger_info_json_roundtrips() {
        let record = virtual_record(&secret_bearing_config());
        let mapping = two_table_mapping();
        let mut counts = HashMap::new();
        counts.insert("openflights.airlines".to_string(), 100);
        counts.insert("openflights.routes".to_string(), 50);

        let built = build_virtual_ledger_info(&record, Some(&mapping), &iceberg_meta(), &counts);
        let as_value = serde_json::to_value(&built).unwrap();
        let back: LedgerInfo = serde_json::from_value(as_value.clone()).unwrap();
        assert_eq!(
            serde_json::to_value(&back).unwrap(),
            as_value,
            "LedgerInfo did not survive a JSON round-trip"
        );
    }

    const GOLDEN_VIRTUAL_FULL: &str = r#"{"ledger_id":"sales:main","t":42,"ledger":{"alias":"sales:main","t":42,"commit-t":null,"index-t":null,"flakes":150,"size":0,"named-graphs":[{"iri":"urn:default","g-id":0,"flakes":150,"size":0}]},"graph":"urn:default","stats":{"flakes":150,"size":0,"properties":{"http://ex.org/airline":{"count":50,"datatypes":{"@id":50}},"http://ex.org/distance":{"count":50,"datatypes":{"xsd:string":50}},"http://ex.org/founded":{"count":100,"datatypes":{"xsd:integer":100}},"http://ex.org/name":{"count":100,"datatypes":{"xsd:string":100}}},"classes":{"http://ex.org/Airline":{"count":100,"properties":{"http://ex.org/founded":{"types":{"xsd:integer":100},"langs":{},"ref-classes":{}},"http://ex.org/name":{"types":{"xsd:string":100},"langs":{},"ref-classes":{}}}},"http://ex.org/Route":{"count":50,"properties":{"http://ex.org/airline":{"types":{"@id":50},"langs":{},"ref-classes":{"http://ex.org/Airline":50}},"http://ex.org/distance":{"types":{"xsd:string":50},"langs":{},"ref-classes":{}}}}}},"commit":null,"nameservice":{"@context":{"f":"https://ns.flur.ee/db#"},"@id":"f:sales:main","@type":["f:MappedSource","f:IcebergMapping"],"f:name":"sales","f:branch":"main","f:status":"ready","f:graphSourceConfig":{"@value":"{\"catalog\":{\"type\":\"rest\",\"uri\":\"https://polaris.example.com\",\"auth\":{\"type\":\"oauth2_client_credentials\",\"token_url\":\"https://polaris.example.com/tokens\",\"client_id\":\"svc-client\",\"client_secret\":\"[redacted]\"}},\"table\":\"openflights.airlines\"}"},"f:graphSourceDependencies":[]},"source":{"virtual":true,"type":"Iceberg","tables":["openflights.airlines","openflights.routes"],"snapshot":42,"catalog":{"type":"rest","uri":"https://polaris.example.com","warehouse":"wh1"},"table-row-counts":{"openflights.airlines":100,"openflights.routes":50}}}"#;

    const GOLDEN_VIRTUAL_NULL: &str = r#"{"ledger_id":"sales:main","t":42,"ledger":{"alias":"sales:main","t":42,"commit-t":null,"index-t":null,"flakes":null,"size":0,"named-graphs":[{"iri":"urn:default","g-id":0,"flakes":null,"size":0}]},"graph":"urn:default","stats":{"flakes":null,"size":0,"properties":{"http://ex.org/airline":{"count":null,"datatypes":{"@id":null}},"http://ex.org/distance":{"count":null,"datatypes":{"xsd:string":null}},"http://ex.org/founded":{"count":null,"datatypes":{"xsd:integer":null}},"http://ex.org/name":{"count":null,"datatypes":{"xsd:string":null}}},"classes":{"http://ex.org/Airline":{"count":null,"properties":{"http://ex.org/founded":{"types":{"xsd:integer":null},"langs":{},"ref-classes":{}},"http://ex.org/name":{"types":{"xsd:string":null},"langs":{},"ref-classes":{}}}},"http://ex.org/Route":{"count":null,"properties":{"http://ex.org/airline":{"types":{"@id":null},"langs":{},"ref-classes":{"http://ex.org/Airline":0}},"http://ex.org/distance":{"types":{"xsd:string":null},"langs":{},"ref-classes":{}}}}}},"commit":null,"nameservice":{"@context":{"f":"https://ns.flur.ee/db#"},"@id":"f:sales:main","@type":["f:MappedSource","f:IcebergMapping"],"f:name":"sales","f:branch":"main","f:status":"ready","f:graphSourceConfig":{"@value":"{}"},"f:graphSourceDependencies":[]},"source":{"virtual":true,"type":"Iceberg","tables":["openflights.airlines","openflights.routes"],"snapshot":42,"catalog":{"type":"rest","uri":"https://polaris.example.com","warehouse":"wh1"},"table-row-counts":{}}}"#;

    #[test]
    fn test_redact_graph_source_config_masks_oauth_secret() {
        let redacted = redact_graph_source_config(&secret_bearing_config());
        assert!(!redacted.contains(SECRET_PAT), "secret leaked: {redacted}");
        // Non-secret identifying fields survive.
        assert!(redacted.contains("https://polaris.example.com"));
        assert!(redacted.contains("svc-client"));
        assert!(redacted.contains("[redacted]"));
    }

    #[test]
    fn test_redact_graph_source_config_masks_env_default_but_keeps_var_name() {
        let config = r#"{"catalog":{"type":"rest","uri":"https://c.example.com",
            "auth":{"type":"bearer","token":{"env_var":"POLARIS_TOKEN",
            "default_val":"fallback-secret-value"}}},"table":"ns.t"}"#;
        let redacted = redact_graph_source_config(config);
        assert!(
            redacted.contains("POLARIS_TOKEN"),
            "env var name should survive: {redacted}"
        );
        assert!(
            !redacted.contains("fallback-secret-value"),
            "inline default secret leaked: {redacted}"
        );
    }

    #[test]
    fn test_redact_graph_source_config_noop_for_nonsecret_is_byte_identical() {
        // A BM25 config has no secret keys -> returned unchanged (preserves the
        // exact `gs_record_to_jsonld` output the existing test locks in).
        let bm25 = r#"{"k1":1.2,"b":0.75}"#;
        assert_eq!(redact_graph_source_config(bm25), bm25);
    }

    #[test]
    fn test_gs_record_to_jsonld_redacts_secret_config() {
        let record = virtual_record(&secret_bearing_config());
        let json = gs_record_to_jsonld(&record);
        let serialized = serde_json::to_string(&json).unwrap();
        assert!(
            !serialized.contains(SECRET_PAT),
            "gs_record_to_jsonld leaked the secret: {serialized}"
        );
    }

    /// The lossless storage serialization of a real `IcebergGsConfig` DOES carry
    /// the secret (required for query-time catalog auth), but the redacted
    /// emission MUST NOT — the exact invariant a client-facing response needs.
    #[cfg(feature = "iceberg")]
    #[test]
    fn test_real_iceberg_config_redacts_on_emit() {
        use fluree_db_iceberg::auth::AuthConfig;
        use fluree_db_iceberg::config::{CatalogConfig, IoConfig, TableConfig};
        use fluree_db_iceberg::{ConfigValue, IcebergGsConfig};

        let cfg = IcebergGsConfig {
            catalog: CatalogConfig::Rest {
                catalog_type: "rest".to_string(),
                uri: "https://polaris.example.com".to_string(),
                auth: AuthConfig::OAuth2ClientCredentials {
                    token_url: "https://polaris.example.com/tokens".to_string(),
                    client_id: ConfigValue::literal("svc-client"),
                    client_secret: ConfigValue::literal(SECRET_PAT),
                    scope: Some("PRINCIPAL_ROLE:ALL".to_string()),
                    audience: None,
                },
                warehouse: None,
            },
            table: TableConfig::Identifier("ns.t".to_string()),
            io: IoConfig::default(),
            mapping: None,
        };

        let stored = cfg.to_json().unwrap();
        assert!(
            stored.contains(SECRET_PAT),
            "storage serialization must be lossless"
        );

        let redacted = redact_graph_source_config(&stored);
        assert!(
            !redacted.contains(SECRET_PAT),
            "secret leaked on emit: {redacted}"
        );
        assert!(redacted.contains("https://polaris.example.com"));
        assert!(redacted.contains("svc-client"));
    }

    /// The redactor FAILS CLOSED: input it cannot parse as JSON (unreachable
    /// today — every stored config is serde-serialized) is replaced wholesale,
    /// never echoed. A redactor that cannot inspect its input must not forward it.
    #[test]
    fn test_redact_graph_source_config_fails_closed_on_non_json() {
        let out = redact_graph_source_config("client_secret=oops-not-json");
        assert!(
            !out.contains("oops-not-json"),
            "non-JSON input must not be echoed: {out}"
        );
        assert!(out.contains("redacted"), "placeholder expected: {out}");
    }

    /// VALUE-PATTERN BACKSTOP over EVERY `AuthConfig` variant: serialize each
    /// through the real storage path (`IcebergGsConfig::to_json`) with sentinel
    /// secrets in every secret-capable field (literal AND env-var-default
    /// `ConfigValue` forms) and assert redaction strips every sentinel. The
    /// wildcard-free `match` below is deliberate: adding a new `AuthConfig`
    /// variant stops this test compiling, forcing an explicit redaction
    /// decision (allowlist entry + a case here) before the variant can ship —
    /// the compile-time guard `is_secret_config_key`'s exact-match allowlist
    /// otherwise lacks.
    #[cfg(feature = "iceberg")]
    #[test]
    fn test_every_auth_config_variant_redacts_on_emit() {
        use fluree_db_iceberg::auth::AuthConfig;
        use fluree_db_iceberg::config::{CatalogConfig, IoConfig, TableConfig};
        use fluree_db_iceberg::{ConfigValue, IcebergGsConfig};

        const SENTINEL_A: &str = "sentinel-secret-value-AAAA";
        const SENTINEL_B: &str = "sentinel-secret-value-BBBB";

        let auths: Vec<AuthConfig> = vec![
            AuthConfig::None,
            AuthConfig::Bearer {
                token: ConfigValue::literal(SENTINEL_A),
            },
            AuthConfig::Bearer {
                token: ConfigValue::Dynamic {
                    env_var: Some("MY_TOKEN".to_string()),
                    java_property: None,
                    default_val: Some(SENTINEL_B.to_string()),
                },
            },
            AuthConfig::OAuth2ClientCredentials {
                token_url: "https://c.example.com/tokens".to_string(),
                client_id: ConfigValue::literal("svc-client"),
                client_secret: ConfigValue::Dynamic {
                    env_var: Some("MY_SECRET".to_string()),
                    java_property: None,
                    default_val: Some(SENTINEL_A.to_string()),
                },
                scope: Some("PRINCIPAL_ROLE:ALL".to_string()),
                audience: None,
            },
        ];

        // Exhaustiveness canary — NO wildcard arm. A new AuthConfig variant
        // fails compilation here until it is covered above and its secret
        // fields earn allowlist entries.
        for auth in &auths {
            match auth {
                AuthConfig::None
                | AuthConfig::Bearer { .. }
                | AuthConfig::OAuth2ClientCredentials { .. } => {}
            }
        }

        for auth in auths {
            let cfg = IcebergGsConfig {
                catalog: CatalogConfig::Rest {
                    catalog_type: "rest".to_string(),
                    uri: "https://polaris.example.com".to_string(),
                    auth,
                    warehouse: None,
                },
                table: TableConfig::Identifier("ns.t".to_string()),
                io: IoConfig::default(),
                mapping: None,
            };
            let stored = cfg.to_json().unwrap();
            let redacted = redact_graph_source_config(&stored);
            assert!(
                !redacted.contains(SENTINEL_A) && !redacted.contains(SENTINEL_B),
                "sentinel secret survived redaction: {redacted}"
            );
            // Env-var NAMES survive (they identify, not authenticate).
            if stored.contains("MY_TOKEN") {
                assert!(
                    redacted.contains("MY_TOKEN"),
                    "env var name lost: {redacted}"
                );
            }
            if stored.contains("MY_SECRET") {
                assert!(
                    redacted.contains("MY_SECRET"),
                    "env var name lost: {redacted}"
                );
            }
        }
    }
}
