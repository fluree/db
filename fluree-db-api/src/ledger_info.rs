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
use fluree_db_nameservice::{GraphSourceRecord, NsRecord};
use fluree_db_novelty::{
    assemble_fast_stats, assemble_full_stats, StatsAssemblyError, StatsLookup,
};
use fluree_graph_json_ld::ParsedContext;
use serde_json::{json, Map, Value as JsonValue};
use std::collections::HashMap;
use std::sync::Arc;
use xxhash_rust::xxh3::xxh3_128;

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
    let compactor = IriCompactor::new(ledger.snapshot.namespaces(), &parsed_context);

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

    // Build the response
    let mut result = Map::new();

    // 1. Ledger block (ledger-wide metadata)
    result.insert(
        "ledger".to_string(),
        build_ledger_block(ledger, &stats, binary_store.as_deref()),
    );

    // 2. Graph name
    result.insert("graph".to_string(), json!(graph_name));

    // 3. Graph-scoped stats section
    result.insert(
        "stats".to_string(),
        build_graph_scoped_stats(
            g_id,
            &stats,
            &compactor,
            &schema_index,
            binary_store.as_deref(),
            Some(&ledger.runtime_small_dicts),
            options.include_property_datatypes,
            options.include_property_estimates,
        )?,
    );

    // 4. Commit section (ALWAYS include, even if None)
    if let Some(head_cid) = &ledger.head_commit_id {
        match build_commit_jsonld(storage, head_cid, &ledger.snapshot.ledger_id).await {
            Ok(commit_json) => {
                result.insert("commit".to_string(), commit_json);
            }
            Err(e) => {
                result.insert("commit".to_string(), json!({ "error": format!("{}", e) }));
            }
        }
    } else {
        result.insert("commit".to_string(), JsonValue::Null);
    }

    // Include content identifiers when available
    if let Some(ref cid) = ledger.head_commit_id {
        result.insert("commitId".to_string(), json!(cid.to_string()));
    }
    if let Some(ref cid) = ledger.head_index_id {
        result.insert("indexId".to_string(), json!(cid.to_string()));
    }

    // 5. Nameservice section
    if let Some(ns_record) = &ledger.ns_record {
        result.insert("nameservice".to_string(), ns_record_to_jsonld(ns_record));
    }

    // 6. Index section (if available)
    if let Some(ns_record) = &ledger.ns_record {
        if ns_record.index_head_id.is_some() || ns_record.index_t > 0 {
            let mut index_obj = json!({
                "t": ns_record.index_t,
            });
            if let Some(ref cid) = ledger.head_index_id {
                index_obj["id"] = json!(cid.to_string());
            } else if let Some(ref cid) = ns_record.index_head_id {
                index_obj["id"] = json!(cid.to_string());
            }
            result.insert("index".to_string(), index_obj);
        }
    }

    Ok(JsonValue::Object(result))
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
fn build_ledger_block(
    ledger: &LedgerState,
    stats: &IndexStats,
    store: Option<&BinaryIndexStore>,
) -> JsonValue {
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

    // Build named-graphs list (include per-graph flakes/size when available).
    // The "iri" is the full graph IRI, usable directly in FROM / FROM NAMED clauses.
    let mut named_graphs = Vec::new();
    // Always include default graph
    let (default_flakes, default_size) = graph_totals(0);
    named_graphs.push(json!({
        "iri": "urn:default",
        "g-id": 0,
        "flakes": default_flakes,
        "size": default_size,
    }));
    // Add named graphs from binary store (including txn-meta and config)
    if let Some(store) = store {
        for (g_id, iri) in store.graph_entries() {
            let (flakes, size) = graph_totals(g_id);
            named_graphs.push(json!({
                "iri": iri,
                "g-id": g_id,
                "flakes": flakes,
                "size": size,
            }));
        }
    }

    json!({
        "alias": &ledger.snapshot.ledger_id,
        "t": ledger.t(),
        "commit-t": commit_t,
        "index-t": index_t,
        "flakes": stats.flakes,
        "size": stats.size,
        "named-graphs": named_graphs,
    })
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
) -> Result<JsonValue> {
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
        JsonValue::Object(Map::new())
    };

    // Classes: always from graph-scoped GraphStatsEntry.
    let classes = if let Some(entry) = graph_entry {
        decode_class_stats(&entry.classes, compactor, schema_index)?
    } else {
        JsonValue::Object(Map::new())
    };

    Ok(json!({
        "flakes": graph_flakes,
        "size": graph_size,
        "properties": properties,
        "classes": classes,
    }))
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

    if !commit.previous_refs.is_empty() {
        let parents: Vec<_> = commit
            .previous_refs
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

    let mut obj = json!({
        "@context": { "f": "https://ns.flur.ee/db#" },
        "@id": &canonical_id,
        "@type": [kind_type_str, record.source_type.to_type_string()],
        "f:name": &record.name,
        "f:branch": &record.branch,
        "f:status": status,
        "f:graphSourceConfig": { "@value": &record.config },
        "f:graphSourceDependencies": &record.dependencies,
    });

    if let Some(ref index_id) = record.index_id {
        obj["f:graphSourceIndex"] = json!(index_id.to_string());
        obj["f:graphSourceIndexT"] = json!(record.index_t);
    }

    obj
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
) -> Result<JsonValue> {
    let mut result = Map::new();

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

        let mut prop_obj = Map::new();
        prop_obj.insert("count".to_string(), json!(entry.count));
        prop_obj.insert("last-modified-t".to_string(), json!(entry.last_modified_t));

        if include_datatypes {
            let mut dts = Map::new();
            for (tag, count) in &entry.datatypes {
                let label = datatype_display_string(*tag);
                dts.insert(label, json!(*count));
            }
            prop_obj.insert("datatypes".to_string(), JsonValue::Object(dts));
        }

        if include_estimates {
            prop_obj.insert("ndv-values".to_string(), json!(entry.ndv_values));
            prop_obj.insert("ndv-subjects".to_string(), json!(entry.ndv_subjects));
            prop_obj.insert(
                "selectivity-value".to_string(),
                json!(compute_selectivity(entry.count, entry.ndv_values)),
            );
            prop_obj.insert(
                "selectivity-subject".to_string(),
                json!(compute_selectivity(entry.count, entry.ndv_subjects)),
            );
        }

        // Add sub-property-of from schema hierarchy
        if let Some(sid) = &sid_for_schema {
            if let Some(schema_info) = schema_index.get(sid) {
                if !schema_info.parent_props.is_empty() {
                    let parent_iris: Vec<String> = schema_info
                        .parent_props
                        .iter()
                        .filter_map(|parent_sid| {
                            compactor
                                .decode_sid(parent_sid)
                                .ok()
                                .map(|iri| compactor.compact_vocab_iri(&iri))
                        })
                        .collect();
                    if !parent_iris.is_empty() {
                        prop_obj.insert("sub-property-of".to_string(), json!(parent_iris));
                    }
                }
            }
        }

        result.insert(compacted, JsonValue::Object(prop_obj));
    }

    Ok(JsonValue::Object(result))
}

/// Decode class statistics with IRI compaction, including types/langs/ref-classes.
fn decode_class_stats(
    classes: &Option<Vec<ClassStatEntry>>,
    compactor: &IriCompactor,
    schema_index: &SchemaIndex,
) -> Result<JsonValue> {
    let mut result = Map::new();

    let Some(classes) = classes else {
        return Ok(JsonValue::Object(result));
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

        let mut class_obj = Map::new();
        class_obj.insert("count".to_string(), json!(entry.count));

        // Add subclass-of from schema hierarchy
        if let Some(schema_info) = schema_index.get(&entry.class_sid) {
            if !schema_info.subclass_of.is_empty() {
                let parent_iris: Vec<String> = schema_info
                    .subclass_of
                    .iter()
                    .filter_map(|parent_sid| {
                        compactor
                            .decode_sid(parent_sid)
                            .ok()
                            .map(|iri| compactor.compact_vocab_iri(&iri))
                    })
                    .collect();
                if !parent_iris.is_empty() {
                    class_obj.insert("subclass-of".to_string(), json!(parent_iris));
                }
            }
        }

        // Decode class->property stats with types/langs/ref-classes
        let mut props_map = Map::new();
        let mut props_list: Vec<JsonValue> = Vec::new();

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
            props_list.push(json!(prop_compacted.clone()));

            let mut prop_obj = Map::new();

            // types: per-datatype counts
            let mut types_obj = Map::new();
            for &(tag, count) in &usage.datatypes {
                let label = datatype_display_string(tag);
                types_obj.insert(label, json!(count));
            }
            prop_obj.insert("types".to_string(), JsonValue::Object(types_obj));

            // langs: per-language-tag counts
            let mut langs_obj = Map::new();
            for (lang, count) in &usage.langs {
                langs_obj.insert(lang.clone(), json!(*count));
            }
            prop_obj.insert("langs".to_string(), JsonValue::Object(langs_obj));

            // ref-classes: per-target-class ref counts
            let mut refs_obj = Map::new();
            for rc in &usage.ref_classes {
                let class_iri = compactor.decode_sid(&rc.class_sid).map_err(|e| match e {
                    crate::format::FormatError::UnknownNamespace(code) => {
                        LedgerInfoError::UnknownNamespace(code)
                    }
                    _ => LedgerInfoError::Storage(e.to_string()),
                })?;
                let class_compacted = compactor.compact_vocab_iri(&class_iri);
                refs_obj.insert(class_compacted, json!(rc.count));
            }
            prop_obj.insert("ref-classes".to_string(), JsonValue::Object(refs_obj));

            props_map.insert(prop_compacted, JsonValue::Object(prop_obj));
        }

        class_obj.insert("properties".to_string(), JsonValue::Object(props_map));
        class_obj.insert("property-list".to_string(), JsonValue::Array(props_list));

        result.insert(compacted, JsonValue::Object(class_obj));
    }

    Ok(JsonValue::Object(result))
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
        let ledger = self.fluree.ledger(&self.ledger_id).await?;

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
}
