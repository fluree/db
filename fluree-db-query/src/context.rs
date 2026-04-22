//! Execution context for query operators
//!
//! The `ExecutionContext` provides access to database state and configuration
//! needed by operators during execution.

use crate::bm25::{Bm25IndexProvider, Bm25SearchProvider};
use crate::dataset::{ActiveGraph, ActiveGraphs, DataSet};
use crate::error::QueryError;
use crate::policy::QueryPolicyEnforcer;
use crate::r2rml::{R2rmlProvider, R2rmlTableProvider};
use crate::remote_service::RemoteServiceExecutor;
use crate::var_registry::VarRegistry;
use crate::vector::VectorIndexProvider;
use fluree_db_binary_index::{BinaryGraphView, BinaryIndexStore, FulltextArena};
use fluree_db_core::dict_novelty::DictNovelty;
use fluree_db_core::{
    GraphDbRef, GraphId, LedgerSnapshot, NoOverlay, OverlayProvider, RuntimeSmallDicts, Sid,
    Tracker,
};

use crate::binary_range::BinaryRangeProvider;
use fluree_db_spatial::SpatialIndexProvider;
use fluree_vocab::namespaces::{FLUREE_DB, JSON_LD, OGC_GEO, RDF, XSD};
use fluree_vocab::{geo_names, xsd_names};
use std::collections::HashMap;
use std::sync::Arc;

/// Map from `(graph_id, predicate_id, lang_id)` to fulltext BoW arenas used
/// by `fulltext()` BM25 scoring.
pub type FulltextProviders = HashMap<(GraphId, u32, u16), Arc<FulltextArena>>;

/// Execution context providing access to database and query state.
///
/// # Dataset Support
///
/// When `dataset` is `Some`, the context supports multi-graph queries:
/// - `active_graph` indicates which graph(s) are currently being scanned
/// - `with_active_graph()` creates a new context targeting a specific named graph
/// - Operators should use `active_graphs()` to get the appropriate graph(s) to scan
///
/// When `dataset` is `None`, this is single-db mode and operators use `active_snapshot`/`overlay()`/`to_t`.
///
pub struct ExecutionContext<'a> {
    /// The snapshot used for data access in the current execution scope.
    ///
    /// In single-graph mode this is the primary db snapshot. In per-graph
    /// contexts (created by [`with_graph_ref`](Self::with_graph_ref)) it is
    /// replaced with the per-graph snapshot. See [`original_snapshot`](Self::original_snapshot)
    /// for the stable primary reference used for pattern SID decoding.
    pub active_snapshot: &'a LedgerSnapshot,
    /// Variable registry for this query
    pub vars: &'a VarRegistry,
    /// Target transaction time (for time-travel queries)
    pub to_t: i64,
    /// Optional start time for history range queries
    pub from_t: Option<i64>,
    /// Optional overlay provider (novelty); None means no overlay
    pub overlay: Option<&'a dyn OverlayProvider>,
    /// Maximum batch size for operators
    pub batch_size: usize,
    /// Optional policy enforcer for async policy evaluation with f:query support
    ///
    /// When present, scan operators should use this for per-leaf batch filtering
    /// via `filter_flakes`. This provides full f:query support without deadlocks.
    pub policy_enforcer: Option<Arc<QueryPolicyEnforcer>>,
    /// Optional BM25 index provider for `Pattern::IndexSearch` (legacy, returns raw index)
    pub bm25_provider: Option<&'a dyn Bm25IndexProvider>,
    /// Optional BM25 search provider for `Pattern::IndexSearch` (preferred, returns search results)
    ///
    /// This is the preferred provider for the search service protocol. When set, the operator
    /// uses this instead of `bm25_provider`. Use [`EmbeddedBm25SearchProvider`] to wrap
    /// a `Bm25IndexProvider` for embedded mode.
    pub bm25_search_provider: Option<&'a dyn Bm25SearchProvider>,
    /// Optional vector index provider for `Pattern::VectorSearch`
    pub vector_provider: Option<&'a dyn VectorIndexProvider>,
    /// Optional R2RML mapping provider for `Pattern::R2rml`
    pub r2rml_provider: Option<&'a dyn R2rmlProvider>,
    /// Optional R2RML table provider for Iceberg table scanning
    pub r2rml_table_provider: Option<&'a dyn R2rmlTableProvider>,
    /// Optional dataset for multi-graph queries
    pub dataset: Option<&'a DataSet<'a>>,
    /// Currently active graph (Default or Named) - only meaningful when dataset is Some
    pub active_graph: ActiveGraph,
    /// Optional execution tracker (time/fuel/policy)
    pub tracker: Tracker,
    /// History mode flag - when true, includes both assertions and retractions
    /// and captures the op (operation) metadata in bindings for @op support.
    pub history_mode: bool,
    /// When true, bind evaluation errors are treated as query errors.
    pub strict_bind_errors: bool,
    /// Optional binary columnar index store for fast local-file scans.
    ///
    /// When present, scan operators use the binary cursor path for queries
    /// against the binary columnar indexes. When absent, scans fall back
    /// to `range_with_overlay()`.
    pub binary_store: Option<Arc<BinaryIndexStore>>,
    /// Graph ID for binary index scans (typically 0 for default graph).
    pub binary_g_id: GraphId,
    /// Dictionary novelty layer for subject/string lookups in binary scans.
    ///
    /// When present, `DictOverlay` delegates subject/string lookups to this
    /// shared layer (populated during commit). When absent, an uninitialized
    /// `DictNovelty` is used as fallback (routes everything to persisted tree).
    pub dict_novelty: Option<Arc<DictNovelty>>,
    /// Snapshot-derived namespace codes used when a novelty subject resolves to
    /// a namespace introduced after the attached binary store was loaded.
    pub namespace_codes_fallback: Option<Arc<HashMap<u16, String>>>,
    /// Ledger-scoped runtime IDs for predicates and datatypes.
    pub runtime_small_dicts: Option<&'a RuntimeSmallDicts>,
    /// Optional spatial index providers for `Pattern::S2Search`.
    ///
    /// Keys are graph-scoped: `"g{g_id}:{predicate_iri}"`.
    /// The S2SearchOperator routes queries to the appropriate provider based on
    /// the graph ID and predicate.
    pub spatial_providers: Option<&'a HashMap<String, Arc<dyn SpatialIndexProvider>>>,
    /// Optional fulltext BoW arenas for `fulltext()` function BM25 scoring.
    ///
    /// Keys are `(g_id, p_id, lang_id)` triples — one arena per language on
    /// each property. `@fulltext`-datatype and configured-English content
    /// share the same bucket under `"en"`'s dict-assigned lang_id.
    /// When present, `eval_fulltext` uses arena-based BM25 scoring with
    /// corpus-wide IDF and avgdl stats. When absent, falls back to
    /// per-document TF-saturation scoring.
    pub fulltext_providers: Option<&'a FulltextProviders>,
    /// Dict-assigned lang_id for BCP-47 `"en"`, resolved at context
    /// construction time from the binary index's language dict.
    ///
    /// Used as the arena-lookup key for `@fulltext`-datatype values (which
    /// carry no row lang tag, so `lang_id == 0`) and as the final fallback
    /// in the language-resolution chain for configured full-text properties.
    /// `None` means the ledger has no indexed English content.
    pub english_lang_id: Option<u16>,
    /// Optional remote SERVICE executor for `fluree:remote:` endpoints.
    ///
    /// When present, `ServiceOperator` can execute SPARQL queries against
    /// remote Fluree instances via HTTP. Populated from connection-level
    /// remote endpoint registration.
    pub remote_service: Option<&'a dyn RemoteServiceExecutor>,
    /// Set of ledger IDs that are backed by R2RML graph sources.
    ///
    /// Precomputed at context setup. When a scan operator encounters a graph
    /// with its `ledger_id` in this set, it routes triple patterns through
    /// R2RML scan instead of native index scan.
    pub r2rml_graph_ids: std::collections::HashSet<Arc<str>>,
    /// Cached result of the multi-ledger check.
    ///
    /// `true` when the active query scope spans more than one distinct ledger ID.
    /// Computed once at construction time (and recomputed when `active_graph` changes
    /// via `with_active_graph` / `with_default_graph`). This avoids re-iterating
    /// active graphs on every per-row call site.
    multi_ledger: bool,
    /// When true, `BinaryScanOperator` always returns resolved
    /// `Binding::Sid`/`Lit` instead of late-materialized `EncodedSid`/`EncodedLit`.
    ///
    /// Propagated from [`GraphDbRef::eager`]. Use for infrastructure queries
    /// (config resolution, policy loading) that call `binding.as_sid()` /
    /// `binding.as_lit()` directly.
    pub eager_materialization: bool,
    /// The snapshot this context was originally constructed from.
    ///
    /// Equal to `active_snapshot` in the common single-graph case. In per-graph
    /// contexts (created by [`with_graph_ref`](Self::with_graph_ref)),
    /// `active_snapshot` is replaced with the per-graph snapshot for data access
    /// while this field preserves the parent's snapshot so that pattern
    /// SIDs — encoded in the original namespace space — can be decoded
    /// correctly (see `reencode_sid` in `build_match_val_for_snapshot`).
    pub original_snapshot: &'a LedgerSnapshot,
}

impl<'a> ExecutionContext<'a> {
    /// Create a new execution context
    pub fn new(snapshot: &'a LedgerSnapshot, vars: &'a VarRegistry) -> Self {
        Self {
            active_snapshot: snapshot,
            vars,
            to_t: snapshot.t,
            from_t: None,
            overlay: None,
            batch_size: 1000, // Default batch size
            policy_enforcer: None,
            bm25_provider: None,
            bm25_search_provider: None,
            vector_provider: None,
            r2rml_provider: None,
            r2rml_table_provider: None,
            dataset: None,
            active_graph: ActiveGraph::Default,
            tracker: Tracker::disabled(),
            history_mode: false,
            strict_bind_errors: false,
            binary_store: None,
            binary_g_id: 0,
            dict_novelty: None,
            namespace_codes_fallback: None,
            runtime_small_dicts: None,
            spatial_providers: None,
            fulltext_providers: None,
            english_lang_id: None,
            remote_service: None,
            r2rml_graph_ids: std::collections::HashSet::new(),
            multi_ledger: false,
            eager_materialization: false,
            original_snapshot: snapshot,
        }
    }

    /// Create from a `GraphDbRef`, pulling snapshot, graph id, overlay, and `to_t`.
    ///
    /// If the snapshot carries a `RangeProvider`, the binary store is extracted
    /// via a single `TypeId` downcast so that `BinaryScanOperator` can use it.
    pub fn from_graph_db_ref(db: GraphDbRef<'a>, vars: &'a VarRegistry) -> Self {
        let binary_store = Self::extract_binary_store(db.snapshot);
        let dict_novelty = Self::extract_dict_novelty(db.snapshot);
        let namespace_codes_fallback = binary_store
            .as_ref()
            .map(|_| Arc::new(db.snapshot.namespaces().clone()));
        let runtime_small_dicts = db
            .runtime_small_dicts
            .or_else(|| Self::extract_runtime_small_dicts(db.snapshot));

        Self {
            active_snapshot: db.snapshot,
            vars,
            to_t: db.t,
            from_t: None,
            overlay: Some(db.overlay),
            batch_size: 1000,
            policy_enforcer: None,
            bm25_provider: None,
            bm25_search_provider: None,
            vector_provider: None,
            r2rml_provider: None,
            r2rml_table_provider: None,
            dataset: None,
            active_graph: ActiveGraph::Default,
            tracker: Tracker::disabled(),
            history_mode: false,
            strict_bind_errors: false,
            binary_store,
            binary_g_id: db.g_id,
            dict_novelty,
            namespace_codes_fallback,
            runtime_small_dicts,
            spatial_providers: None,
            fulltext_providers: None,
            english_lang_id: None,
            remote_service: None,
            r2rml_graph_ids: std::collections::HashSet::new(),
            multi_ledger: false,
            eager_materialization: db.eager,
            original_snapshot: db.snapshot,
        }
    }

    /// Create from a `GraphDbRef` with an explicit `from_t` for history queries.
    ///
    /// Like [`from_graph_db_ref`](Self::from_graph_db_ref), auto-populates
    /// `binary_store` from the snapshot's range provider.
    pub fn from_graph_db_ref_with_from_t(
        db: GraphDbRef<'a>,
        vars: &'a VarRegistry,
        from_t: Option<i64>,
    ) -> Self {
        let binary_store = Self::extract_binary_store(db.snapshot);
        let dict_novelty = Self::extract_dict_novelty(db.snapshot);
        let namespace_codes_fallback = binary_store
            .as_ref()
            .map(|_| Arc::new(db.snapshot.namespaces().clone()));
        let runtime_small_dicts = db
            .runtime_small_dicts
            .or_else(|| Self::extract_runtime_small_dicts(db.snapshot));

        Self {
            active_snapshot: db.snapshot,
            vars,
            to_t: db.t,
            from_t,
            overlay: Some(db.overlay),
            batch_size: 1000,
            policy_enforcer: None,
            bm25_provider: None,
            bm25_search_provider: None,
            vector_provider: None,
            r2rml_provider: None,
            r2rml_table_provider: None,
            dataset: None,
            active_graph: ActiveGraph::Default,
            tracker: Tracker::disabled(),
            history_mode: false,
            strict_bind_errors: false,
            binary_store,
            binary_g_id: db.g_id,
            dict_novelty,
            namespace_codes_fallback,
            runtime_small_dicts,
            spatial_providers: None,
            fulltext_providers: None,
            english_lang_id: None,
            remote_service: None,
            r2rml_graph_ids: std::collections::HashSet::new(),
            multi_ledger: false,
            eager_materialization: db.eager,
            original_snapshot: db.snapshot,
        }
    }

    /// Create context with specific time-travel settings
    pub fn with_time(
        snapshot: &'a LedgerSnapshot,
        vars: &'a VarRegistry,
        to_t: i64,
        from_t: Option<i64>,
    ) -> Self {
        Self {
            active_snapshot: snapshot,
            vars,
            to_t,
            from_t,
            overlay: None,
            batch_size: 1000,
            policy_enforcer: None,
            bm25_provider: None,
            bm25_search_provider: None,
            vector_provider: None,
            r2rml_provider: None,
            r2rml_table_provider: None,
            dataset: None,
            active_graph: ActiveGraph::Default,
            tracker: Tracker::disabled(),
            history_mode: false,
            strict_bind_errors: false,
            binary_store: None,
            binary_g_id: 0,
            dict_novelty: None,
            namespace_codes_fallback: None,
            runtime_small_dicts: None,
            spatial_providers: None,
            fulltext_providers: None,
            english_lang_id: None,
            remote_service: None,
            r2rml_graph_ids: std::collections::HashSet::new(),
            multi_ledger: false,
            eager_materialization: false,
            original_snapshot: snapshot,
        }
    }

    /// Enable history mode (include assertions and retractions, capture op metadata)
    pub fn with_history_mode(mut self) -> Self {
        self.history_mode = true;
        self
    }

    /// Create a new execution context with an overlay provider (novelty)
    pub fn with_overlay(
        snapshot: &'a LedgerSnapshot,
        vars: &'a VarRegistry,
        overlay: &'a dyn OverlayProvider,
    ) -> Self {
        Self {
            active_snapshot: snapshot,
            vars,
            to_t: snapshot.t,
            from_t: None,
            overlay: Some(overlay),
            batch_size: 1000,
            policy_enforcer: None,
            bm25_provider: None,
            bm25_search_provider: None,
            vector_provider: None,
            r2rml_provider: None,
            r2rml_table_provider: None,
            dataset: None,
            active_graph: ActiveGraph::Default,
            tracker: Tracker::disabled(),
            history_mode: false,
            strict_bind_errors: false,
            binary_store: None,
            binary_g_id: 0,
            dict_novelty: None,
            namespace_codes_fallback: None,
            runtime_small_dicts: None,
            spatial_providers: None,
            fulltext_providers: None,
            english_lang_id: None,
            remote_service: None,
            r2rml_graph_ids: std::collections::HashSet::new(),
            multi_ledger: false,
            eager_materialization: false,
            original_snapshot: snapshot,
        }
    }

    /// Create context with time-travel settings and an overlay provider
    pub fn with_time_and_overlay(
        snapshot: &'a LedgerSnapshot,
        vars: &'a VarRegistry,
        to_t: i64,
        from_t: Option<i64>,
        overlay: &'a dyn OverlayProvider,
    ) -> Self {
        Self {
            active_snapshot: snapshot,
            vars,
            to_t,
            from_t,
            overlay: Some(overlay),
            batch_size: 1000,
            policy_enforcer: None,
            bm25_provider: None,
            bm25_search_provider: None,
            vector_provider: None,
            r2rml_provider: None,
            r2rml_table_provider: None,
            dataset: None,
            active_graph: ActiveGraph::Default,
            tracker: Tracker::disabled(),
            history_mode: false,
            strict_bind_errors: false,
            binary_store: None,
            binary_g_id: 0,
            dict_novelty: None,
            namespace_codes_fallback: None,
            runtime_small_dicts: None,
            spatial_providers: None,
            fulltext_providers: None,
            english_lang_id: None,
            remote_service: None,
            r2rml_graph_ids: std::collections::HashSet::new(),
            multi_ledger: false,
            eager_materialization: false,
            original_snapshot: snapshot,
        }
    }

    /// Attach a BM25 index provider to this context (for IndexSearch patterns).
    ///
    /// This is the legacy provider that returns the raw index. For the search service
    /// protocol, prefer using [`with_bm25_search_provider`] instead.
    pub fn with_bm25_provider(mut self, provider: &'a dyn Bm25IndexProvider) -> Self {
        self.bm25_provider = Some(provider);
        self
    }

    /// Attach a BM25 search provider to this context (for IndexSearch patterns).
    ///
    /// This is the preferred provider for the search service protocol. It returns
    /// search results directly, supporting both embedded and remote backends.
    pub fn with_bm25_search_provider(mut self, provider: &'a dyn Bm25SearchProvider) -> Self {
        self.bm25_search_provider = Some(provider);
        self
    }

    /// Attach a vector index provider to this context (for VectorSearch patterns).
    pub fn with_vector_provider(mut self, provider: &'a dyn VectorIndexProvider) -> Self {
        self.vector_provider = Some(provider);
        self
    }

    /// Attach R2RML providers to this context (for R2rml patterns).
    ///
    /// Both providers are required for R2RML scans:
    /// - `mapping_provider`: Loads compiled R2RML mappings for graph sources
    /// - `table_provider`: Executes Iceberg table scans
    pub fn with_r2rml_providers(
        mut self,
        mapping_provider: &'a dyn R2rmlProvider,
        table_provider: &'a dyn R2rmlTableProvider,
    ) -> Self {
        self.r2rml_provider = Some(mapping_provider);
        self.r2rml_table_provider = Some(table_provider);
        self
    }

    /// Attach spatial index providers to this context (for S2Search patterns).
    pub fn with_spatial_providers(
        mut self,
        providers: &'a HashMap<String, Arc<dyn SpatialIndexProvider>>,
    ) -> Self {
        self.spatial_providers = Some(providers);
        self
    }

    /// Attach fulltext BoW arenas to this context (for `fulltext()` BM25 scoring).
    pub fn with_fulltext_providers(mut self, providers: &'a FulltextProviders) -> Self {
        self.fulltext_providers = Some(providers);
        self
    }

    /// Attach a remote SERVICE executor to this context.
    pub fn with_remote_service(mut self, executor: &'a dyn RemoteServiceExecutor) -> Self {
        self.remote_service = Some(executor);
        self
    }

    /// Add policy enforcer to this execution context
    ///
    /// This enables per-leaf batch filtering with full f:query policy support.
    /// The enforcer wraps a PolicyContext and provides async evaluation.
    /// Access the raw PolicyContext via `enforcer.policy()` if needed.
    pub fn with_policy_enforcer(mut self, enforcer: Arc<QueryPolicyEnforcer>) -> Self {
        self.policy_enforcer = Some(enforcer);
        self
    }

    /// Attach an execution tracker to this context.
    pub fn with_tracker(mut self, tracker: Tracker) -> Self {
        self.tracker = tracker;
        self
    }

    /// Enable strict bind error handling.
    pub fn with_strict_bind_errors(mut self) -> Self {
        self.strict_bind_errors = true;
        self
    }

    /// Check if this context has an active (non-root) policy
    pub fn has_policy(&self) -> bool {
        self.policy_enforcer
            .as_ref()
            .map(|e| !e.is_root())
            .unwrap_or(false)
    }

    /// Get the effective overlay (NoOverlay if none set)
    pub fn overlay(&self) -> &'a dyn OverlayProvider {
        self.overlay.unwrap_or(&NoOverlay)
    }

    /// Set the batch size
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Encode an IRI to a SID using the database's namespace codes
    pub fn encode_iri(&self, iri: &str) -> Option<Sid> {
        self.active_snapshot.encode_iri(iri)
    }

    /// Encode an IRI to a SID, returning `None` if no registered namespace
    /// prefix matches (does NOT fall back to the EMPTY namespace).
    ///
    /// Use this for runtime IRI resolution where unknown namespaces should
    /// remain as IRI strings rather than silently mapping to namespace 0.
    pub fn encode_iri_strict(&self, iri: &str) -> Option<Sid> {
        self.active_snapshot.encode_iri_strict(iri)
    }

    /// Decode a SID to an IRI using the database's namespace codes
    pub fn decode_sid(&self, sid: &Sid) -> Option<String> {
        self.active_snapshot.decode_sid(sid)
    }

    /// Check if we're in multi-ledger (dataset) mode.
    ///
    /// Returns `true` only when the currently active query scope spans more than
    /// one distinct ledger ID. This is a cached value computed at construction
    /// time, so it is free to call per-row.
    #[inline]
    pub fn is_multi_ledger(&self) -> bool {
        self.multi_ledger
    }

    /// Compute the multi-ledger flag from dataset + active_graph state.
    ///
    /// A dataset wrapper alone is not enough: single-ledger `FROM` queries
    /// still need binary-store-backed late materialization and should behave like
    /// normal single-ledger execution.
    fn compute_multi_ledger(dataset: Option<&DataSet<'_>>, active_graph: &ActiveGraph) -> bool {
        let Some(ds) = dataset else {
            return false;
        };
        let graphs: Vec<_> = match active_graph {
            ActiveGraph::Default => ds.default_graphs().iter().collect(),
            ActiveGraph::Named(iri) => ds.named_graph(iri).into_iter().collect(),
        };
        let Some(first) = graphs.first() else {
            return false;
        };
        graphs
            .iter()
            .skip(1)
            .any(|graph| graph.ledger_id.as_ref() != first.ledger_id.as_ref())
    }

    /// Decode a SID to an IRI using a specific ledger's namespace table
    ///
    /// Used in multi-ledger mode to decode SIDs from the correct ledger.
    /// Falls back to the primary db if the ledger is not found.
    pub fn decode_sid_in_ledger(&self, sid: &Sid, ledger_id: &str) -> Option<String> {
        if let Some(ds) = &self.dataset {
            // Search all graphs (default and named) by ledger_id
            if let Some(graph) = ds.find_by_ledger_id(ledger_id) {
                return graph.snapshot.decode_sid(sid);
            }
        }
        // Fallback to primary db
        self.active_snapshot.decode_sid(sid)
    }

    /// Encode an IRI to a SID using a specific ledger's namespace table
    ///
    /// Used in multi-ledger mode when re-encoding an IRI for a target ledger.
    /// This is needed when an IriMatch from one ledger needs to be used in
    /// a scan against a different ledger.
    pub fn encode_iri_in_ledger(&self, iri: &str, ledger_id: &str) -> Option<Sid> {
        if let Some(ds) = &self.dataset {
            // Search all graphs (default and named) by ledger_id
            if let Some(graph) = ds.find_by_ledger_id(ledger_id) {
                return graph.snapshot.encode_iri(iri);
            }
        }
        // Fallback to primary db
        self.active_snapshot.encode_iri(iri)
    }

    /// Get the ledger ID for the currently active graph (if in dataset mode)
    ///
    /// Returns the ledger ID when a single named graph is active,
    /// or None for single-db mode or when multiple default graphs are active.
    pub fn active_ledger_id(&self) -> Option<&str> {
        match (&self.dataset, &self.active_graph) {
            (Some(ds), ActiveGraph::Named(iri)) => {
                ds.named_graph(iri).map(|g| g.ledger_id.as_ref())
            }
            _ => None,
        }
    }

    /// Attach a dataset to this execution context for multi-graph queries
    pub fn with_dataset(mut self, dataset: &'a DataSet<'a>) -> Self {
        self.multi_ledger = Self::compute_multi_ledger(Some(dataset), &self.active_graph);
        self.dataset = Some(dataset);
        self
    }

    /// Get active graphs for scanning
    ///
    /// Returns `Single` when no dataset is present (callers should use `ctx.active_snapshot`),
    /// or `Many` with the active graph(s) from the dataset.
    ///
    /// Returns `Single` when no dataset is present, or `Many` with the relevant graph references to iterate over.
    pub fn active_graphs(&self) -> ActiveGraphs<'a, '_> {
        match (&self.dataset, &self.active_graph) {
            (None, _) => ActiveGraphs::Single,
            (Some(ds), ActiveGraph::Default) => {
                ActiveGraphs::Many(ds.default_graphs().iter().collect())
            }
            (Some(ds), ActiveGraph::Named(iri)) => {
                ActiveGraphs::Many(ds.named_graph(iri).into_iter().collect())
            }
        }
    }

    /// Require that the query targets exactly one graph.
    ///
    /// Returns `(db, overlay, to_t)` for the single active graph — either from
    /// single-db mode or a dataset with exactly one active graph. Returns
    /// `QueryError::InvalidQuery` if multiple graphs are active.
    pub fn require_single_graph(
        &self,
    ) -> Result<(&'a LedgerSnapshot, &'a dyn OverlayProvider, i64), QueryError> {
        match self.active_graphs() {
            ActiveGraphs::Single => Ok((self.active_snapshot, self.overlay(), self.to_t)),
            ActiveGraphs::Many(graphs) if graphs.len() == 1 => {
                let g = graphs[0];
                Ok((g.snapshot, g.overlay, g.to_t))
            }
            ActiveGraphs::Many(_) => Err(QueryError::InvalidQuery(
                "Property paths over multi-graph datasets are not supported; \
                 use GRAPH to select a single graph"
                    .to_string(),
            )),
        }
    }

    /// Check whether the binary index fast path is available.
    ///
    /// Returns `true` when a binary store is present and the query is in
    /// single-ledger mode. Individual call sites may layer additional
    /// conditions (e.g. `to_t >= base_t`, `!history_mode`).
    pub fn has_binary_store(&self) -> bool {
        if self.is_multi_ledger() || self.binary_store.is_none() {
            return false;
        }
        // Binary scan fast paths only support a single active graph.
        match (&self.dataset, &self.active_graph) {
            (None, _) => true,
            (Some(ds), ActiveGraph::Default) => ds.default_graphs().len() == 1,
            (Some(ds), ActiveGraph::Named(iri)) => ds.named_graph(iri).is_some(),
        }
    }

    /// Return a `BinaryGraphView` for the current graph, combining the binary store
    /// with `binary_g_id`.
    ///
    /// Returns `None` if no binary store is attached. Callers should grab this
    /// once at operator construction and store it — not call it in tight loops
    /// (each call clones an Arc).
    pub fn graph_view(&self) -> Option<BinaryGraphView> {
        if !self.has_binary_store() {
            return None;
        }
        let store = self.binary_store.as_ref()?;
        let view = store
            .graph_with_novelty(self.binary_g_id, self.dict_novelty.clone())
            .with_namespace_codes_fallback(self.namespace_codes_fallback.clone())
            .with_tracker(self.tracker.clone());
        Some(view)
    }

    /// Decode an `EncodedLit` binding value using DictNovelty-aware routing.
    ///
    /// Thin wrapper around [`BinaryGraphView::decode_value_from_kind`] which
    /// handles watermark routing automatically when `dict_novelty` is present.
    ///
    /// Returns `None` if no binary store is attached.
    pub fn decode_encoded_value(
        &self,
        o_kind: u8,
        o_key: u64,
        p_id: u32,
        dt_id: u16,
        lang_id: u16,
    ) -> Option<std::io::Result<fluree_db_core::FlakeValue>> {
        let gv = self.graph_view()?;
        Some(gv.decode_value_from_kind(o_kind, o_key, p_id, dt_id, lang_id))
    }

    /// Resolve a subject ID to an IRI, using DictNovelty-aware routing.
    ///
    /// Thin wrapper around [`BinaryGraphView::resolve_subject_iri`] which
    /// handles watermark routing automatically when `dict_novelty` is present.
    ///
    /// Returns `None` if no binary store is attached.
    pub fn resolve_subject_iri(&self, s_id: u64) -> Option<std::io::Result<String>> {
        let gv = self.graph_view()?;
        Some(gv.resolve_subject_iri(s_id))
    }

    /// Get the default graphs slice without allocation (for scan hot path).
    ///
    /// Returns `Some(&[GraphRef])` if in dataset mode with default graph active,
    /// `None` otherwise (single-db mode or named graph active).
    ///
    /// Use this instead of `active_graphs()` in tight loops to avoid Vec allocation.
    pub fn default_graphs_slice(&self) -> Option<&[crate::dataset::GraphRef<'a>]> {
        match (&self.dataset, &self.active_graph) {
            (Some(ds), ActiveGraph::Default) => Some(ds.default_graphs()),
            _ => None,
        }
    }

    /// Create a new context with a specific named graph active
    ///
    /// This is cheap: just creates a new context with a different `active_graph` enum.
    /// Used by `GraphOperator` to switch graph context during GRAPH pattern execution.
    pub fn with_active_graph(&self, iri: Arc<str>) -> Self {
        // In dataset mode, ensure binary scans route to the active graph's g_id.
        // Without this, `BinaryScanOperator` would continue scanning the original graph
        // even inside a `GRAPH <iri> { ... }` pattern.
        let binary_g_id = self
            .dataset
            .and_then(|ds| ds.named_graph(&iri).map(|g| g.g_id))
            .unwrap_or(self.binary_g_id);
        let active_graph = ActiveGraph::Named(iri);
        let multi_ledger = Self::compute_multi_ledger(self.dataset, &active_graph);
        Self {
            active_snapshot: self.active_snapshot,
            vars: self.vars,
            to_t: self.to_t,
            from_t: self.from_t,
            overlay: self.overlay,
            batch_size: self.batch_size,
            policy_enforcer: self.policy_enforcer.clone(),
            bm25_provider: self.bm25_provider,
            bm25_search_provider: self.bm25_search_provider,
            vector_provider: self.vector_provider,
            r2rml_provider: self.r2rml_provider,
            r2rml_table_provider: self.r2rml_table_provider,
            dataset: self.dataset,
            active_graph,
            tracker: self.tracker.clone(),
            history_mode: self.history_mode,
            strict_bind_errors: self.strict_bind_errors,
            binary_store: self.binary_store.clone(),
            binary_g_id,
            dict_novelty: self.dict_novelty.clone(),
            namespace_codes_fallback: self.namespace_codes_fallback.clone(),
            runtime_small_dicts: self.runtime_small_dicts,
            spatial_providers: self.spatial_providers,
            fulltext_providers: self.fulltext_providers,
            english_lang_id: self.english_lang_id,
            remote_service: self.remote_service,
            r2rml_graph_ids: self.r2rml_graph_ids.clone(),
            multi_ledger,
            eager_materialization: self.eager_materialization,
            original_snapshot: self.original_snapshot,
        }
    }

    /// Create a new context with the default graph(s) active
    ///
    /// Returns to querying the default graph(s) after a GRAPH pattern.
    pub fn with_default_graph(&self) -> Self {
        // In dataset mode, ensure binary scans route back to the default graph's g_id.
        // When multiple default graphs are active, binary scans can't represent a union;
        // in those cases, operators should fall back to range scans (dataset-aware).
        let binary_g_id = self
            .dataset
            .and_then(|ds| {
                if ds.default_graphs().len() == 1 {
                    ds.default_graphs().first().map(|g| g.g_id)
                } else {
                    None
                }
            })
            .unwrap_or(self.binary_g_id);
        Self {
            active_snapshot: self.active_snapshot,
            vars: self.vars,
            to_t: self.to_t,
            from_t: self.from_t,
            overlay: self.overlay,
            batch_size: self.batch_size,
            policy_enforcer: self.policy_enforcer.clone(),
            bm25_provider: self.bm25_provider,
            bm25_search_provider: self.bm25_search_provider,
            vector_provider: self.vector_provider,
            r2rml_provider: self.r2rml_provider,
            r2rml_table_provider: self.r2rml_table_provider,
            dataset: self.dataset,
            active_graph: ActiveGraph::Default,
            tracker: self.tracker.clone(),
            history_mode: self.history_mode,
            strict_bind_errors: self.strict_bind_errors,
            binary_store: self.binary_store.clone(),
            binary_g_id,
            dict_novelty: self.dict_novelty.clone(),
            namespace_codes_fallback: self.namespace_codes_fallback.clone(),
            runtime_small_dicts: self.runtime_small_dicts,
            spatial_providers: self.spatial_providers,
            fulltext_providers: self.fulltext_providers,
            english_lang_id: self.english_lang_id,
            remote_service: self.remote_service,
            r2rml_graph_ids: self.r2rml_graph_ids.clone(),
            multi_ledger: Self::compute_multi_ledger(self.dataset, &ActiveGraph::Default),
            eager_materialization: self.eager_materialization,
            original_snapshot: self.original_snapshot,
        }
    }

    /// Create a new context targeting a specific graph reference
    ///
    /// Used by SERVICE operator to execute patterns against a specific ledger.
    /// The new context uses the graph's db, overlay, and to_t settings.
    pub fn with_graph_ref(&self, graph: &crate::dataset::GraphRef<'a>) -> Self {
        // A per-graph context represents a single specific graph — NOT a
        // dataset. Clear the dataset and multi_ledger flag so inner operators
        // (BinaryScanOperator, etc.) take the single-graph path. Provenance
        // stamping is the DatasetOperator's responsibility, not the inner
        // scan's.
        Self {
            active_snapshot: graph.snapshot,
            vars: self.vars,
            to_t: graph.to_t,
            from_t: self.from_t,
            overlay: Some(graph.overlay),
            batch_size: self.batch_size,
            policy_enforcer: graph
                .policy_enforcer
                .clone()
                .or_else(|| self.policy_enforcer.clone()),
            bm25_provider: self.bm25_provider,
            bm25_search_provider: self.bm25_search_provider,
            vector_provider: self.vector_provider,
            r2rml_provider: self.r2rml_provider,
            r2rml_table_provider: self.r2rml_table_provider,
            dataset: None,
            active_graph: ActiveGraph::Default,
            tracker: self.tracker.clone(),
            history_mode: self.history_mode,
            strict_bind_errors: self.strict_bind_errors,
            binary_store: Self::extract_binary_store(graph.snapshot),
            binary_g_id: graph.g_id,
            dict_novelty: Self::extract_dict_novelty(graph.snapshot),
            namespace_codes_fallback: None,
            runtime_small_dicts: Self::extract_runtime_small_dicts(graph.snapshot),
            spatial_providers: self.spatial_providers,
            fulltext_providers: self.fulltext_providers,
            english_lang_id: self.english_lang_id,
            remote_service: self.remote_service,
            r2rml_graph_ids: self.r2rml_graph_ids.clone(),
            multi_ledger: false,
            eager_materialization: self.eager_materialization,
            original_snapshot: self.original_snapshot,
        }
    }

    /// Extract `BinaryIndexStore` from a snapshot's `RangeProvider` via downcast.
    ///
    /// Cost: one `TypeId` comparison. Returns `None` when no range provider is
    /// attached (e.g. genesis / metadata-only snapshot).
    fn extract_binary_store(snapshot: &LedgerSnapshot) -> Option<Arc<BinaryIndexStore>> {
        snapshot
            .range_provider
            .as_ref()
            .and_then(|rp| rp.as_any().downcast_ref::<BinaryRangeProvider>())
            .map(|brp| Arc::clone(brp.store()))
    }

    /// Extract `DictNovelty` from a snapshot's `RangeProvider` via downcast.
    ///
    /// This is critical for decoding novelty-only subject/string IDs when executing via
    /// `BinaryScanOperator`. Without it, decoding falls back to persisted forward packs and
    /// fails for novelty IDs (e.g. "string id N not found in forward packs").
    fn extract_dict_novelty(snapshot: &LedgerSnapshot) -> Option<Arc<DictNovelty>> {
        snapshot
            .range_provider
            .as_ref()
            .and_then(|rp| rp.as_any().downcast_ref::<BinaryRangeProvider>())
            .map(|brp| Arc::clone(brp.dict_novelty()))
    }

    /// Extract `RuntimeSmallDicts` from a snapshot's `RangeProvider` via downcast.
    pub(crate) fn extract_runtime_small_dicts(
        snapshot: &'a LedgerSnapshot,
    ) -> Option<&'a RuntimeSmallDicts> {
        snapshot
            .range_provider
            .as_ref()
            .and_then(|rp| rp.as_any().downcast_ref::<BinaryRangeProvider>())
            .map(|brp| brp.runtime_small_dicts().as_ref())
    }

    /// Attach a binary columnar index store for fast local-file scans.
    ///
    /// When set, `BinaryScanOperator` uses the binary cursor path instead of
    /// the `range_with_overlay` fallback for reading from binary columnar indexes.
    pub fn with_binary_store(mut self, store: Arc<BinaryIndexStore>, g_id: GraphId) -> Self {
        self.binary_store = Some(store);
        self.binary_g_id = g_id;
        self
    }

    /// Attach a dictionary novelty layer for binary scan subject/string lookups.
    pub fn with_dict_novelty(mut self, dict_novelty: Arc<DictNovelty>) -> Self {
        self.dict_novelty = Some(dict_novelty);
        self
    }

    /// Attach runtime predicate/datatype IDs carried by the db value.
    pub fn with_runtime_small_dicts(mut self, runtime_small_dicts: &'a RuntimeSmallDicts) -> Self {
        self.runtime_small_dicts = Some(runtime_small_dicts);
        self
    }

    /// Force eager materialization of binary-scan bindings.
    pub fn with_eager_materialization(mut self) -> Self {
        self.eager_materialization = true;
        self
    }

    /// Set the graph ID for range queries.
    ///
    /// This sets `binary_g_id` independently of `binary_store`, which is needed
    /// for the `range_with_overlay()` fallback path in `BinaryScanOperator` where
    /// the graph ID must reach the range provider even without a local binary store.
    pub fn with_graph_id(mut self, g_id: GraphId) -> Self {
        self.binary_g_id = g_id;
        self
    }
}

/// Well-known datatype SIDs
///
/// These are common XSD datatypes used in Fluree.
///
/// Also provides fast datatype family equivalence checking for the scan loop.
/// Integer family: xsd:integer, xsd:long, xsd:int, xsd:short, xsd:byte
/// Float family: xsd:double, xsd:float
#[derive(Debug, Clone)]
pub struct WellKnownDatatypes {
    /// xsd:string (namespace code 2)
    pub xsd_string: Sid,
    /// xsd:long (namespace code 2)
    pub xsd_long: Sid,
    /// xsd:integer (namespace code 2) - arbitrary precision integer
    pub xsd_integer: Sid,
    /// xsd:int (namespace code 2) - 32-bit integer
    pub xsd_int: Sid,
    /// xsd:short (namespace code 2) - 16-bit integer
    pub xsd_short: Sid,
    /// xsd:byte (namespace code 2) - 8-bit integer
    pub xsd_byte: Sid,
    /// xsd:double (namespace code 2)
    pub xsd_double: Sid,
    /// xsd:float (namespace code 2) - 32-bit float
    pub xsd_float: Sid,
    /// xsd:decimal (namespace code 2) - arbitrary precision decimal
    pub xsd_decimal: Sid,
    /// xsd:boolean (namespace code 2)
    pub xsd_boolean: Sid,
    /// xsd:dateTime (namespace code 2)
    pub xsd_datetime: Sid,
    /// xsd:date (namespace code 2)
    pub xsd_date: Sid,
    /// xsd:time (namespace code 2)
    pub xsd_time: Sid,
    /// xsd:gYear
    pub xsd_g_year: Sid,
    /// xsd:gYearMonth
    pub xsd_g_year_month: Sid,
    /// xsd:gMonth
    pub xsd_g_month: Sid,
    /// xsd:gDay
    pub xsd_g_day: Sid,
    /// xsd:gMonthDay
    pub xsd_g_month_day: Sid,
    /// xsd:duration
    pub xsd_duration: Sid,
    /// xsd:dayTimeDuration
    pub xsd_day_time_duration: Sid,
    /// xsd:yearMonthDuration
    pub xsd_year_month_duration: Sid,
    /// $id (reference type) - returned by DATATYPE() for IRIs
    pub id_type: Sid,
    /// fluree:embeddingVector (https://ns.flur.ee/db#embeddingVector)
    pub fluree_vector: Sid,
    /// rdf:JSON (@json datatype)
    pub rdf_json: Sid,
    /// geo:wktLiteral (http://www.opengis.net/ont/geosparql#wktLiteral)
    pub geo_wkt_literal: Sid,
}

impl Default for WellKnownDatatypes {
    fn default() -> Self {
        Self::new()
    }
}

impl WellKnownDatatypes {
    /// Create with standard Fluree namespace codes
    pub fn new() -> Self {
        Self {
            xsd_string: Sid::new(XSD, xsd_names::STRING),
            xsd_long: Sid::new(XSD, xsd_names::LONG),
            xsd_integer: Sid::new(XSD, xsd_names::INTEGER),
            xsd_int: Sid::new(XSD, xsd_names::INT),
            xsd_short: Sid::new(XSD, xsd_names::SHORT),
            xsd_byte: Sid::new(XSD, xsd_names::BYTE),
            xsd_double: Sid::new(XSD, xsd_names::DOUBLE),
            xsd_float: Sid::new(XSD, xsd_names::FLOAT),
            xsd_decimal: Sid::new(XSD, xsd_names::DECIMAL),
            xsd_boolean: Sid::new(XSD, xsd_names::BOOLEAN),
            xsd_datetime: Sid::new(XSD, xsd_names::DATE_TIME),
            xsd_date: Sid::new(XSD, xsd_names::DATE),
            xsd_time: Sid::new(XSD, xsd_names::TIME),
            xsd_g_year: Sid::new(XSD, xsd_names::G_YEAR),
            xsd_g_year_month: Sid::new(XSD, xsd_names::G_YEAR_MONTH),
            xsd_g_month: Sid::new(XSD, xsd_names::G_MONTH),
            xsd_g_day: Sid::new(XSD, xsd_names::G_DAY),
            xsd_g_month_day: Sid::new(XSD, xsd_names::G_MONTH_DAY),
            xsd_duration: Sid::new(XSD, xsd_names::DURATION),
            xsd_day_time_duration: Sid::new(XSD, xsd_names::DAY_TIME_DURATION),
            xsd_year_month_duration: Sid::new(XSD, xsd_names::YEAR_MONTH_DURATION),
            id_type: Sid::new(JSON_LD, "id"),
            fluree_vector: Sid::new(FLUREE_DB, "vector"),
            rdf_json: Sid::new(RDF, "JSON"),
            geo_wkt_literal: Sid::new(OGC_GEO, geo_names::WKT_LITERAL),
        }
    }

    /// Check if a SID is in the integer family (xsd:integer, long, int, short, byte)
    #[inline]
    pub fn is_integer_family(&self, sid: &Sid) -> bool {
        *sid == self.xsd_integer
            || *sid == self.xsd_long
            || *sid == self.xsd_int
            || *sid == self.xsd_short
            || *sid == self.xsd_byte
    }

    /// Check if a SID is in the float family (xsd:double, float)
    #[inline]
    pub fn is_float_family(&self, sid: &Sid) -> bool {
        *sid == self.xsd_double || *sid == self.xsd_float
    }

    /// Check if two SIDs are equivalent for datatype matching
    ///
    /// Two datatypes are equivalent if:
    /// 1. They are exactly equal, OR
    /// 2. They are both in the integer family (xsd:integer ≈ xsd:int ≈ xsd:long ≈ xsd:short ≈ xsd:byte)
    /// 3. They are both in the float family (xsd:double ≈ xsd:float)
    ///
    /// This is a **fast SID-based check** (no string comparisons) for use in the scan hot path.
    #[inline]
    pub fn datatypes_equivalent(&self, a: &Sid, b: &Sid) -> bool {
        // Fast path: exact match
        if a == b {
            return true;
        }
        // Integer family equivalence
        if self.is_integer_family(a) && self.is_integer_family(b) {
            return true;
        }
        // Float family equivalence
        if self.is_float_family(a) && self.is_float_family(b) {
            return true;
        }
        false
    }
}
