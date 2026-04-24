//! Resolve the schema-bundle closure driven by `f:schemaSource` and `owl:imports`.
//!
//! # What this does
//!
//! Given a resolved [`ReasoningDefaults`], walk from the configured
//! `f:schemaSource` through the transitive closure of `owl:imports` triples
//! and produce a deduped list of local graph IDs that together constitute
//! the schema for reasoning. The result is a [`ResolvedSchemaBundle`] that
//! the query runner feeds into a `SchemaBundleOverlay` so the existing
//! RDFS/OWL-RL/OWL-QL extraction code can operate unchanged.
//!
//! # Resolution order for each `owl:imports` IRI
//!
//! 1. **Named graph IRI match** — the import IRI is registered as a graph in
//!    the current ledger's [`GraphRegistry`]. Resolve to that `GraphId`.
//! 2. **Explicit mapping** — the import IRI appears in
//!    `f:ontologyImportMap`; resolve via the bound [`GraphSourceRef`].
//! 3. **Error** — strict: unresolved imports fail the query. There is no
//!    silent skip.
//!
//! Imports resolve to graphs within the same ledger. A `GraphSourceRef`
//! that names a different ledger is rejected with a clear error; same
//! treatment for `f:atT`, `f:trustPolicy`, and `f:rollbackGuard`, which are
//! parsed by the config layer but not honored by bundle resolution.
//!
//! # `at_t` / temporal semantics
//!
//! Every named graph in a Fluree ledger advances together at the ledger's
//! monotonic `t`, so the entire closure is resolved at the query's
//! effective `to_t` — one number, one consistent view, no per-import
//! bookkeeping.
//!
//! # Caching
//!
//! The bundle is cached keyed by `(ledger_id, to_t, starting_g_id,
//! follow_imports)` via a process-global [`SchemaBundleCache`]. Because
//! ledger config lives in the config graph (g_id=2) of the same ledger,
//! any config change advances `t` and naturally invalidates older cache
//! entries; no explicit invalidation is needed.

use std::collections::{HashSet, VecDeque};
use std::sync::Arc;

use fluree_db_core::graph_registry::{CONFIG_GRAPH_ID, DEFAULT_GRAPH_ID, TXN_META_GRAPH_ID};
use fluree_db_core::ledger_config::{GraphSourceRef, ReasoningDefaults};
use fluree_db_core::{GraphDbRef, GraphId, LedgerSnapshot, OverlayProvider};
use fluree_db_query::{execute_pattern_with_overlay_at, Ref, Term, TriplePattern, VarRegistry};
use fluree_vocab::config_iris;

use crate::error::{ApiError, Result};

/// The resolved schema-graph closure for a query.
///
/// Produced by [`resolve_schema_bundle`] from a [`ReasoningDefaults`] that
/// configures `f:schemaSource` (and optionally follows `owl:imports`).
///
/// All `sources` belong to the same ledger (`ledger_id`) at the same logical
/// point-in-time (`to_t`) — guaranteed by the same-ledger resolution rule.
#[derive(Debug, Clone)]
pub struct ResolvedSchemaBundle {
    /// Ledger the bundle was resolved against.
    pub ledger_id: Arc<str>,
    /// Logical `t` at which the closure was walked.
    pub to_t: i64,
    /// Deduplicated graph IDs in BFS discovery order; the starting graph
    /// (from `f:schemaSource`) is always first.
    pub sources: Vec<GraphId>,
}

/// Resolve the schema bundle for a query.
///
/// Returns `Ok(None)` when no bundle is needed — typically because
/// `reasoning.schema_source` is not configured, in which case the caller
/// should keep the existing default-graph behavior.
///
/// When `schema_source` is configured, always returns `Ok(Some(_))`; the
/// returned bundle always includes the starting graph. Imports are only
/// walked when `follow_owl_imports` is `true` (or, when unset, the caller
/// opts in at its own discretion).
///
/// Errors with [`ApiError::OntologyImport`] on:
/// - an unresolved `owl:imports` IRI,
/// - an `f:schemaSource` whose `graph_selector` cannot be resolved locally,
/// - a `GraphSourceRef` that targets a different ledger, sets `f:atT`, or
///   carries a `f:trustPolicy` / `f:rollbackGuard` — see
///   [`resolve_local_graph_source`] for the full list of enforced invariants.
pub async fn resolve_schema_bundle(
    snapshot: &LedgerSnapshot,
    overlay: &dyn OverlayProvider,
    to_t: i64,
    reasoning: &ReasoningDefaults,
) -> Result<Option<Arc<ResolvedSchemaBundle>>> {
    let Some(schema_source) = reasoning.schema_source.as_ref() else {
        return Ok(None);
    };

    let starting_g_id = resolve_local_graph_source(snapshot, schema_source)?;

    let follow_imports = reasoning.follow_owl_imports.unwrap_or(false);

    // Cache lookup — keyed on (ledger_id, to_t, starting_g_id). Different
    // `follow_owl_imports` / import-map settings change `t` (because config
    // is transactional) so they don't need separate key dimensions.
    let cache_key = SchemaBundleCacheKey {
        ledger_id: Arc::from(snapshot.ledger_id.as_str()),
        to_t,
        starting_g_id,
        follow_imports,
    };
    if let Some(cached) = global_schema_bundle_cache().get(&cache_key) {
        return Ok(Some(cached));
    }

    // BFS walk the import closure.
    let mut sources: Vec<GraphId> = vec![starting_g_id];
    let mut seen: HashSet<GraphId> = HashSet::from([starting_g_id]);

    if follow_imports {
        let mut queue: VecDeque<GraphId> = VecDeque::from([starting_g_id]);
        while let Some(g_id) = queue.pop_front() {
            for import_iri in scan_owl_imports_in_graph(snapshot, overlay, to_t, g_id).await? {
                let resolved = resolve_import_iri(snapshot, reasoning, &import_iri)?;
                if seen.insert(resolved) {
                    sources.push(resolved);
                    queue.push_back(resolved);
                }
                // If `seen` already contained it, BFS naturally handles the cycle.
            }
        }
    }

    let bundle = Arc::new(ResolvedSchemaBundle {
        ledger_id: Arc::from(snapshot.ledger_id.as_str()),
        to_t,
        sources,
    });
    global_schema_bundle_cache().insert(cache_key, bundle.clone());

    Ok(Some(bundle))
}

/// Resolve a `GraphSourceRef` to a local `GraphId` in the current ledger.
///
/// Central chokepoint for the three invariants applied to every
/// `f:schemaSource` or `f:ontologyImportMap` entry:
///
/// - **Same-ledger resolution.** Refs targeting a different ledger are
///   rejected with a clear error.
/// - **No reserved system graphs.** Resolving to `g_id=1` (txn-meta) or
///   `g_id=2` (config) is always an error — those graphs hold framework
///   triples that must not be mixed into the reasoning view.
/// - **No unsupported `GraphSourceRef` fields.** `at_t`, `trust_policy`,
///   and `rollback_guard` are accepted by the config parser but not
///   honored by bundle resolution; setting any of them is rejected so
///   misconfigurations surface immediately rather than running with a
///   silent behavior gap.
fn resolve_local_graph_source(
    snapshot: &LedgerSnapshot,
    source: &GraphSourceRef,
) -> Result<GraphId> {
    if let Some(ledger) = source.ledger.as_deref() {
        if ledger != snapshot.ledger_id {
            return Err(ApiError::OntologyImport(format!(
                "schema/import sources must resolve within the current \
                 ledger (ref targets ledger '{ledger}', current ledger is \
                 '{}'). Move the schema into the current ledger.",
                snapshot.ledger_id
            )));
        }
    }

    // Unsupported `GraphSourceRef` fields: reject rather than silently
    // ignore. Accepting these would make the user think they'd pinned a
    // specific `at_t` or constrained trust, while in reality the bundle
    // is resolved at the query's `to_t` with no trust checks.
    if source.at_t.is_some() {
        return Err(ApiError::OntologyImport(
            "`f:atT` on an `f:schemaSource` / `f:ontologyImportMap` graph \
             ref is not honored — the schema closure resolves at the query's \
             `to_t`. Remove the pin."
                .to_string(),
        ));
    }
    if source.trust_policy.is_some() {
        return Err(ApiError::OntologyImport(
            "`f:trustPolicy` on an `f:schemaSource` / `f:ontologyImportMap` \
             graph ref is not honored — same-ledger resolution requires no \
             separate trust verification. Remove the trust policy."
                .to_string(),
        ));
    }
    if source.rollback_guard.is_some() {
        return Err(ApiError::OntologyImport(
            "`f:rollbackGuard` on an `f:schemaSource` / `f:ontologyImportMap` \
             graph ref is not honored — rollback semantics apply to \
             cross-ledger refs, which aren't used by bundle resolution. \
             Remove the guard."
                .to_string(),
        ));
    }

    let g_id = match source.graph_selector.as_deref() {
        None => DEFAULT_GRAPH_ID,
        Some(sel) if sel == config_iris::DEFAULT_GRAPH => DEFAULT_GRAPH_ID,
        Some(sel) if sel == config_iris::TXN_META_GRAPH => TXN_META_GRAPH_ID,
        Some(sel) => snapshot
            .graph_registry
            .graph_id_for_iri(sel)
            .ok_or_else(|| {
                ApiError::OntologyImport(format!(
                    "f:schemaSource / f:graphRef points to graph '{sel}' \
                     which is not a named graph in ledger '{}'. \
                     Add a mapping via f:ontologyImportMap or change the \
                     selector to match a local graph.",
                    snapshot.ledger_id
                ))
            })?,
    };

    // Reject resolution to a reserved system graph, regardless of how we
    // arrived here (direct `f:graphSelector <f:txnMetaGraph>`, mapped
    // import, or a user graph IRI that happens to be registered at g_id=1/2).
    reject_if_system_graph(g_id)?;
    Ok(g_id)
}

/// Gate any resolved `GraphId` against the reserved-system-graph list.
///
/// Called at the single chokepoint (`resolve_local_graph_source`) so the
/// check can't be bypassed by a new resolution path in the future.
fn reject_if_system_graph(g_id: GraphId) -> Result<()> {
    if g_id == CONFIG_GRAPH_ID || g_id == TXN_META_GRAPH_ID {
        return Err(ApiError::OntologyImport(format!(
            "schema/import resolution landed on a reserved system graph \
             (g_id={g_id}); refusing to use it as a schema source."
        )));
    }
    Ok(())
}

/// Resolve an `owl:imports <X>` IRI to a local `GraphId`.
///
/// Resolution order:
/// 1. `X` is a named graph IRI in the current ledger.
/// 2. `X` has an entry in `f:ontologyImportMap` whose `graph_ref` resolves
///    locally.
/// 3. Otherwise, strict error.
///
/// The reserved-system-graph guard lives in [`resolve_local_graph_source`],
/// so both resolution paths (direct registry lookup and mapping-table
/// fallback) are covered by the single check.
fn resolve_import_iri(
    snapshot: &LedgerSnapshot,
    reasoning: &ReasoningDefaults,
    import_iri: &str,
) -> Result<GraphId> {
    if let Some(g_id) = snapshot.graph_registry.graph_id_for_iri(import_iri) {
        reject_if_system_graph(g_id)?;
        return Ok(g_id);
    }

    if let Some(binding) = reasoning
        .ontology_import_map
        .iter()
        .find(|b| b.ontology_iri == import_iri)
    {
        return resolve_local_graph_source(snapshot, &binding.graph_ref);
    }

    Err(ApiError::OntologyImport(format!(
        "owl:imports <{import_iri}> could not be resolved: not a named \
         graph in ledger '{}' and not present in f:ontologyImportMap. \
         Add a mapping entry or change the import IRI to match a local graph.",
        snapshot.ledger_id
    )))
}

/// Find every object IRI of `owl:imports` within a single graph at `to_t`.
///
/// # Subject is wildcarded (deliberate, broader than OWL header semantics)
///
/// Every `?s owl:imports ?o` triple in the graph is treated as
/// authoritative, **regardless of whether `?s` carries an
/// `rdf:type owl:Ontology` assertion**. Strict OWL 2 would restrict
/// `owl:imports` to the ontology-header triple, but:
///
/// - Many real-world OWL files omit the `owl:Ontology` type assertion and
///   rely on file-level provenance instead; matching only typed ontologies
///   would silently break those inputs.
/// - The resolution layer is already strict: every resolved import lands
///   on a specific local graph, so a stray/garbage `owl:imports` triple
///   fails the query rather than silently expanding the closure.
///
/// The tradeoff: a stray `owl:imports` triple sneaked into a schema graph
/// will turn into a hard `ApiError::OntologyImport`. That's the intended
/// failure mode — users see the broken reference immediately instead of
/// inheriting it as silent schema.
async fn scan_owl_imports_in_graph(
    snapshot: &LedgerSnapshot,
    overlay: &dyn OverlayProvider,
    to_t: i64,
    g_id: GraphId,
) -> Result<Vec<String>> {
    let Some(imports_pred_sid) = snapshot.encode_iri(fluree_vocab::owl::IMPORTS) else {
        // The namespace/name has never been seen in this ledger — no imports possible.
        return Ok(Vec::new());
    };

    let mut vars = VarRegistry::new();
    let subj_var = vars.get_or_insert("?s");
    let obj_var = vars.get_or_insert("?o");

    let pattern = TriplePattern::new(
        Ref::Var(subj_var),
        Ref::Sid(imports_pred_sid),
        Term::Var(obj_var),
    );

    let db = GraphDbRef::new(snapshot, g_id, overlay, to_t).eager();
    let batches = execute_pattern_with_overlay_at(db, &vars, pattern, None)
        .await
        .map_err(|e| {
            ApiError::OntologyImport(format!("failed to scan owl:imports in graph {g_id}: {e}"))
        })?;

    let mut iris = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    for batch in &batches {
        for row in 0..batch.len() {
            let Some(binding) = batch.get(row, obj_var) else {
                continue;
            };
            let Some(sid) = binding.as_sid() else {
                continue;
            };
            let Some(iri) = snapshot.decode_sid(sid) else {
                continue;
            };
            if seen.insert(iri.clone()) {
                iris.push(iri);
            }
        }
    }
    Ok(iris)
}

// ============================================================================
// Cache
// ============================================================================

/// Cache key for [`SchemaBundleCache`].
///
/// `to_t` naturally captures config changes (the config graph lives in the
/// same ledger, so any config edit advances `t`). No separate "config
/// version" dimension is needed.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SchemaBundleCacheKey {
    /// Ledger the bundle was resolved against.
    pub ledger_id: Arc<str>,
    /// Query "as-of" `t`.
    pub to_t: i64,
    /// The `f:schemaSource` starting `GraphId`. Per-graph overrides can
    /// point different queries at different schema sources in the same
    /// ledger, so we key separately.
    pub starting_g_id: GraphId,
    /// Whether the closure was walked via `owl:imports`; a closure with
    /// `follow_imports=false` is just `[starting_g_id]` and must not be
    /// reused at a query that requests the full closure.
    pub follow_imports: bool,
}

/// Process-global LRU cache of resolved schema bundles.
///
/// Backed by `moka::sync::Cache`. Entries have no TTL — eviction is purely
/// by LRU capacity; stale entries age out naturally as new (ledger, t)
/// combinations dominate.
pub struct SchemaBundleCache {
    inner: moka::sync::Cache<SchemaBundleCacheKey, Arc<ResolvedSchemaBundle>>,
}

impl SchemaBundleCache {
    /// Capacity chosen to cover hundreds of active ledgers at a handful of
    /// recent `t` values each, which is well below the reasoning cache
    /// budget and comfortable for a small-payload key/value pair.
    const DEFAULT_CAPACITY: u64 = 1024;

    /// Create a new cache with the given entry capacity.
    pub fn with_capacity(capacity: u64) -> Self {
        Self {
            inner: moka::sync::Cache::new(capacity),
        }
    }

    /// Fetch a bundle, if cached.
    pub fn get(&self, key: &SchemaBundleCacheKey) -> Option<Arc<ResolvedSchemaBundle>> {
        self.inner.get(key)
    }

    /// Insert a bundle.
    pub fn insert(&self, key: SchemaBundleCacheKey, bundle: Arc<ResolvedSchemaBundle>) {
        self.inner.insert(key, bundle);
    }

    /// Clear the cache. Intended for tests only.
    #[cfg(test)]
    pub fn clear(&self) {
        self.inner.invalidate_all();
        self.inner.run_pending_tasks();
    }
}

impl Default for SchemaBundleCache {
    fn default() -> Self {
        Self::with_capacity(Self::DEFAULT_CAPACITY)
    }
}

/// Process-global schema bundle cache.
pub fn global_schema_bundle_cache() -> &'static SchemaBundleCache {
    use std::sync::OnceLock;
    static CACHE: OnceLock<SchemaBundleCache> = OnceLock::new();
    CACHE.get_or_init(SchemaBundleCache::default)
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::ledger_config::{OntologyImportBinding, OverrideControl};

    fn make_reasoning(
        schema_source: Option<GraphSourceRef>,
        follow: Option<bool>,
        map: Vec<OntologyImportBinding>,
    ) -> ReasoningDefaults {
        ReasoningDefaults {
            modes: None,
            schema_source,
            follow_owl_imports: follow,
            ontology_import_map: map,
            override_control: OverrideControl::default(),
        }
    }

    #[tokio::test]
    async fn no_schema_source_returns_none() {
        let snapshot = LedgerSnapshot::genesis("test:a");
        let reasoning = make_reasoning(None, None, Vec::new());
        let bundle = resolve_schema_bundle(&snapshot, &fluree_db_core::NoOverlay, 0, &reasoning)
            .await
            .unwrap();
        assert!(bundle.is_none());
    }

    #[tokio::test]
    async fn cross_ledger_rejected() {
        let snapshot = LedgerSnapshot::genesis("test:a");
        let reasoning = make_reasoning(
            Some(GraphSourceRef {
                ledger: Some("other:main".into()),
                graph_selector: None,
                at_t: None,
                trust_policy: None,
                rollback_guard: None,
            }),
            None,
            Vec::new(),
        );
        let err = resolve_schema_bundle(&snapshot, &fluree_db_core::NoOverlay, 0, &reasoning)
            .await
            .unwrap_err();
        match err {
            ApiError::OntologyImport(msg) => {
                assert!(
                    msg.contains("current ledger") && msg.contains("other:main"),
                    "unexpected message: {msg}"
                );
            }
            e => panic!("expected OntologyImport, got {e:?}"),
        }
    }

    #[tokio::test]
    async fn default_graph_schema_source_resolves_to_zero() {
        let snapshot = LedgerSnapshot::genesis("test:a");
        let reasoning = make_reasoning(
            Some(GraphSourceRef {
                ledger: None,
                graph_selector: None,
                at_t: None,
                trust_policy: None,
                rollback_guard: None,
            }),
            None,
            Vec::new(),
        );
        let bundle = resolve_schema_bundle(&snapshot, &fluree_db_core::NoOverlay, 0, &reasoning)
            .await
            .unwrap()
            .expect("bundle");
        assert_eq!(bundle.sources, vec![DEFAULT_GRAPH_ID]);
    }

    #[tokio::test]
    async fn unknown_graph_selector_errors() {
        let snapshot = LedgerSnapshot::genesis("test:a");
        let reasoning = make_reasoning(
            Some(GraphSourceRef {
                ledger: None,
                graph_selector: Some("urn:does:not:exist".into()),
                at_t: None,
                trust_policy: None,
                rollback_guard: None,
            }),
            None,
            Vec::new(),
        );
        let err = resolve_schema_bundle(&snapshot, &fluree_db_core::NoOverlay, 0, &reasoning)
            .await
            .unwrap_err();
        match err {
            ApiError::OntologyImport(msg) => {
                assert!(msg.contains("not a named graph"), "unexpected: {msg}");
            }
            e => panic!("expected OntologyImport, got {e:?}"),
        }
    }
}
