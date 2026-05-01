//! Unified query execution runner
//!
//! This module provides the core execution pipeline that all `execute_with_*`
//! functions share. By extracting the common logic here, we eliminate duplication
//! and ensure consistent behavior (including tracing) across all execution paths.

use crate::binding::Batch;
use crate::context::{ExecutionContext, FulltextProviders};
use crate::dataset::DataSet;
use crate::error::Result;
use crate::ir::Pattern;
use crate::operator::BoxedOperator;
use crate::options::QueryOptions;
use crate::parse::ParsedQuery;
use crate::reasoning::ReasoningOverlay;
use crate::rewrite_owl_ql::Ontology;
use crate::schema_bundle::SchemaBundleOverlay;
use crate::stats_cache::cached_stats_view_for_db;
use crate::triple::{Ref, Term, TriplePattern};
use crate::var_registry::VarRegistry;
use fluree_db_binary_index::BinaryIndexStore;
use fluree_db_core::dict_novelty::DictNovelty;
use fluree_db_core::{GraphDbRef, GraphId, LedgerSnapshot, Tracker};
use fluree_db_reasoner::DerivedFactsOverlay;
use fluree_db_spatial::SpatialIndexProvider;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tracing::Instrument;

use super::operator_tree::build_operator_tree;
use super::reasoning_prep::{
    compute_derived_facts, effective_reasoning_modes, schema_hierarchy_with_overlay,
};
use super::rewrite_glue::rewrite_query_patterns;

/// Remove exact duplicate triple patterns inside conjunctive blocks.
///
/// Some benchmark query generators (notably BSBM) occasionally emit redundant
/// triples like:
/// `?s bsbm:productFeature ex:Feature214 .`
/// repeated twice in the same UNION branch. This does not change semantics but
/// can amplify intermediate results and force extra join/distinct work.
///
/// We keep first occurrence order and only dedup `Pattern::Triple` nodes.
fn dedup_exact_triples(patterns: Vec<Pattern>) -> Vec<Pattern> {
    fn dedup_list(list: Vec<Pattern>) -> Vec<Pattern> {
        let mut out: Vec<Pattern> = Vec::with_capacity(list.len());
        let mut seen_triples: Vec<TriplePattern> = Vec::new();

        for p in list {
            match p {
                Pattern::Triple(tp) => {
                    if seen_triples.iter().any(|t| t == &tp) {
                        continue;
                    }
                    seen_triples.push(tp.clone());
                    out.push(Pattern::Triple(tp));
                }
                Pattern::Optional(inner) => out.push(Pattern::Optional(dedup_list(inner))),
                Pattern::Union(branches) => out.push(Pattern::Union(
                    branches.into_iter().map(dedup_list).collect(),
                )),
                Pattern::Minus(inner) => out.push(Pattern::Minus(dedup_list(inner))),
                Pattern::Exists(inner) => out.push(Pattern::Exists(dedup_list(inner))),
                Pattern::NotExists(inner) => out.push(Pattern::NotExists(dedup_list(inner))),
                Pattern::Graph { name, patterns } => out.push(Pattern::Graph {
                    name,
                    patterns: dedup_list(patterns),
                }),
                other => out.push(other),
            }
        }
        out
    }

    dedup_list(patterns)
}

/// Query with execution options
///
/// Combines a parsed query with solution modifiers for execution.
/// The `options` field allows overriding the options embedded in `ParsedQuery`.
#[derive(Debug)]
pub struct ExecutableQuery {
    /// The parsed query (contains embedded options)
    pub query: ParsedQuery,
    /// Execution options (may override query.options)
    pub options: QueryOptions,
}

impl ExecutableQuery {
    /// Create a new executable query with explicit options override
    pub fn new(query: ParsedQuery, options: QueryOptions) -> Self {
        Self { query, options }
    }

    /// Create an executable query using the query's embedded options
    pub fn simple(query: ParsedQuery) -> Self {
        let options = query.options.clone();
        Self { query, options }
    }

    /// True if any pattern in this query calls `fulltext(...)`.
    ///
    /// The query-context setup code checks this before allocating the
    /// per-graph fulltext arena map and resolving the English `lang_id`,
    /// skipping that work for queries that don't use full-text scoring.
    pub fn uses_fulltext(&self) -> bool {
        self.query
            .patterns
            .iter()
            .any(|p| crate::ir::pattern_contains_function(p, &crate::ir::Function::Fulltext))
    }
}

/// Prepared execution environment
///
/// Contains all the pre-computed state needed to execute a query:
/// - Derived facts overlay (if any)
/// - Rewritten patterns
/// - Operator tree
///
/// This struct captures the result of the "preparation" phase, which is
/// common to all execution paths. The actual execution just needs to
/// run the operator tree with an appropriate ExecutionContext.
///
/// # Future Enhancements
///
/// Additional fields may be added to support:
/// - Schema hierarchy for context building
/// - Reasoning modes for diagnostics/debugging
pub struct PreparedExecution {
    /// The operator tree to execute
    pub operator: BoxedOperator,
    /// Derived facts overlay (kept alive during execution)
    pub derived_overlay: Option<Arc<DerivedFactsOverlay>>,
}

/// Inputs that the preparation phase needs to know up front.
///
/// Lives here rather than as loose parameters so that future planner inputs
/// (additional [`PlanningContext`] fields, alternate stats sources, etc.)
/// don't keep extending the prepare-call signature.
#[derive(Clone, Copy, Default)]
pub struct PrepareConfig<'a> {
    /// Optional shared binary index store used by the planner stats cache
    /// and by mode-aware scan construction.
    pub binary_store: Option<&'a Arc<BinaryIndexStore>>,
    /// Planning-time decisions captured before prepare runs.
    pub planning: crate::temporal_mode::PlanningContext,
}

impl<'a> PrepareConfig<'a> {
    /// Construct a config for current-state queries with the given binary store.
    ///
    /// Canonical root for the planner-mode invariant: any production caller
    /// that knows it wants current-state semantics goes through here. Calling
    /// `PlanningContext::current()` directly outside of true roots is the
    /// drift hazard the planner-mode refactor was designed to eliminate.
    pub fn current(binary_store: Option<&'a Arc<BinaryIndexStore>>) -> Self {
        Self {
            binary_store,
            planning: crate::temporal_mode::PlanningContext::current(),
        }
    }

    /// Construct a config for history-range queries with the given binary store.
    ///
    /// Canonical root for history-range planning. Detection happens at the
    /// dataset/view layer (`view::dataset_query::query_dataset` consults
    /// `dataset.history_time_range()` before prepare runs) — never inside
    /// the planner or operator construction.
    pub fn history(binary_store: Option<&'a Arc<BinaryIndexStore>>) -> Self {
        Self {
            binary_store,
            planning: crate::temporal_mode::PlanningContext::history(),
        }
    }
}

/// Prepare query execution with an overlay
///
/// This performs all the common preparation steps:
/// 1. Compute schema hierarchy from overlay
/// 2. Determine effective reasoning modes
/// 3. Compute derived facts (OWL2-RL / datalog)
/// 4. Build ontology for OWL2-QL (if enabled)
/// 5. Rewrite patterns for reasoning
/// 6. Build operator tree
///
/// The result can then be executed with any ExecutionContext.
pub async fn prepare_execution(
    db: GraphDbRef<'_>,
    query: &ExecutableQuery,
) -> Result<PreparedExecution> {
    prepare_execution_with_config(db, query, &PrepareConfig::default()).await
}

/// Back-compat wrapper. Prefer [`prepare_execution_with_config`] for new code.
pub async fn prepare_execution_with_binary_store(
    db: GraphDbRef<'_>,
    query: &ExecutableQuery,
    binary_store: Option<&Arc<BinaryIndexStore>>,
) -> Result<PreparedExecution> {
    prepare_execution_with_config(db, query, &PrepareConfig::current(binary_store)).await
}

/// Prepare execution given an explicit [`PrepareConfig`].
///
/// This is the canonical entry point: callers compute the planning context
/// (in particular [`crate::temporal_mode::TemporalMode`]) before invoking
/// prepare, so the operator tree can be built mode-aware in subsequent
/// phases of the planner-mode refactor.
pub async fn prepare_execution_with_config(
    db: GraphDbRef<'_>,
    query: &ExecutableQuery,
    config: &PrepareConfig<'_>,
) -> Result<PreparedExecution> {
    let binary_store = config.binary_store;
    let planning = config.planning;
    let span = tracing::debug_span!(
        "query_prepare",
        db_t = db.snapshot.t,
        to_t = db.t,
        pattern_count = query.query.patterns.len()
    );
    // Use an async block with .instrument() so the span is NOT held
    // across .await via a thread-local guard (which would cause cross-request
    // trace contamination in tokio's multi-threaded runtime).
    async move {
        tracing::debug!("preparing query execution");

        // ---- reasoning_prep: schema hierarchy, reasoning modes, derived facts, ontology ----
        let reasoning_span = tracing::debug_span!("reasoning_prep");
        // If the upstream API layer pre-resolved an `f:schemaSource` + `owl:imports`
        // closure into `query.options.schema_bundle`, project it as an overlay now.
        // This makes schema-whitelisted flakes from every source graph visible at
        // `g_id=0`, which is what RDFS/OWL extraction code scans.
        let schema_overlay_binding: Option<SchemaBundleOverlay<'_>> = query
            .options
            .schema_bundle
            .as_ref()
            .filter(|b| !b.is_empty())
            .map(|bundle| SchemaBundleOverlay::new(db.overlay, bundle.clone()));
        let effective_overlay: &dyn fluree_db_core::OverlayProvider = schema_overlay_binding
            .as_ref()
            .map(|o| o as &dyn fluree_db_core::OverlayProvider)
            .unwrap_or(db.overlay);
        let (hierarchy, reasoning, derived_overlay, ontology) = async {
            // Step 1: Compute schema hierarchy from overlay
            let hierarchy = schema_hierarchy_with_overlay(db.snapshot, effective_overlay, db.t);

            // Step 2: Determine effective reasoning modes
            let reasoning =
                effective_reasoning_modes(&query.options.reasoning, hierarchy.is_some());

            if reasoning.rdfs || reasoning.owl2ql || reasoning.owl2rl || reasoning.datalog {
                tracing::debug!(
                    rdfs = reasoning.rdfs,
                    owl2ql = reasoning.owl2ql,
                    owl2rl = reasoning.owl2rl,
                    datalog = reasoning.datalog,
                    "reasoning enabled"
                );
            }

            // Step 3: Compute derived facts from OWL2-RL and/or datalog rules
            //
            // Note: `compute_derived_facts` reads the query graph (`db.g_id`)
            // for instance data but uses `effective_overlay` so that OWL2-RL
            // axioms (e.g. `?p a owl:TransitiveProperty`) from the import
            // closure are visible when scanning g_id=0, and base-overlay
            // novelty remains visible for other graphs.
            let derived_overlay =
                compute_derived_facts(db.snapshot, db.g_id, effective_overlay, db.t, &reasoning)
                    .await;

            // Step 4: Build ontology for OWL2-QL mode (if enabled)
            let reasoning_overlay_for_ontology: Option<ReasoningOverlay<'_>> = derived_overlay
                .as_ref()
                .map(|derived| ReasoningOverlay::new(effective_overlay, derived.clone()));

            let effective_overlay_for_ontology: &dyn fluree_db_core::OverlayProvider =
                reasoning_overlay_for_ontology
                    .as_ref()
                    .map(|o| o as &dyn fluree_db_core::OverlayProvider)
                    .unwrap_or(effective_overlay);

            let ontology = if reasoning.owl2ql {
                tracing::debug!("building OWL2-QL ontology");
                let ontology_db = fluree_db_core::GraphDbRef::new(
                    db.snapshot,
                    db.g_id,
                    effective_overlay_for_ontology,
                    db.t,
                );
                Some(Ontology::from_db_with_overlay(ontology_db).await?)
            } else {
                None
            };

            Ok::<_, crate::error::QueryError>((hierarchy, reasoning, derived_overlay, ontology))
        }
        .instrument(reasoning_span)
        .await?;

        // ---- pattern_rewrite: encode IRIs and apply reasoning rewrites ----
        //
        // OWL2-QL rewriting (and current RDFS expansion) require SIDs for ontology/hierarchy lookup.
        // Lowering may produce `Term::Iri` to support cross-ledger joins; for single-ledger execution
        // we can safely encode IRIs to SIDs here.
        fn encode_ref(snapshot: &LedgerSnapshot, r: &Ref) -> Ref {
            match r {
                Ref::Iri(iri) => match snapshot.encode_iri(iri) {
                    Some(sid) => Ref::Sid(sid),
                    None => r.clone(),
                },
                other => other.clone(),
            }
        }

        fn encode_term(snapshot: &LedgerSnapshot, t: &Term) -> Term {
            match t {
                Term::Iri(iri) => snapshot
                    .encode_iri(iri)
                    .map(Term::Sid)
                    .unwrap_or_else(|| t.clone()),
                _ => t.clone(),
            }
        }

        fn encode_patterns_for_reasoning(
            snapshot: &LedgerSnapshot,
            patterns: &[Pattern],
        ) -> Vec<Pattern> {
            patterns
                .iter()
                .map(|p| match p {
                    Pattern::Triple(tp) => Pattern::Triple(TriplePattern {
                        s: encode_ref(snapshot, &tp.s),
                        p: encode_ref(snapshot, &tp.p),
                        o: encode_term(snapshot, &tp.o),
                        dtc: tp.dtc.clone(),
                    }),
                    Pattern::Optional(inner) => {
                        Pattern::Optional(encode_patterns_for_reasoning(snapshot, inner))
                    }
                    Pattern::Union(branches) => Pattern::Union(
                        branches
                            .iter()
                            .map(|b| encode_patterns_for_reasoning(snapshot, b))
                            .collect(),
                    ),
                    Pattern::Minus(inner) => {
                        Pattern::Minus(encode_patterns_for_reasoning(snapshot, inner))
                    }
                    Pattern::Exists(inner) => {
                        Pattern::Exists(encode_patterns_for_reasoning(snapshot, inner))
                    }
                    Pattern::NotExists(inner) => {
                        Pattern::NotExists(encode_patterns_for_reasoning(snapshot, inner))
                    }
                    Pattern::Graph { name, patterns } => Pattern::Graph {
                        name: name.clone(),
                        patterns: encode_patterns_for_reasoning(snapshot, patterns),
                    },
                    _ => p.clone(),
                })
                .collect()
        }

        let rewritten_query = {
            let _rewrite_span = tracing::debug_span!(
                "pattern_rewrite",
                patterns_before = query.query.patterns.len(),
                patterns_after = tracing::field::Empty,
            )
            .entered();

            let patterns_for_rewrite = if reasoning.rdfs || reasoning.owl2ql {
                encode_patterns_for_reasoning(db.snapshot, &query.query.patterns)
            } else {
                query.query.patterns.clone()
            };
            let (rewritten_patterns, _diag) = rewrite_query_patterns(
                &patterns_for_rewrite,
                hierarchy.clone(),
                &reasoning,
                ontology.as_ref(),
            );

            // Step 5b: Rewrite geof:distance patterns → Pattern::GeoSearch
            //
            // Detects Triple(?s, pred, ?loc) + Bind(?dist = geof:distance(?loc, WKT)) + Filter(?dist < r)
            // and collapses them into a single Pattern::GeoSearch for index acceleration.
            // This runs for both SPARQL and JSON-LD queries — same patterns, same rewrite.
            let rewritten_patterns =
                crate::geo_rewrite::rewrite_geo_patterns(rewritten_patterns, &|iri: &str| {
                    db.snapshot.encode_iri(iri)
                });

            let before_dedup = rewritten_patterns.len();
            let rewritten_patterns = dedup_exact_triples(rewritten_patterns);
            if rewritten_patterns.len() != before_dedup {
                tracing::debug!(
                    before = before_dedup,
                    after = rewritten_patterns.len(),
                    "deduped exact duplicate triples"
                );
            }

            tracing::Span::current().record("patterns_after", rewritten_patterns.len());

            if rewritten_patterns.len() != query.query.patterns.len() {
                tracing::debug!(
                    original_count = query.query.patterns.len(),
                    rewritten_count = rewritten_patterns.len(),
                    "patterns rewritten for reasoning"
                );
            }

            query.query.with_patterns(rewritten_patterns)
        };

        // ---- plan: build operator tree from rewritten query ----
        // Planning context is computed at the dataset/view layer before
        // `prepare_execution_with_config` runs. The planner branches on
        // `planning.mode` at scan-construction time (phase 3).
        let operator = {
            let _plan_span = tracing::debug_span!(
                "plan",
                pattern_count = rewritten_query.patterns.len(),
                mode = ?planning.mode,
            )
            .entered();

            let stats_view = cached_stats_view_for_db(db, binary_store);
            build_operator_tree(&rewritten_query, &query.options, stats_view, &planning)?
        };

        Ok(PreparedExecution {
            operator,
            derived_overlay,
        })
    }
    .instrument(span)
    .await
}

/// Run an operator tree to completion and collect all result batches
///
/// This is the common execution loop used by all execution paths.
/// Includes consistent tracing for debugging and monitoring.
pub async fn run_operator(
    mut operator: BoxedOperator,
    ctx: &ExecutionContext<'_>,
) -> Result<Vec<Batch>> {
    let op_type = std::any::type_name_of_val(operator.as_ref());
    // Temporal mode is captured at planner-time inside the operator tree, not on
    // ExecutionContext, so it is no longer surfaced as a span field here. The
    // `plan` span (in `prepare_execution_with_config`) records `mode` once at
    // planning time.
    let span = tracing::debug_span!(
        "query_run",
        operator = op_type,
        to_t = ctx.to_t,
        from_t = tracing::field::Empty,
        has_overlay = ctx.overlay.is_some(),
        batch_size = ctx.batch_size,
        open_ms = tracing::field::Empty,
        total_ms = tracing::field::Empty,
        total_batches = tracing::field::Empty,
        total_rows = tracing::field::Empty,
        max_batch_ms = tracing::field::Empty
    );
    // Use an async block with .instrument() so the span is NOT held
    // across .await via a thread-local guard (which would cause cross-request
    // trace contamination in tokio's multi-threaded runtime).
    async move {
        let span = tracing::Span::current();

        span.record("from_t", ctx.from_t);

        let open_start = Instant::now();
        operator
            .open(ctx)
            .instrument(tracing::debug_span!("operator_open"))
            .await?;
        span.record(
            "open_ms",
            (open_start.elapsed().as_secs_f64() * 1000.0) as u64,
        );

        let mut results = Vec::new();
        let mut batch_count = 0;
        let mut total_rows: usize = 0;
        let mut max_batch_ms: u64 = 0;
        let run_start = Instant::now();
        while {
            let batch_start = Instant::now();
            let next = operator.next_batch(ctx).await?;
            let batch_ms = (batch_start.elapsed().as_secs_f64() * 1000.0) as u64;
            if batch_ms > max_batch_ms {
                max_batch_ms = batch_ms;
            }
            if let Some(batch) = next {
                batch_count += 1;
                total_rows += batch.len();
                tracing::debug!(
                    batch_num = batch_count,
                    row_count = batch.len(),
                    batch_ms,
                    "received batch"
                );
                results.push(batch);
                true
            } else {
                false
            }
        } {}

        operator.close();

        // If the operator is blocking, results often arrive in a small number of batches.
        // We record overall totals here; operator-level spans provide the breakdown.
        let total_ms = (run_start.elapsed().as_secs_f64() * 1000.0) as u64;
        span.record("total_ms", total_ms);
        span.record("total_batches", batch_count as u64);
        span.record("total_rows", total_rows as u64);
        span.record("max_batch_ms", max_batch_ms);
        tracing::debug!(
            total_batches = batch_count,
            total_rows,
            "query execution completed"
        );

        Ok(results)
    }
    .instrument(span)
    .await
}

/// Execution context configuration
///
/// Specifies the optional components to add to the execution context.
/// This is the unified knob for all query execution paths.
pub struct ContextConfig<'a, 'b> {
    pub tracker: Option<&'a Tracker>,
    /// Policy enforcer for async policy evaluation with full f:query support.
    ///
    /// When set, scan operators will use per-leaf batch filtering via `filter_flakes`.
    /// Access the raw PolicyContext via `enforcer.policy()` if needed.
    pub policy_enforcer: Option<Arc<crate::policy::QueryPolicyEnforcer>>,
    pub dataset: Option<&'a DataSet<'a>>,
    pub r2rml: Option<(
        &'b dyn crate::r2rml::R2rmlProvider,
        &'b dyn crate::r2rml::R2rmlTableProvider,
    )>,
    /// BM25 index provider for `Pattern::IndexSearch` (graph source BM25 queries).
    ///
    /// When set, BM25 search operators can load indexes from graph sources.
    pub bm25_provider: Option<&'b dyn crate::bm25::Bm25IndexProvider>,
    /// Vector index provider for `Pattern::VectorSearch` (graph source vector queries).
    ///
    /// When set, vector search operators can load indexes from graph sources.
    pub vector_provider: Option<&'b dyn crate::vector::VectorIndexProvider>,
    /// Optional lower time bound for history/range queries.
    /// Defaults to None (no lower bound).
    pub from_t: Option<i64>,
    /// When true, bind evaluation errors become query errors.
    pub strict_bind_errors: bool,
    /// Binary columnar index store for `BinaryScanOperator`.
    ///
    /// This is the explicit path — separate from `LedgerSnapshot.range_provider` which
    /// serves the transparent `range_with_overlay()` callers.
    pub binary_store: Option<Arc<BinaryIndexStore>>,
    /// Graph ID for binary index lookups (default 0 = default graph).
    pub binary_g_id: GraphId,
    /// Dictionary novelty layer for binary scan subject/string lookups.
    pub dict_novelty: Option<Arc<DictNovelty>>,
    /// Spatial index providers for S2Search patterns.
    /// Keys are graph-scoped: `"g{g_id}:{predicate_iri}"`.
    pub spatial_providers: Option<&'a HashMap<String, Arc<dyn SpatialIndexProvider>>>,
    /// Fulltext BoW arenas for `fulltext()` BM25 scoring.
    /// Keys are `(g_id, p_id, lang_id)` triples — one arena per language on
    /// each property.
    pub fulltext_providers: Option<&'a FulltextProviders>,
    /// Dict-assigned lang_id for BCP-47 `"en"`, used as the arena-lookup
    /// key for `@fulltext`-datatype values and as the final fallback in
    /// the language-resolution chain for configured full-text properties.
    pub english_lang_id: Option<u16>,
    /// Remote SERVICE executor for `fluree:remote:` endpoints.
    pub remote_service: Option<&'b dyn crate::remote_service::RemoteServiceExecutor>,
}

impl<'a, 'b> Default for ContextConfig<'a, 'b> {
    fn default() -> Self {
        // strict-by-default: query semantics treat bind eval errors as query
        // errors. Every wrapper in this file set `strict_bind_errors: true`,
        // and the view layer sets it true; the only lax path was the legacy
        // `execute_where_with_overlay_at` family in `lib.rs`, which never
        // went through `ContextConfig`.
        Self {
            tracker: None,
            policy_enforcer: None,
            dataset: None,
            r2rml: None,
            bm25_provider: None,
            vector_provider: None,
            from_t: None,
            strict_bind_errors: true,
            binary_store: None,
            binary_g_id: 0,
            dict_novelty: None,
            spatial_providers: None,
            fulltext_providers: None,
            english_lang_id: None,
            remote_service: None,
        }
    }
}

impl<'a, 'b> ContextConfig<'a, 'b> {
    /// Attach a policy enforcer built from a `&PolicyContext`.
    ///
    /// Wraps the `Arc::new(QueryPolicyEnforcer::new(Arc::new(policy.clone())))`
    /// boilerplate so callers handing in a raw `&PolicyContext` don't have to
    /// reconstruct it. Callers that already hold a cached
    /// `Arc<QueryPolicyEnforcer>` (e.g., the view layer via
    /// `db.policy_enforcer().cloned()`) should set `policy_enforcer` directly.
    pub fn with_policy(mut self, policy: &fluree_db_policy::PolicyContext) -> Self {
        self.policy_enforcer = Some(Arc::new(crate::policy::QueryPolicyEnforcer::new(
            Arc::new(policy.clone()),
        )));
        self
    }
}

/// Execute a prepared query with configurable context options
///
/// This is the unified internal execution path that handles all variants.
/// The `config` parameter specifies which optional components to add to the context.
pub async fn execute_prepared<'a, 'b>(
    db: GraphDbRef<'a>,
    vars: &VarRegistry,
    prepared: PreparedExecution,
    config: ContextConfig<'a, 'b>,
) -> Result<Vec<Batch>> {
    // Create composite overlay if we have derived facts
    let reasoning_overlay: Option<ReasoningOverlay<'a>> = prepared
        .derived_overlay
        .as_ref()
        .map(|derived| ReasoningOverlay::new(db.overlay, derived.clone()));

    // Use composite overlay if available, otherwise base overlay
    let effective_overlay: &dyn fluree_db_core::OverlayProvider = reasoning_overlay
        .as_ref()
        .map(|o| o as &dyn fluree_db_core::OverlayProvider)
        .unwrap_or(db.overlay);

    // Build context with all configured options
    let mut ctx = ExecutionContext::with_time_and_overlay(
        db.snapshot,
        vars,
        db.t,
        config.from_t,
        effective_overlay,
    );
    // Always propagate the graph id, even when no binary store is attached.
    //
    // Overlay-only historical queries (genesis snapshot + commit replay) must still
    // route all range queries through the correct graph partition in the overlay.
    ctx = ctx.with_graph_id(db.g_id);
    if let Some(runtime_small_dicts) = db
        .runtime_small_dicts
        .or_else(|| ExecutionContext::extract_runtime_small_dicts(db.snapshot))
    {
        ctx = ctx.with_runtime_small_dicts(runtime_small_dicts);
    }
    if db.eager {
        ctx = ctx.with_eager_materialization();
    }

    if let Some(tracker) = config.tracker {
        ctx = ctx.with_tracker(tracker.clone());
    }
    if let Some(enforcer) = config.policy_enforcer {
        ctx = ctx.with_policy_enforcer(enforcer);
    }
    if let Some(dataset) = config.dataset {
        ctx = ctx.with_dataset(dataset);
    }
    if let Some((r2rml_provider, r2rml_table_provider)) = config.r2rml {
        ctx = ctx.with_r2rml_providers(r2rml_provider, r2rml_table_provider);
    }
    if let Some(p) = config.bm25_provider {
        ctx = ctx.with_bm25_provider(p);
    }
    if let Some(p) = config.vector_provider {
        ctx = ctx.with_vector_provider(p);
    }
    if config.strict_bind_errors {
        ctx = ctx.with_strict_bind_errors();
    }
    if let Some(store) = config.binary_store {
        ctx = ctx.with_binary_store(store, config.binary_g_id);
    }
    if let Some(dn) = config.dict_novelty {
        ctx = ctx.with_dict_novelty(dn);
    }
    if let Some(providers) = config.spatial_providers {
        ctx = ctx.with_spatial_providers(providers);
    }
    if let Some(providers) = config.fulltext_providers {
        ctx = ctx.with_fulltext_providers(providers);
    }
    ctx.english_lang_id = config.english_lang_id;
    if let Some(executor) = config.remote_service {
        ctx = ctx.with_remote_service(executor);
    }

    // Precompute which graphs in the dataset are R2RML-backed.
    if let (Some(r2rml_provider), Some(dataset)) = (ctx.r2rml_provider, ctx.dataset) {
        let mut r2rml_ids = std::collections::HashSet::new();
        for graph_ref in dataset.default_graphs() {
            let is_r2rml = r2rml_provider.has_r2rml_mapping(&graph_ref.ledger_id).await;
            if is_r2rml {
                r2rml_ids.insert(Arc::clone(&graph_ref.ledger_id));
            }
        }
        for (iri, graph_ref) in dataset.named_graphs_iter() {
            let is_r2rml = r2rml_provider.has_r2rml_mapping(&graph_ref.ledger_id).await;
            if is_r2rml {
                // Insert the graph IRI (not ledger_id) since graph.rs
                // checks r2rml_graph_ids against the GRAPH pattern IRI.
                r2rml_ids.insert(Arc::clone(iri));
            }
        }
        ctx.r2rml_graph_ids = r2rml_ids;
    }
    // Also check the primary snapshot's ledger_id (for single-source graph source queries)
    if let Some(provider) = ctx.r2rml_provider {
        if ctx.dataset.is_none() {
            let is_r2rml = provider.has_r2rml_mapping(&db.snapshot.ledger_id).await;
            if is_r2rml {
                ctx.r2rml_graph_ids
                    .insert(Arc::from(db.snapshot.ledger_id.as_str()));
            }
        }
    }

    run_operator(prepared.operator, &ctx).await
}

/// Prepare and execute a query in a single call.
///
/// This is the canonical one-call entry point for callers that don't need to
/// separate preparation from execution. Callers that want to inspect or share
/// a `PreparedExecution` (for example, the view layer's eager + tracked
/// variants) should call [`prepare_execution`] / [`prepare_execution_with_config`]
/// and then [`execute_prepared`] explicitly.
pub async fn execute<'a, 'b>(
    db: GraphDbRef<'a>,
    vars: &VarRegistry,
    query: &ExecutableQuery,
    config: ContextConfig<'a, 'b>,
) -> Result<Vec<Batch>> {
    let prepared = prepare_execution(db, query).await?;
    execute_prepared(db, vars, prepared, config).await
}

