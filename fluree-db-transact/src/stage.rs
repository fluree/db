//! Transaction staging
//!
//! This module provides the `stage` function that executes a parsed transaction
//! against a ledger and produces a staged view with the resulting flakes.
//!
//! ## SHACL Validation
//!
//! When the `shacl` feature is enabled, you can use [`stage_with_shacl`] to validate
//! staged flakes against SHACL shapes before returning the view. This ensures that
//! data conforms to the defined shape constraints.

use crate::error::{Result, TransactError};
use crate::generate::{infer_datatype, FlakeAccumulator, FlakeGenerator};
use crate::ir::InlineValues;
use crate::ir::{TemplateTerm, TripleTemplate, Txn, TxnType};
use crate::namespace::NamespaceRegistry;
use fluree_db_core::OverlayProvider;
use fluree_db_core::Tracker;
use fluree_db_core::{Flake, FlakeValue, GraphId, Sid};
use fluree_db_ledger::{IndexConfig, LedgerState, LedgerView};
use fluree_db_policy::{
    is_schema_flake, populate_class_cache, PolicyContext, PolicyDecision, PolicyError,
};
use fluree_db_query::parse::{lower_unresolved_patterns, UnresolvedPattern};
use fluree_db_query::{
    Batch, Binding, Pattern, QueryPolicyExecutor, Ref, Term, TriplePattern, VarId, VarRegistry,
};
use fluree_db_sparql::ast::{
    QueryBody as SparqlQueryBody, SelectClause, SelectQuery, SolutionModifiers, SparqlAst,
    WhereClause as SparqlWhereClauseAst,
};
use fluree_db_sparql::lower_sparql;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tracing::Instrument;

#[cfg(feature = "shacl")]
use fluree_db_shacl::{ShaclCache, ShaclEngine, ValidationReport};

/// Build a reverse lookup from graph Sid → GraphId.
///
/// Given `graph_sids` (GraphId → Sid from `txn.graph_delta`), returns the
/// inverse mapping. Used by SHACL/policy to determine which graph a flake
/// belongs to based on its `Flake.g` field.
fn build_reverse_graph_lookup(graph_sids: &HashMap<GraphId, Sid>) -> HashMap<Sid, GraphId> {
    graph_sids
        .iter()
        .map(|(&g_id, sid)| (sid.clone(), g_id))
        .collect()
}

/// Resolve a flake's graph ID from its `Flake.g` field.
///
/// - `None` → default graph (g_id = 0)
/// - `Some(sid)` → looked up in `reverse_graph`; returns error if unknown
fn resolve_flake_graph_id(flake: &Flake, reverse_graph: &HashMap<Sid, GraphId>) -> Result<GraphId> {
    match &flake.g {
        None => Ok(0),
        Some(g_sid) => reverse_graph.get(g_sid).copied().ok_or_else(|| {
            TransactError::FlakeGeneration(format!(
                "staged flake references unknown graph Sid: {g_sid}"
            ))
        }),
    }
}

/// Options for transaction staging
///
/// This struct groups optional configuration parameters for the [`stage`] function,
/// reducing the number of function parameters and making call sites cleaner.
#[derive(Default, Clone)]
pub struct StageOptions<'a> {
    /// Index configuration for backpressure checks.
    /// If provided, staging will fail with `NoveltyAtMax` when novelty is at capacity.
    pub index_config: Option<&'a IndexConfig>,

    /// Policy context for authorization.
    /// If provided (and not root), modify policies will be enforced on staged flakes.
    pub policy_ctx: Option<&'a PolicyContext>,

    /// Tracker for fuel accounting.
    /// If provided, fuel will be consumed for each staged flake.
    pub tracker: Option<&'a Tracker>,

    /// Graph routing map for named-graph flakes.
    ///
    /// Maps `GraphId → Sid` so that `stage_flakes` can resolve each flake's
    /// `Flake.g` to a `GraphId` for per-graph policy enforcement and SHACL validation.
    ///
    /// **Required** when any flake has `g != None`. If `None` is provided and
    /// named-graph flakes are present, `stage_flakes` will return an error.
    ///
    /// The normal `stage()` path builds this internally from `txn.graph_delta`.
    pub graph_sids: Option<&'a HashMap<GraphId, Sid>>,
}

impl<'a> StageOptions<'a> {
    /// Create new stage options with all fields set to None
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the index configuration for backpressure checks
    pub fn with_index_config(mut self, config: &'a IndexConfig) -> Self {
        self.index_config = Some(config);
        self
    }

    /// Set the policy context for authorization
    pub fn with_policy(mut self, policy: &'a PolicyContext) -> Self {
        self.policy_ctx = Some(policy);
        self
    }

    /// Set the tracker for fuel accounting
    pub fn with_tracker(mut self, tracker: &'a Tracker) -> Self {
        self.tracker = Some(tracker);
        self
    }

    /// Set the graph routing map for named-graph flakes
    pub fn with_graph_sids(mut self, graph_sids: &'a HashMap<GraphId, Sid>) -> Self {
        self.graph_sids = Some(graph_sids);
        self
    }
}

/// Stage a transaction against a ledger
///
/// This function:
/// 1. Checks backpressure (rejects if novelty at max)
/// 2. Executes WHERE patterns against the ledger to get bindings
/// 3. Generates retractions from DELETE templates with those bindings
/// 4. Generates assertions from INSERT templates with those bindings
/// 5. Applies cancellation (matching assertion/retraction pairs cancel out)
/// 6. Returns a LedgerView with the staged flakes
///
/// # Arguments
///
/// * `ledger` - The ledger state (consumed by value)
/// * `txn` - The parsed transaction IR
/// * `ns_registry` - Namespace registry for IRI resolution
/// * `options` - Optional configuration for backpressure, policy, and tracking
///
/// # Unbound Variable Behavior
///
/// When a variable in a template is unbound (no matching WHERE result) or poisoned
/// (from an OPTIONAL that didn't match), the flake is **silently skipped**. This
/// follows SPARQL UPDATE semantics where:
///
/// - `DELETE { ?s :name ?name }` with unbound `?name` produces no retractions
/// - `INSERT { ?s :name ?name }` with unbound `?name` produces no assertions
///
/// This is intentional: it allows patterns like "delete all existing values before
/// inserting new ones" to work correctly when there are no existing values.
///
/// If you need to require that all variables are bound, validate the WHERE results
/// before calling stage.
///
/// # Errors
///
/// Returns `TransactError::NoveltyAtMax` if novelty is at the maximum size and
/// reindexing is required before new transactions can be processed.
///
///
/// # Example
///
/// ```ignore
/// let options = StageOptions::new().with_index_config(&config);
/// let view = stage(ledger, txn, ns_registry, options).await?;
/// // Query the view to see staged changes
/// // Or commit the view to persist changes
/// ```
pub async fn stage(
    ledger: LedgerState,
    mut txn: Txn,
    mut ns_registry: NamespaceRegistry,
    options: StageOptions<'_>,
) -> Result<(LedgerView, NamespaceRegistry)> {
    let span = tracing::debug_span!("txn_stage",
        current_t = ledger.t(),
        txn_type = ?txn.txn_type,
        insert_count = txn.insert_templates.len(),
        delete_count = txn.delete_templates.len()
    );
    async move {
        tracing::info!("starting transaction staging");

        // 1. Check backpressure - reject early if novelty is at max
        if let Some(config) = options.index_config {
            if ledger.at_max_novelty(config) {
                tracing::warn!("novelty at max, rejecting transaction");
                return Err(TransactError::NoveltyAtMax);
            }
        }

        // Per-transaction baseline (100 fuel) covering parse, validation,
        // commit log write, and indexing overhead. Per-flake cost (1 micro-fuel)
        // is charged later against the staged flake set.
        if let Some(tracker) = options.tracker {
            tracker.consume_fuel(100_000)?;
        }

        let new_t = ledger.t() + 1;
        tracing::debug!(new_t = new_t, "computed new transaction t");

        // Track whether this transaction has an explicit WHERE clause, before
        // execute_where consumes the patterns. Needed to distinguish "WHERE that
        // matched nothing" (→ no-op) from "no WHERE at all" (→ fire templates once).
        let has_where_clause =
            !txn.where_patterns.is_empty() || txn.values.is_some() || txn.sparql_where.is_some();

        // Pure-DELETE fast path: no INSERT templates and not an Upsert. Skips
        // assertion generation and the assertion/retraction cancellation hashmap;
        // a sort-and-dedup pass over retractions is sufficient since all
        // retractions share `t` and `op=false`.
        let pure_delete = txn.insert_templates.is_empty() && txn.txn_type != TxnType::Upsert;

        // Project WHERE results down to only template-used vars before materialization.
        // For pure delete, only delete-template vars matter; otherwise both groups.
        let template_vars: Vec<VarId> = if pure_delete {
            collect_template_vars(&[txn.delete_templates.as_slice()])
        } else {
            collect_template_vars(&[
                txn.delete_templates.as_slice(),
                txn.insert_templates.as_slice(),
            ])
        };

        // Generate transaction ID for blank node skolemization
        let txn_id = generate_txn_id();

        // Convert graph_delta (g_id -> IRI) to graph_sids (g_id -> Sid) for named graph support
        let graph_sids: HashMap<GraphId, Sid> = txn
            .graph_delta
            .iter()
            .map(|(&g_id, iri)| (g_id, ns_registry.sid_for_iri(iri)))
            .collect();
        // Build reverse graph routing for novelty application.
        //
        // IMPORTANT: `txn.graph_delta` keys are *transaction-local* graph IDs used by templates.
        // Novelty routing, however, must use the ledger's `GraphRegistry` IDs (g_id=3+ for user graphs).
        // Use `GraphRegistry::provisional_ids()` so new graphs referenced in this txn route consistently
        // during staging even before the commit is applied.
        let provisional_graph_ids = ledger
            .snapshot
            .graph_registry
            .provisional_ids(&txn.graph_delta.values().cloned().collect::<Vec<_>>());
        let mut reverse_graph: HashMap<Sid, GraphId> = HashMap::new();
        for iri in txn.graph_delta.values() {
            if let Some(g_id) = provisional_graph_ids.get(iri.as_str()).copied() {
                reverse_graph.insert(ns_registry.sid_for_iri(iri), g_id);
            }
        }

        let mut generator = FlakeGenerator::new(new_t, &mut ns_registry, txn_id)
            .with_graph_sids(graph_sids.clone());

        // Stream the WHERE result into a single accumulator per-batch,
        // projecting / materializing / hydrating in the same step. This keeps
        // peak memory bounded by one batch (plus the accumulator's survivor
        // set) rather than by the total WHERE cardinality.
        let mut acc = if pure_delete {
            FlakeAccumulator::pure_delete(64)
        } else {
            FlakeAccumulator::mixed(64)
        };

        let where_span = tracing::debug_span!(
            "where_exec",
            pattern_count = txn.where_patterns.len(),
            binding_rows = tracing::field::Empty,
            retraction_count = tracing::field::Empty,
            assertion_count = tracing::field::Empty,
        );
        let stream_stats = async {
            let stats = stream_where_into_accumulator(
                &ledger,
                &mut txn,
                &template_vars,
                &mut generator,
                pure_delete,
                &reverse_graph,
                &mut acc,
            )
            .await?;
            let span = tracing::Span::current();
            span.record("binding_rows", stats.total_binding_rows);
            span.record("retraction_count", stats.retraction_count as u64);
            span.record("assertion_count", stats.assertion_count as u64);
            Ok::<_, TransactError>(stats)
        }
        .instrument(where_span)
        .await?;

        // Per SPARQL 1.1 Update spec (§3.1.3): INSERT/DELETE templates are
        // instantiated once per solution row from WHERE. Zero solutions = zero
        // instantiations *except* for all-literal INSERT templates, which
        // must still fire once against a single empty solution (supports the
        // common "delete-if-exists, always insert" pattern).
        //
        // The signal must be "total emitted rows == 0", not "cursor yielded
        // any batch". Operators like VALUES, geo/vector search, and some
        // join/optional shapes legitimately emit `Some(empty_batch)` to
        // represent a zero-row result while still signalling completion —
        // the old eager path detected this via `bindings.is_empty()` on the
        // merged batch. Using `saw_any_batch` would flip that semantic and
        // suppress the post-loop fallback for valid zero-row cases.
        //
        // has_where_clause gates the fallback so the no-WHERE case (where
        // the SingleEmpty cursor emits an empty-schema-empty batch that
        // already fires all-literal templates once via the in-loop path)
        // doesn't double-fire.
        let where_returned_no_rows = has_where_clause && stream_stats.total_binding_rows == 0;
        if where_returned_no_rows && !pure_delete {
            let empty_solution = Batch::single_empty();
            let assertions =
                generator.generate_assertions(&txn.insert_templates, &empty_solution)?;
            acc.push_assertions(assertions);
        }

        // Upsert second wave: retractions derived from direct ledger lookups
        // (not WHERE). These flakes already carry correct `m` from the
        // underlying asserted flakes, so no hydration is needed.
        if txn.txn_type == TxnType::Upsert {
            tracing::debug!("generating upsert deletions");
            let upsert_retractions =
                generate_upsert_deletions(&ledger, &txn, new_t, &graph_sids).await?;
            tracing::debug!(
                upsert_retraction_count = upsert_retractions.len(),
                "upsert deletions generated"
            );
            acc.push_retractions(upsert_retractions);
        }

        let retraction_count = stream_stats.retraction_count;
        let assertion_count = stream_stats.assertion_count;
        let total_inputs = acc.input_count();
        let flakes = if pure_delete {
            let _span =
                tracing::debug_span!("dedup_retractions", retraction_count = retraction_count)
                    .entered();
            let f = acc.finalize();
            if f.len() as u64 != total_inputs {
                tracing::debug!(
                    before = total_inputs,
                    after = f.len(),
                    cancelled = total_inputs - f.len() as u64,
                    "duplicate retractions collapsed"
                );
            }
            f
        } else {
            let _span = tracing::debug_span!(
                "cancellation",
                retraction_count = retraction_count,
                assertion_count = assertion_count,
            )
            .entered();
            let f = acc.finalize();
            if f.len() as u64 != total_inputs {
                tracing::debug!(
                    before = total_inputs,
                    after = f.len(),
                    cancelled = total_inputs - f.len() as u64,
                    "cancellation applied"
                );
            }
            f
        };

        // Count fuel per staged non-schema flake (mirrors query-side fuel counting).
        // NOTE: fuel exhaustion now returns an error (previously silently ignored).
        // This is intentional — transactions exceeding fuel limits should fail
        // before policy enforcement runs.
        if let Some(tracker) = options.tracker {
            for flake in &flakes {
                if !is_schema_flake(&flake.p, &flake.o) {
                    tracker.consume_fuel(1)?;
                }
            }
        }

        // Enforce modify policies (if policy context provided and not root)
        if let Some(policy) = options.policy_ctx {
            if !policy.wrapper().is_root() {
                let policy_span = tracing::debug_span!("policy_enforce");
                async {
                    enforce_modify_policies(
                        &flakes,
                        policy,
                        &ledger,
                        options.tracker,
                        &reverse_graph,
                    )
                    .await
                }
                .instrument(policy_span)
                .await?;
            }
        }

        let total_flakes = flakes.len();
        let assertions = flakes.iter().filter(|f| f.op).count();
        let retractions = total_flakes - assertions;

        tracing::info!(
            flake_count = total_flakes,
            assertions = assertions,
            retractions = retractions,
            "transaction staging completed"
        );

        Ok((
            LedgerView::stage(ledger, flakes, &reverse_graph)?,
            ns_registry,
        ))
    }
    .instrument(span)
    .await
}

/// Stage pre-built flakes against a ledger (bypass WHERE/template pipeline).
///
/// This is the fast path for bulk INSERT from Turtle where flakes are already
/// constructed by [`FlakeSink`](crate::flake_sink::FlakeSink). No WHERE
/// execution, template materialization, or cancellation is performed.
///
/// # Named Graph Support
///
/// When flakes include named-graph data (`Flake.g = Some(_)`), the caller
/// **must** provide `StageOptions.graph_sids` so that policy enforcement
/// and SHACL validation can resolve each flake's graph. If named-graph flakes
/// are present without a routing map, this function returns an error.
///
/// # Arguments
/// * `ledger` - The ledger state (consumed)
/// * `flakes` - Pre-built assertion flakes
/// * `options` - Optional backpressure / policy / tracking / graph routing configuration
pub async fn stage_flakes(
    ledger: LedgerState,
    flakes: Vec<Flake>,
    options: StageOptions<'_>,
) -> Result<LedgerView> {
    let span = tracing::debug_span!("stage_flakes", flake_count = flakes.len());
    async move {
        // 1. Backpressure check
        if let Some(config) = options.index_config {
            if ledger.at_max_novelty(config) {
                tracing::warn!("novelty at max, rejecting transaction");
                return Err(TransactError::NoveltyAtMax);
            }
        }

        // Per-transaction baseline (100 fuel) covering parse, validation,
        // commit log write, and indexing overhead. Per-flake cost (1 micro-fuel)
        // is charged below.
        if let Some(tracker) = options.tracker {
            tracker.consume_fuel(100_000)?;
        }

        // 2. Build graph routing map.
        //
        // If the caller provided graph_sids (push/import path), use it.
        // Otherwise, verify no named-graph flakes are present — stage_flakes
        // cannot correctly enforce policy/SHACL without a routing map.
        let reverse_graph: HashMap<Sid, GraphId> = match options.graph_sids {
            Some(gs) => build_reverse_graph_lookup(gs),
            None => {
                if flakes.iter().any(|f| f.g.is_some()) {
                    return Err(TransactError::FlakeGeneration(
                        "stage_flakes received named-graph flakes but no graph_sids \
                         routing map was provided in StageOptions"
                            .to_string(),
                    ));
                }
                HashMap::new()
            }
        };

        // 3. Count fuel per staged non-schema flake.
        // NOTE: fuel exhaustion now returns an error (previously silently ignored).
        if let Some(tracker) = options.tracker {
            for flake in &flakes {
                if !is_schema_flake(&flake.p, &flake.o) {
                    tracker.consume_fuel(1)?;
                }
            }
        }

        // 4. Policy enforcement
        if let Some(policy) = options.policy_ctx {
            if !policy.wrapper().is_root() {
                tracing::debug!("enforcing modify policies on pre-built flakes");
                enforce_modify_policies(&flakes, policy, &ledger, options.tracker, &reverse_graph)
                    .await?;
            }
        }

        tracing::info!(flake_count = flakes.len(), "stage_flakes completed");
        Ok(LedgerView::stage(ledger, flakes, &reverse_graph)?)
    }
    .instrument(span)
    .await
}

async fn hydrate_list_index_meta_for_retractions(
    ledger: &LedgerState,
    retractions: &mut [Flake],
    reverse_graph: &HashMap<Sid, GraphId>,
) -> Result<()> {
    for flake in retractions.iter_mut() {
        // Only retractions with no metadata are candidates.
        if flake.op {
            continue;
        }
        if flake.m.is_some() {
            continue;
        }

        // Resolve the correct graph for this retraction flake.
        let g_id = resolve_flake_graph_id(flake, reverse_graph)?;

        // Find currently asserted matching flakes (db + novelty overlay) and copy list index meta if present.
        let rm = fluree_db_core::RangeMatch::new()
            .with_subject(flake.s.clone())
            .with_predicate(flake.p.clone())
            .with_object(flake.o.clone())
            .with_datatype(flake.dt.clone());

        let found = fluree_db_core::range_with_overlay(
            &ledger.snapshot,
            g_id,
            ledger.novelty.as_ref(),
            fluree_db_core::IndexType::Spot,
            fluree_db_core::RangeTest::Eq,
            rm,
            fluree_db_core::RangeOptions::new().with_to_t(ledger.t()),
        )
        .await?;

        if let Some(existing) = found
            .into_iter()
            .find(|f| f.op && f.m.as_ref().and_then(|m| m.i).is_some())
        {
            flake.m = existing.m;
        }
    }

    Ok(())
}

/// Enforce modify policies on staged flakes
///
/// This function handles the complete policy enforcement flow:
/// 1. Populates the class cache for f:onClass policy support (if needed)
/// 2. Enforces modify policies on each flake with full f:query support
///
/// Returns `Ok(())` if all flakes pass policy, or an error if any flake is denied
/// or if the class cache population fails.
async fn enforce_modify_policies(
    flakes: &[Flake],
    policy: &PolicyContext,
    ledger: &LedgerState,
    tracker: Option<&Tracker>,
    reverse_graph: &HashMap<Sid, GraphId>,
) -> Result<()> {
    // Pre-populate class cache for f:onClass policy support, per graph.
    if policy.wrapper().has_class_policies() {
        // Group subjects by graph to populate class cache with correct g_id.
        let mut subjects_by_graph: HashMap<GraphId, HashSet<Sid>> = HashMap::new();
        for flake in flakes {
            let g_id = resolve_flake_graph_id(flake, reverse_graph)?;
            subjects_by_graph
                .entry(g_id)
                .or_default()
                .insert(flake.s.clone());
        }

        for (g_id, subjects) in &subjects_by_graph {
            let subject_vec: Vec<Sid> = subjects.iter().cloned().collect();
            populate_class_cache(&subject_vec, ledger.as_graph_db_ref(*g_id), policy)
                .await
                .map_err(|e| {
                    TransactError::Query(fluree_db_query::QueryError::Internal(format!(
                        "Failed to populate class cache: {e}"
                    )))
                })?;
        }
    }

    // Enforce modify policies with full f:query support
    enforce_modify_policy_per_flake(flakes, policy, ledger, tracker, reverse_graph).await
}

/// Enforce modify policies on each flake individually
///
/// Returns `Ok(())` if all flakes pass policy, or `Err(PolicyError)` with
/// the policy's f:exMessage if any flake is denied.
///
/// This function supports f:query policies by executing them against
/// the pre-transaction ledger view (db + novelty at current t).
async fn enforce_modify_policy_per_flake(
    flakes: &[Flake],
    policy: &PolicyContext,
    ledger: &LedgerState,
    tracker: Option<&Tracker>,
    reverse_graph: &HashMap<Sid, GraphId>,
) -> Result<()> {
    // Build per-graph QueryPolicyExecutors so f:query policies execute against
    // the correct graph. Cache executors to avoid rebuilding for every flake.
    let mut executors: HashMap<GraphId, QueryPolicyExecutor<'_>> = HashMap::new();

    // Fuel is now counted upstream in stage() after flake generation,
    // so we only use the tracker here for async policy query calls.
    let async_tracker = tracker.cloned().unwrap_or_else(Tracker::disabled);

    for flake in flakes {
        // Schema flakes always allowed (needed for internal operations)
        if is_schema_flake(&flake.p, &flake.o) {
            continue;
        }

        // Get subject classes from cache (empty if not cached)
        // Class cache is populated per-graph by enforce_modify_policies() above.
        let subject_classes = policy
            .get_cached_subject_classes(&flake.s)
            .unwrap_or_default();

        // Resolve the graph for this flake and get/create a cached executor.
        let g_id = resolve_flake_graph_id(flake, reverse_graph)?;
        let executor = executors.entry(g_id).or_insert_with(|| {
            // Modify policy queries see the state *before* this transaction.
            QueryPolicyExecutor::with_overlay(&ledger.snapshot, ledger.novelty.as_ref(), ledger.t())
                .with_graph_id(g_id)
        });

        // Evaluate modify policies with full f:query support using detailed API
        let decision = policy
            .allow_modify_flake_async_detailed(
                &flake.s,
                &flake.p,
                &flake.o,
                &subject_classes,
                executor,
                &async_tracker,
            )
            .await?;

        if let PolicyDecision::Denied { .. } = &decision {
            // Extract error message from the candidate restrictions, or use default
            let message = decision
                .deny_message()
                .unwrap_or("Policy enforcement prevents modification.");
            return Err(PolicyError::modify_denied(message.to_string()).into());
        }
    }
    Ok(())
}

/// Collect the set of variables referenced by INSERT/DELETE templates.
///
/// Used to project the WHERE-result Batch down to only the columns flake
/// generation actually reads, before materialization or any further copies.
fn collect_template_vars(template_groups: &[&[TripleTemplate]]) -> Vec<VarId> {
    let mut seen: HashSet<VarId> = HashSet::new();
    let mut out: Vec<VarId> = Vec::new();
    for group in template_groups {
        for tmpl in *group {
            for term in [&tmpl.subject, &tmpl.predicate, &tmpl.object] {
                if let TemplateTerm::Var(v) = term {
                    if seen.insert(*v) {
                        out.push(*v);
                    }
                }
            }
        }
    }
    out
}

/// Stats collected while streaming the WHERE result into the accumulator.
///
/// - `total_binding_rows` is the sum of row counts across all batches. The
///   caller uses this (combined with `has_where_clause`) to decide whether
///   all-literal INSERT templates should fire once post-loop against
///   `Batch::single_empty()`. Using the emitted-row total rather than
///   "any batch arrived" is important: operators like VALUES, geo/vector
///   search, and some join/optional shapes legitimately emit an empty
///   batch to signal a zero-row result, and those cases must be treated
///   as "WHERE matched nothing" for fallback purposes.
/// - `retraction_count` / `assertion_count` are the pre-dedup totals pushed
///   into the accumulator (for tracing).
struct WhereStreamStats {
    total_binding_rows: u64,
    retraction_count: usize,
    assertion_count: usize,
}

/// Stream the WHERE result into `acc`, projecting → materializing encoded
/// bindings in place → generating retractions → hydrating list-index meta →
/// pushing into the accumulator, one batch at a time. Assertions are
/// generated and pushed on the same batch when not in pure-delete mode.
///
/// `template_vars` is the union of variables referenced by INSERT/DELETE
/// templates; WHERE-only helper columns are dropped before materialization
/// to keep per-batch memory tied to template width, not WHERE width.
///
/// **Hydration must run before push.** `Flake` identity includes `m`, so a
/// raw retraction with `m = None` would collapse with its peers in the
/// accumulator before hydrate had a chance to fill in the list-index — we'd
/// end up retracting only one of N list entries. Hydrating per batch keeps
/// `m` correct on every retraction before it reaches the dedup layer.
async fn stream_where_into_accumulator(
    ledger: &LedgerState,
    txn: &mut Txn,
    template_vars: &[VarId],
    generator: &mut FlakeGenerator<'_>,
    pure_delete: bool,
    reverse_graph: &HashMap<Sid, GraphId>,
    acc: &mut FlakeAccumulator,
) -> Result<WhereStreamStats> {
    // Lower transaction WHERE clause to query patterns.
    //
    // - JSON-LD updates: `txn.where_patterns` is an UnresolvedPattern list, lowered here using the
    //   current ledger snapshot as the IRI encoder.
    // - SPARQL UPDATE (Modify): `txn.sparql_where` is lowered here using the SPARQL lowering
    //   pipeline + the shared query engine, also using the current ledger snapshot as the IRI encoder.
    let mut query_patterns = if let Some(sparql_where) = txn.sparql_where.as_ref() {
        lower_sparql_where_patterns(sparql_where, &ledger.snapshot, &mut txn.vars)?
    } else {
        // Lower UnresolvedPattern to Pattern using the ledger's LedgerSnapshot as the IRI encoder.
        // This also assigns VarIds to any variables referenced in WHERE patterns.
        lower_where_patterns(&txn.where_patterns, &ledger.snapshot, &mut txn.vars)?
    };

    // If VALUES clause present, prepend it as first pattern (seeds the join)
    if let Some(inline_values) = &txn.values {
        let values_pattern = inline_values_to_pattern(inline_values)?;
        query_patterns.insert(0, values_pattern);
    }

    // If no patterns at all (no WHERE, no VALUES), the streaming cursor's
    // SingleEmpty variant emits one empty batch (schema=[], len=0) so the
    // per-batch loop below still fires — `generate_retractions` /
    // `generate_assertions` interpret an empty-schema-empty batch as "single
    // empty solution", letting all-literal templates fire once.

    // Select the default graph(s) for WHERE execution.
    //
    // SPARQL Update semantics:
    // - `USING <g>` clauses scope WHERE evaluation (default graphs). Multiple USING clauses
    //   are evaluated as a merged default graph.
    // - `WITH <g>` scopes WHERE evaluation only when no USING is present
    //
    // JSON-LD Update semantics:
    // - top-level `graph` scopes WHERE evaluation (default graph)
    let desired_where_default_graph_iris: Vec<&str> =
        if let Some(sparql_where) = txn.sparql_where.as_ref() {
            if !sparql_where.using_default_graph_iris.is_empty() {
                sparql_where
                    .using_default_graph_iris
                    .iter()
                    .map(std::string::String::as_str)
                    .collect()
            } else if let Some(with) = sparql_where.with_graph_iri.as_deref() {
                vec![with]
            } else {
                Vec::new()
            }
        } else if let Some(iris) = txn.update_where_default_graph_iris.as_deref() {
            iris.iter().map(std::string::String::as_str).collect()
        } else {
            Vec::new()
        };

    // Resolve IRI -> graph id, preferring the snapshot registry with a binary-store fallback.
    let binary_store: Option<Arc<fluree_db_binary_index::BinaryIndexStore>> =
        ledger.binary_store.as_ref().and_then(|te| {
            Arc::clone(&te.0)
                .downcast::<fluree_db_binary_index::BinaryIndexStore>()
                .ok()
        });
    let resolve_graph_id = |iri: &str| -> Option<GraphId> {
        ledger
            .snapshot
            .graph_registry
            .graph_id_for_iri(iri)
            .or_else(|| binary_store.as_ref().and_then(|s| s.graph_id_for_iri(iri)))
    };

    // Base GraphDbRef is used to provide snapshot/overlay/time; dataset controls active graphs.
    // For multi-default-graph datasets we use g_id=0 as the base reference.
    let base_db = if desired_where_default_graph_iris.len() <= 1 {
        let base_g_id: GraphId = desired_where_default_graph_iris
            .first()
            .and_then(|iri| resolve_graph_id(iri))
            .unwrap_or(0);
        ledger.as_graph_db_ref(base_g_id)
    } else {
        ledger.as_graph_db_ref(0)
    };

    let make_graph_ref = |g_id: GraphId| -> fluree_db_query::GraphRef {
        fluree_db_query::GraphRef::new(
            base_db.snapshot,
            g_id,
            base_db.overlay,
            base_db.t,
            base_db.snapshot.ledger_id.as_str(),
        )
    };

    let composite_graph_key =
        |iri: &str| -> String { format!("{}#{}", base_db.snapshot.ledger_id, iri) };

    let mut runtime_dataset = if desired_where_default_graph_iris.len() <= 1 {
        fluree_db_query::DataSet::new().with_default_graph(make_graph_ref(base_db.g_id))
    } else {
        let mut ds = fluree_db_query::DataSet::new();
        for iri in &desired_where_default_graph_iris {
            let Some(g_id) = resolve_graph_id(iri) else {
                continue;
            };
            ds = ds.with_default_graph(make_graph_ref(g_id));
        }
        ds
    };

    // Prefer snapshot GraphRegistry, but also include binary-store graph entries as a fallback.
    // This mirrors the query path's safety fallback when registry is temporarily missing entries.
    //
    // Named-graph visibility restrictions:
    // - SPARQL UPDATE `USING NAMED <iri>` restricts WHERE-visible named graphs to that one graph
    // - JSON-LD update `fromNamed` restricts WHERE-visible named graphs to the provided set,
    //   optionally providing dataset-local aliases for `["graph", "<alias>", ...]` patterns.
    let allowed_named_graphs: Option<Vec<(String, Option<String>)>> =
        if let Some(w) = txn.sparql_where.as_ref() {
            if w.using_named_graph_iris.is_empty() {
                None
            } else {
                Some(
                    w.using_named_graph_iris
                        .iter()
                        .map(|iri| (iri.clone(), None))
                        .collect(),
                )
            }
        } else {
            txn.update_where_named_graphs
                .as_ref()
                .map(|v| v.iter().map(|g| (g.iri.clone(), g.alias.clone())).collect())
        };

    let mut seen_named_keys: HashSet<Arc<str>> = HashSet::new();

    if let Some(allowlist) = allowed_named_graphs {
        for (iri, alias) in allowlist {
            let g_id = resolve_graph_id(&iri);
            let Some(g_id) = g_id else {
                continue;
            };

            let iri_key: Arc<str> = Arc::from(iri.as_str());
            if seen_named_keys.insert(Arc::clone(&iri_key)) {
                runtime_dataset =
                    runtime_dataset.with_named_graph(Arc::clone(&iri_key), make_graph_ref(g_id));
            }

            // Also register a composite ledger-local graph identifier:
            // `<ledger_id>#<graph_iri>`. This matches the syntax used to reference a named
            // graph as a queryable graph source (e.g., via `from`), and allows GRAPH patterns
            // to use that same identifier when desired.
            let composite = composite_graph_key(iri_key.as_ref());
            let composite_key: Arc<str> = Arc::from(composite.as_str());
            if seen_named_keys.insert(Arc::clone(&composite_key)) {
                runtime_dataset = runtime_dataset
                    .with_named_graph(Arc::clone(&composite_key), make_graph_ref(g_id));
            }

            if let Some(alias) = alias {
                let alias_key: Arc<str> = Arc::from(alias.as_str());
                if seen_named_keys.insert(Arc::clone(&alias_key)) {
                    runtime_dataset = runtime_dataset
                        .with_named_graph(Arc::clone(&alias_key), make_graph_ref(g_id));
                }
            }
        }
    } else {
        for (g_id, iri) in ledger.snapshot.graph_registry.iter_entries() {
            let iri: Arc<str> = Arc::from(iri);
            if seen_named_keys.insert(Arc::clone(&iri)) {
                runtime_dataset =
                    runtime_dataset.with_named_graph(Arc::clone(&iri), make_graph_ref(g_id));
            }

            let composite = composite_graph_key(iri.as_ref());
            let composite_key: Arc<str> = Arc::from(composite.as_str());
            if seen_named_keys.insert(Arc::clone(&composite_key)) {
                runtime_dataset = runtime_dataset
                    .with_named_graph(Arc::clone(&composite_key), make_graph_ref(g_id));
            }
        }

        if let Some(store) = &binary_store {
            for (g_id, iri) in store.graph_entries() {
                let iri: Arc<str> = Arc::from(iri);
                if seen_named_keys.insert(Arc::clone(&iri)) {
                    runtime_dataset =
                        runtime_dataset.with_named_graph(Arc::clone(&iri), make_graph_ref(g_id));
                }

                let composite = composite_graph_key(iri.as_ref());
                let composite_key: Arc<str> = Arc::from(composite.as_str());
                if seen_named_keys.insert(Arc::clone(&composite_key)) {
                    runtime_dataset = runtime_dataset
                        .with_named_graph(Arc::clone(&composite_key), make_graph_ref(g_id));
                }
            }
        }
    }

    // Open the streaming WHERE cursor. For empty patterns it emits one
    // empty-schema/empty-len batch then EOF, mirroring the eager API's
    // `vec![Batch::empty(...)]` behavior.
    let mut cursor = fluree_db_query::execute_where_streaming_in_dataset(
        base_db,
        &txn.vars,
        &query_patterns,
        None,
        Some(&runtime_dataset),
    )
    .await
    .map_err(TransactError::Query)?;

    let mut total_binding_rows: u64 = 0;
    let mut retraction_count: usize = 0;
    let mut assertion_count: usize = 0;

    while let Some(batch) = cursor.next_batch().await.map_err(TransactError::Query)? {
        total_binding_rows += batch.len() as u64;

        // Per-batch shape: project → materialize in place → generate →
        // hydrate (retractions only) → push. Batch drops at end of iter.
        let batch = batch.project_owned(template_vars);
        let batch = materialize_encoded_bindings_for_txn(ledger, batch)?;

        // Per-batch `delete_gen` span. Nested under `where_exec`. Fields:
        // `template_count` (stable per txn), `retraction_count` (per-batch
        // generated count, recorded deferred).
        let delete_span = tracing::debug_span!(
            "delete_gen",
            template_count = txn.delete_templates.len(),
            retraction_count = tracing::field::Empty,
        );
        let retractions = {
            let _g = delete_span.enter();
            let mut r = generator.generate_retractions(&txn.delete_templates, &batch)?;

            // Hydrate BEFORE push. `Flake::eq` includes `m`, so raw retractions
            // with `m = None` must have their list-index filled in from the
            // asserted flake before they reach the accumulator — otherwise
            // N list entries with the same `(s,p,o,dt)` would collapse to one
            // retraction survivor and only one list entry would actually be
            // retracted from the index.
            hydrate_list_index_meta_for_retractions(ledger, &mut r, reverse_graph).await?;

            delete_span.record("retraction_count", r.len() as u64);
            r
        };
        retraction_count += retractions.len();
        acc.push_retractions(retractions);

        if !pure_delete {
            // Per-batch `insert_gen` span. Nested under `where_exec`.
            let insert_span = tracing::debug_span!(
                "insert_gen",
                template_count = txn.insert_templates.len(),
                assertion_count = tracing::field::Empty,
            );
            let assertions = {
                let _g = insert_span.enter();
                let a = generator.generate_assertions(&txn.insert_templates, &batch)?;
                insert_span.record("assertion_count", a.len() as u64);
                a
            };
            assertion_count += assertions.len();
            acc.push_assertions(assertions);
        }
    }
    cursor.close();

    Ok(WhereStreamStats {
        total_binding_rows,
        retraction_count,
        assertion_count,
    })
}

/// Lower a stored SPARQL WHERE clause (from SPARQL UPDATE) into query patterns.
///
/// This constructs a synthetic `SELECT * WHERE { ... }` query so we can reuse the
/// existing SPARQL lowering pipeline (which already supports subqueries + aggregates)
/// and keep one execution path in `fluree-db-query`.
fn lower_sparql_where_patterns(
    sparql_where: &crate::ir::SparqlWhereClause,
    encoder: &fluree_db_core::LedgerSnapshot,
    vars: &mut VarRegistry,
) -> Result<Vec<Pattern>> {
    // Propagate the original parsed span so lowering errors report helpful locations.
    let span = sparql_where.pattern.span();
    let where_clause = SparqlWhereClauseAst::new(sparql_where.pattern.clone(), true, span);
    let select = SelectClause::star(span);
    let modifiers = SolutionModifiers::new();
    let select_query = SelectQuery::new(select, where_clause, modifiers, span);
    let ast = SparqlAst::new(
        sparql_where.prologue.clone(),
        SparqlQueryBody::Select(select_query),
        span,
    );

    lower_sparql(&ast, encoder, vars)
        .map(|pq| pq.patterns)
        .map_err(Into::into)
}

/// Materialize any late-materialized (`Binding::Encoded*`) values in a WHERE-result batch.
///
/// Transaction flake generation (`FlakeGenerator`) expects concrete `Binding::Sid` and
/// `Binding::Lit` values, and will error on encoded bindings.
///
/// This consumes `batch` by value and rewrites encoded bindings in place. Two
/// short-circuits keep the steady state cheap:
///
/// - If no binary store is configured, encoded bindings cannot appear and the
///   batch is returned as-is.
/// - Per column: a one-pass scan checks whether the column contains any
///   `Encoded*` variant. Already-concrete columns are left untouched (no
///   per-binding clone, no Vec reallocation). Only columns that need it pay
///   for in-place rewriting.
fn materialize_encoded_bindings_for_txn(ledger: &LedgerState, batch: Batch) -> Result<Batch> {
    if batch.is_empty() {
        return Ok(batch);
    }

    // If no binary store is present, encoded bindings should not appear.
    let Some(te) = &ledger.binary_store else {
        return Ok(batch);
    };
    let Ok(store) = Arc::clone(&te.0).downcast::<fluree_db_binary_index::BinaryIndexStore>() else {
        return Ok(batch);
    };

    let gv = fluree_db_binary_index::BinaryGraphView::new(Arc::clone(&store), 0);

    let (schema, mut columns, len) = batch.into_parts();

    for col in &mut columns {
        if !column_needs_materialization(col) {
            continue;
        }
        for b in col.iter_mut() {
            materialize_one_binding(b, ledger, &gv)?;
        }
    }

    // Use `from_parts` (not `Batch::new`) so the row count survives when
    // `columns` is empty — e.g. an `empty_schema_with_len(N)` batch produced
    // by `project_owned` against an all-literal template set must keep `N`
    // so flake generation fires once per WHERE solution row, not once total.
    Batch::from_parts(schema, columns, len).map_err(|e| TransactError::Query(e.into()))
}

/// True if any binding in `col` is an `Encoded*` variant requiring rewrite.
/// Tight loop with no allocation; `Encoded*` columns return early on first hit.
fn column_needs_materialization(col: &[Binding]) -> bool {
    col.iter().any(|b| {
        matches!(
            b,
            Binding::EncodedSid { .. } | Binding::EncodedPid { .. } | Binding::EncodedLit { .. }
        )
    })
}

/// Rewrite a single `Binding` in place if it is an `Encoded*` variant.
/// Already-concrete bindings are left untouched (no clone).
fn materialize_one_binding(
    b: &mut Binding,
    ledger: &LedgerState,
    gv: &fluree_db_binary_index::BinaryGraphView,
) -> Result<()> {
    let store_ref = gv.store();
    match b {
        Binding::EncodedSid { s_id } => {
            let iri = store_ref.resolve_subject_iri(*s_id).map_err(|e| {
                TransactError::Query(fluree_db_query::QueryError::Internal(format!(
                    "resolve_subject_iri: {e}"
                )))
            })?;
            let sid = ledger.snapshot.encode_iri(&iri).ok_or_else(|| {
                TransactError::Query(fluree_db_query::QueryError::Internal(format!(
                    "encode_iri returned None for subject IRI: {iri}"
                )))
            })?;
            *b = Binding::Sid(sid);
        }
        Binding::EncodedPid { p_id } => {
            let iri = store_ref.resolve_predicate_iri(*p_id).ok_or_else(|| {
                TransactError::Query(fluree_db_query::QueryError::Internal(format!(
                    "unknown predicate id: {p_id}"
                )))
            })?;
            let sid = ledger.snapshot.encode_iri(iri).ok_or_else(|| {
                TransactError::Query(fluree_db_query::QueryError::Internal(format!(
                    "encode_iri returned None for predicate IRI: {iri}"
                )))
            })?;
            *b = Binding::Sid(sid);
        }
        Binding::EncodedLit {
            o_kind,
            o_key,
            p_id,
            dt_id,
            lang_id,
            i_val,
            t,
        } => {
            let (o_kind, o_key, p_id, dt_id, lang_id, i_val, t) =
                (*o_kind, *o_key, *p_id, *dt_id, *lang_id, *i_val, *t);
            let val = gv
                .decode_value_from_kind(o_kind, o_key, p_id, dt_id, lang_id)
                .map_err(|e| {
                    TransactError::Query(fluree_db_query::QueryError::Internal(format!(
                        "decode_value_from_kind: {e}"
                    )))
                })?;
            match val {
                FlakeValue::Ref(sid) => {
                    *b = Binding::Sid(sid);
                }
                other => {
                    let dt_sid = store_ref
                        .dt_sids()
                        .get(dt_id as usize)
                        .cloned()
                        .unwrap_or_else(|| Sid::new(0, ""));
                    let dt_iri = store_ref.sid_to_iri(&dt_sid).ok_or_else(|| {
                        TransactError::Query(fluree_db_query::QueryError::Internal(format!(
                            "sid_to_iri failed: unknown namespace code {} for datatype {:?}",
                            dt_sid.namespace_code, dt_sid.name
                        )))
                    })?;
                    let dt = ledger.snapshot.encode_iri(&dt_iri).ok_or_else(|| {
                        TransactError::Query(fluree_db_query::QueryError::Internal(format!(
                            "encode_iri returned None for datatype IRI: {dt_iri}"
                        )))
                    })?;
                    let meta = store_ref.decode_meta(lang_id, i_val);
                    let dtc = meta
                        .as_ref()
                        .and_then(|m| m.lang.as_ref())
                        .map(|s| {
                            fluree_db_core::DatatypeConstraint::LangTag(std::sync::Arc::from(
                                s.as_str(),
                            ))
                        })
                        .unwrap_or_else(|| fluree_db_core::DatatypeConstraint::Explicit(dt));
                    *b = Binding::Lit {
                        val: other,
                        dtc,
                        t: Some(t),
                        op: None,
                        p_id: Some(p_id),
                    };
                }
            }
        }
        // Already-concrete bindings need no rewrite.
        _ => {}
    }
    Ok(())
}

/// Lower UnresolvedPattern list to Pattern list
///
/// This converts string IRIs to encoded Sids using the database, and assigns
/// VarIds to variables using the provided VarRegistry (shared with INSERT/DELETE).
fn lower_where_patterns(
    patterns: &[UnresolvedPattern],
    db: &fluree_db_core::LedgerSnapshot,
    vars: &mut VarRegistry,
) -> Result<Vec<Pattern>> {
    let mut pp_counter: u32 = 0;
    lower_unresolved_patterns(patterns, db, vars, &mut pp_counter)
        .map_err(|e| TransactError::Parse(format!("WHERE pattern lowering: {e}")))
}

/// Generate a unique transaction ID for blank node skolemization
pub fn generate_txn_id() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{now:x}")
}

/// Convert a Binding to a (FlakeValue, datatype Sid) pair for flake generation
///
/// Returns `None` for non-materializable bindings (Unbound, Poisoned, Grouped, Iri).
/// This is used when generating retraction flakes from query results.
///
/// When a `Materializer` is provided, encoded bindings (`EncodedLit`, `EncodedSid`)
/// are decoded via the binary index store before conversion. Without a materializer,
/// encoded bindings return `None` (this can cause upsert to silently skip retractions
/// for values that live in the binary index — see issue #88).
fn binding_to_flake_object(
    binding: &Binding,
    materializer: Option<&mut fluree_db_query::Materializer>,
) -> Option<(FlakeValue, Sid)> {
    match binding {
        Binding::Sid(sid) => Some((FlakeValue::Ref(sid.clone()), Sid::new(1, "id"))),
        Binding::IriMatch { primary_sid, .. } => {
            Some((FlakeValue::Ref(primary_sid.clone()), Sid::new(1, "id")))
        }
        Binding::Lit { val, dtc, .. } => Some((val.clone(), dtc.datatype().clone())),
        Binding::EncodedLit { .. } | Binding::EncodedSid { .. } | Binding::EncodedPid { .. } => {
            match materializer {
                Some(mat) => {
                    let materialized = mat.to_term(binding);
                    binding_to_flake_object(&materialized, None)
                }
                None => None,
            }
        }
        // Non-materializable bindings
        Binding::Unbound | Binding::Poisoned => None,
        Binding::Grouped(_) => {
            debug_assert!(
                false,
                "Grouped binding encountered in flake generation (unexpected)"
            );
            None
        }
        Binding::Iri(_) => {
            debug_assert!(
                false,
                "Raw IRI binding cannot be materialized to flake (no SID)"
            );
            None
        }
    }
}

/// Convert a TemplateTerm to a Binding for VALUES clause
fn template_term_to_binding(term: &TemplateTerm) -> Result<Binding> {
    match term {
        TemplateTerm::Sid(sid) => Ok(Binding::Sid(sid.clone())),
        TemplateTerm::Value(val) => {
            let dt = infer_datatype(val);
            Ok(Binding::lit(val.clone(), dt))
        }
        TemplateTerm::Var(_) => Err(TransactError::InvalidTerm(
            "Variables not allowed in VALUES data rows".to_string(),
        )),
        TemplateTerm::BlankNode(_) => Err(TransactError::InvalidTerm(
            "Blank nodes not allowed in VALUES data rows".to_string(),
        )),
    }
}

/// Convert InlineValues to Pattern::Values
fn inline_values_to_pattern(values: &InlineValues) -> Result<Pattern> {
    let vars = values.vars.clone();
    let rows: Result<Vec<Vec<Binding>>> = values
        .rows
        .iter()
        .map(|row| row.iter().map(template_term_to_binding).collect())
        .collect();
    Ok(Pattern::Values { vars, rows: rows? })
}

/// Generate deletions for Upsert transactions
///
/// For each (subject, predicate, graph) tuple with concrete SIDs in the insert templates,
/// query existing values and generate retractions for them. This implements the
/// "replace mode" semantics of Upsert.
///
/// Named graph support: retractions are created in the same graph as the insert templates
/// to ensure proper cancellation with assertions.
async fn generate_upsert_deletions(
    ledger: &LedgerState,
    txn: &Txn,
    new_t: i64,
    graph_sids: &std::collections::HashMap<u16, Sid>,
) -> Result<Vec<fluree_db_core::Flake>> {
    use fluree_db_binary_index::BinaryGraphView;
    use fluree_db_core::Flake;
    use fluree_db_query::materializer::JoinKeyMode;
    use fluree_db_query::{BinaryRangeProvider, Materializer};

    // Collect unique (subject, predicate, graph_id) tuples from insert templates
    // Include graph_id to ensure retractions are created in the correct graph
    let mut spg_tuples: HashSet<(Sid, Sid, Option<u16>)> = HashSet::new();
    for template in &txn.insert_templates {
        if let (TemplateTerm::Sid(s), TemplateTerm::Sid(p)) =
            (&template.subject, &template.predicate)
        {
            spg_tuples.insert((s.clone(), p.clone(), template.graph_id));
        }
        // Variables and blank nodes are skipped - we can't query for them
    }

    if spg_tuples.is_empty() {
        return Ok(Vec::new());
    }

    // Extract the binary index store and DictNovelty (if present) so we can
    // materialize EncodedLit/EncodedSid bindings returned by the binary scan path.
    let brp_ref = ledger
        .snapshot
        .range_provider
        .as_ref()
        .and_then(|rp| rp.as_any().downcast_ref::<BinaryRangeProvider>());
    let binary_store = brp_ref.map(|brp| Arc::clone(brp.store()));
    let dict_novelty = brp_ref.map(|brp| Arc::clone(brp.dict_novelty()));

    let mut retractions = Vec::new();

    // Query existing values for each (subject, predicate, graph) tuple
    let mut query_vars = VarRegistry::new();
    let o_var = query_vars.get_or_insert("?o");

    for (subject, predicate, graph_id) in spg_tuples {
        // IMPORTANT: `TripleTemplate.graph_id` is a transaction-local ID.
        // It must be translated to a ledger-stable GraphId before we can query
        // the correct per-graph index partition.
        //
        // txn_local_g_id -> graph IRI (txn.graph_delta) -> ledger g_id (GraphRegistry)
        let ledger_g_id: Option<u16> = graph_id.and_then(|txn_g_id| {
            txn.graph_delta
                .get(&txn_g_id)
                .and_then(|iri| ledger.snapshot.graph_registry.graph_id_for_iri(iri))
        });

        // Query: <subject> <predicate> ?o
        let pattern = TriplePattern::new(
            Ref::Sid(subject.clone()),
            Ref::Sid(predicate.clone()),
            Term::Var(o_var),
        );

        let batches = if graph_id.is_some() {
            // Named graph: translate txn-local g_id to ledger g_id before querying.
            match ledger_g_id {
                None => {
                    // Graph is not yet in the ledger registry (new graph in this txn),
                    // so there cannot be existing values to retract.
                    Vec::new()
                }
                Some(g_id) => {
                    if ledger.snapshot.range_provider.is_some() {
                        fluree_db_query::execute_pattern_with_overlay_at(
                            ledger.as_graph_db_ref(g_id),
                            &query_vars,
                            pattern,
                            None,
                        )
                        .await?
                    } else {
                        // No binary store available (genesis / not indexed): scan novelty directly.
                        query_novelty_for_graph(ledger, &subject, &predicate, g_id, o_var)
                    }
                }
            }
        } else {
            // Default graph: use standard query path through range_provider
            fluree_db_query::execute_pattern_with_overlay_at(
                ledger.as_graph_db_ref(0),
                &query_vars,
                pattern,
                None,
            )
            .await?
        };

        // Convert each result to a retraction flake in the appropriate graph.
        // Here we use the txn-local g_id to look up the graph Sid (flake.g).
        let graph_sid: Option<Sid> = match graph_id {
            None => None,
            Some(txn_g_id) => Some(
                graph_sids.get(&txn_g_id).cloned().ok_or_else(|| {
                    TransactError::FlakeGeneration(format!(
                        "upsert deletion generation references graph_id {txn_g_id} but no graph Sid was provided; \
                         this indicates a bug in graph delta/sid wiring"
                    ))
                })?,
            ),
        };

        // Create a materializer for this graph context if a binary store exists.
        // BinaryGraphView::with_novelty handles watermark routing internally,
        // so novelty-only string/subject IDs resolve correctly.
        let effective_g_id = ledger_g_id.unwrap_or(0);
        let mut materializer = binary_store.as_ref().map(|store| {
            let view = BinaryGraphView::with_novelty(
                Arc::clone(store),
                effective_g_id,
                dict_novelty.clone(),
            );
            Materializer::new(view, JoinKeyMode::SingleLedger)
        });

        for batch in &batches {
            for row in 0..batch.len() {
                let flake_obj = batch
                    .get(row, o_var)
                    .and_then(|b| binding_to_flake_object(b, materializer.as_mut()));
                if let Some((o, dt)) = flake_obj {
                    let flake = match graph_sid.clone() {
                        Some(g) => Flake::new_in_graph(
                            g,
                            subject.clone(),
                            predicate.clone(),
                            o,
                            dt,
                            new_t,
                            false, // retraction
                            None,
                        ),
                        None => Flake::new(
                            subject.clone(),
                            predicate.clone(),
                            o,
                            dt,
                            new_t,
                            false, // retraction
                            None,
                        ),
                    };
                    retractions.push(flake);
                }
            }
        }
    }

    Ok(retractions)
}

/// Query novelty directly for a specific named graph
///
/// This function scans the novelty overlay for flakes matching the given
/// subject, predicate, and graph context. It's used for named graph upserts
/// because the db.range_provider is scoped to the default graph (g_id=0).
fn query_novelty_for_graph(
    ledger: &LedgerState,
    subject: &Sid,
    predicate: &Sid,
    target_g_id: u16,
    o_var: VarId,
) -> Vec<Batch> {
    use fluree_db_core::IndexType;

    // Collect matching flakes from novelty for the target graph
    let mut matching_values = Vec::new();
    ledger.novelty.for_each_overlay_flake(
        target_g_id,
        IndexType::Spot,
        None,
        None,
        true,
        ledger.t(),
        &mut |flake| {
            // Check if flake matches (subject, predicate) and is an assertion
            if &flake.s == subject && &flake.p == predicate && flake.op {
                matching_values.push((flake.o.clone(), flake.dt.clone()));
            }
        },
    );

    // Convert to batch format
    if matching_values.is_empty() {
        return Vec::new();
    }

    // Create a simple batch with just the object values
    let schema: Arc<[VarId]> = Arc::new([o_var]);
    let mut o_col = Vec::with_capacity(matching_values.len());
    for (o, dt) in &matching_values {
        o_col.push(Binding::from_object(o.clone(), dt.clone()));
    }

    match Batch::new(schema, vec![o_col]) {
        Ok(batch) => vec![batch],
        Err(_) => Vec::new(),
    }
}
/// Stage a transaction with SHACL validation
///
/// This is the same as [`stage`], but additionally validates the staged flakes
/// against SHACL shapes compiled from the database. If validation fails, the
/// function returns an error with the validation report.
///
/// # Arguments
///
/// * `ledger` - The ledger state (consumed by value)
/// * `txn` - The parsed transaction IR
/// * `ns_registry` - Namespace registry for IRI resolution
/// * `options` - Optional configuration for backpressure, policy, and tracking
/// * `shacl_cache` - Compiled SHACL shapes for validation
///
/// # Returns
///
/// Returns `(LedgerView, NamespaceRegistry)` if staging and validation succeed.
/// Returns `TransactError::ShaclViolation` if SHACL validation fails.
#[cfg(feature = "shacl")]
pub async fn stage_with_shacl(
    ledger: LedgerState,
    txn: Txn,
    ns_registry: NamespaceRegistry,
    options: StageOptions<'_>,
    shacl_cache: &ShaclCache,
) -> Result<(LedgerView, NamespaceRegistry)> {
    // Capture graph_delta + tracker before stage() consumes the options/txn.
    let graph_delta = txn.graph_delta.clone();
    let tracker = options.tracker;

    // First, perform regular staging
    let (view, mut ns_registry) = stage(ledger, txn, ns_registry, options).await?;

    // Fast path: if there are no SHACL shapes, elide validation entirely.
    // This ensures SHACL has *zero* transaction-time overhead unless rules exist.
    if shacl_cache.is_empty() {
        return Ok((view, ns_registry));
    }

    // Rebuild graph_sids from the cloned graph_delta + returned ns_registry.
    // These IRIs were already resolved during stage(), so sid_for_iri will find
    // the prefix already registered — no new allocations.
    let graph_sids: HashMap<GraphId, Sid> = graph_delta
        .iter()
        .map(|(&g_id, iri)| (g_id, ns_registry.sid_for_iri(iri)))
        .collect();

    // Create SHACL engine from cache
    let engine = ShaclEngine::new(shacl_cache.clone());

    // Validate staged flakes against shapes (per graph). `None` for
    // `enabled_graphs` means "validate every graph with staged flakes" —
    // this legacy path doesn't consult per-graph config.
    let report = validate_staged_nodes(&view, &engine, Some(&graph_sids), tracker, None).await?;

    if !report.conforms {
        return Err(TransactError::ShaclViolation(format_shacl_report(&report)));
    }

    Ok((view, ns_registry))
}

/// Per-graph SHACL policy — how a specific graph's violations should be
/// treated at transaction time.
///
/// Absence from the policy map passed to [`validate_view_with_shacl`] means
/// the graph is **disabled** (shapes do not fire for subjects in that graph).
/// Presence with `mode = Reject` means violations cause the transaction to
/// fail; `mode = Warn` means violations are returned for the caller to log.
#[cfg(feature = "shacl")]
#[derive(Debug, Clone, Copy)]
pub struct ShaclGraphPolicy {
    pub mode: fluree_db_core::ledger_config::ValidationMode,
}

/// Outcome of a staged SHACL validation, split by mode so the caller can
/// apply warn (log-and-continue) vs reject (propagate as error) per graph.
#[cfg(feature = "shacl")]
#[derive(Debug, Default)]
pub struct ShaclValidationOutcome {
    /// Violations from graphs in `Reject` mode. Non-empty → transaction fails.
    pub reject_violations: Vec<fluree_db_shacl::ValidationResult>,
    /// Violations from graphs in `Warn` mode. The caller should log these.
    pub warn_violations: Vec<fluree_db_shacl::ValidationResult>,
}

#[cfg(feature = "shacl")]
impl ShaclValidationOutcome {
    pub fn conforms(&self) -> bool {
        self.reject_violations.is_empty() && self.warn_violations.is_empty()
    }
}

/// Validate a staged [`LedgerView`] against SHACL shapes.
///
/// `graph_sids` provides the `GraphId → Sid` mapping for per-graph validation.
/// Pass `None` when the mapping is unavailable (e.g., commit-transfer path
/// with no per-graph routing yet) — validation falls back to the default
/// graph (g_id=0).
///
/// `per_graph_policy`:
/// - `None` = treat every graph containing staged flakes as `Reject` mode
///   (legacy / unconditional reject — matches commit-transfer's previous
///   behavior and shapes-exist heuristic).
/// - `Some(map)` = only graphs in the map are validated; their mode comes
///   from the map. Graphs absent from the map are skipped (disabled).
///
/// Returns a [`ShaclValidationOutcome`] split into reject / warn buckets.
/// The caller decides whether to propagate an error, log warnings, or both.
#[cfg(feature = "shacl")]
pub async fn validate_view_with_shacl(
    view: &LedgerView,
    shacl_cache: &ShaclCache,
    graph_sids: Option<&HashMap<GraphId, Sid>>,
    tracker: Option<&fluree_db_core::Tracker>,
    per_graph_policy: Option<&HashMap<GraphId, ShaclGraphPolicy>>,
) -> Result<ShaclValidationOutcome> {
    // Fast path: if there are no SHACL shapes, elide validation entirely.
    if shacl_cache.is_empty() {
        return Ok(ShaclValidationOutcome::default());
    }

    let engine = ShaclEngine::new(shacl_cache.clone());
    let enabled_graphs: Option<HashSet<GraphId>> =
        per_graph_policy.map(|m| m.keys().copied().collect());
    let report =
        validate_staged_nodes(view, &engine, graph_sids, tracker, enabled_graphs.as_ref()).await?;

    // Split violations by the graph's configured mode. `graph_id` on each
    // result was tagged during the per-graph loop in validate_staged_nodes.
    // When per_graph_policy is None, every violation defaults to Reject.
    let mut outcome = ShaclValidationOutcome::default();
    for r in report.results {
        if r.severity != fluree_db_shacl::Severity::Violation {
            continue;
        }
        let mode = match (per_graph_policy, r.graph_id) {
            (Some(m), Some(g_id)) => m
                .get(&g_id)
                .map(|p| p.mode)
                .unwrap_or(fluree_db_core::ledger_config::ValidationMode::Reject),
            _ => fluree_db_core::ledger_config::ValidationMode::Reject,
        };
        match mode {
            fluree_db_core::ledger_config::ValidationMode::Reject => {
                outcome.reject_violations.push(r);
            }
            fluree_db_core::ledger_config::ValidationMode::Warn => {
                outcome.warn_violations.push(r);
            }
        }
    }
    Ok(outcome)
}

/// Validate staged nodes against SHACL shapes, per graph.
///
/// Groups staged subjects by their graph and validates each group with a
/// `GraphDbRef` targeting the correct `g_id`. Shape compilation stays at
/// g_id=0 (shapes are schema-level definitions in the default graph).
///
/// When `graph_sids` is `None` (e.g., commit-transfer path where the txn
/// context is unavailable), falls back to validating all subjects against
/// the default graph (g_id=0) — matching the previous behavior.
#[cfg(feature = "shacl")]
async fn validate_staged_nodes(
    view: &LedgerView,
    engine: &ShaclEngine,
    graph_sids: Option<&HashMap<GraphId, Sid>>,
    tracker: Option<&fluree_db_core::Tracker>,
    enabled_graphs: Option<&HashSet<GraphId>>,
) -> Result<ValidationReport> {
    use fluree_vocab::namespaces::RDF;
    use fluree_vocab::rdf_names;

    // Fast path: no shapes means no validation work.
    if engine.cache().all_shapes().is_empty() {
        return Ok(ValidationReport::conforming());
    }

    if !view.has_staged() {
        return Ok(ValidationReport::conforming());
    }

    // Group staged focus nodes by graph. A subject may appear in multiple
    // graphs.
    //
    // Ref-objects of assert flakes are pulled in as focus nodes too, so
    // `sh:targetObjectsOf` shapes targeting a newly-referenced node get
    // evaluated on the write path. Retractions do NOT expand the focus set
    // via their object — removing an inbound edge doesn't introduce
    // validation work at the target.
    //
    // Predicate-target applicability (`sh:targetSubjectsOf` / `ObjectsOf`)
    // is resolved inside `ShaclEngine::validate_node` by querying the
    // post-transaction view directly. We don't pre-compute hints here
    // because hints derived from staged flakes miss the "base edge persists,
    // node touched for an unrelated reason" case — e.g., alice already has
    // `ex:ssn` in the base DB, and this txn retracts `ex:name`.
    let reverse_graph = graph_sids.map(build_reverse_graph_lookup);
    let mut subjects_by_graph: HashMap<GraphId, HashSet<Sid>> = HashMap::new();
    for flake in view.staged_flakes() {
        let g_id = match &reverse_graph {
            Some(rev) => resolve_flake_graph_id(flake, rev)?,
            // No reverse map (commit-transfer path): fall back to default graph
            None => 0,
        };
        // Subject is always a focus (including for retractions — validators
        // must still see retracted-on subjects so class/node-targeted shapes
        // can re-check cardinality, and so the engine's post-state check can
        // notice that a predicate-target no longer applies).
        subjects_by_graph
            .entry(g_id)
            .or_default()
            .insert(flake.s.clone());

        // Ref-objects of assert flakes become focus nodes in the flake's
        // graph. This is the only way a node that wasn't otherwise touched
        // by the transaction gets pulled in to be validated against
        // `sh:targetObjectsOf` shapes targeting the newly-introduced edge.
        if flake.op {
            if let fluree_db_core::FlakeValue::Ref(obj) = &flake.o {
                subjects_by_graph
                    .entry(g_id)
                    .or_default()
                    .insert(obj.clone());
            }
        }
    }

    let snapshot = view.db();
    let mut all_results = Vec::new();

    for (g_id, subjects) in &subjects_by_graph {
        // Per-graph enable/disable: when the caller supplies an explicit
        // enabled set, graphs not in the set are skipped. Subjects staged in
        // a disabled graph therefore receive no shape validation from this
        // transaction, which matches the documented `shacl.enabled: false`
        // semantics for that graph (`override-control.md`).
        if let Some(enabled) = enabled_graphs {
            if !enabled.contains(g_id) {
                continue;
            }
        }

        // Build GraphDbRef for this graph.
        // Use staged_t so GraphDbRef sees staged flakes (which have t > snapshot.t).
        let mut db = fluree_db_core::GraphDbRef::new(snapshot, *g_id, view, view.staged_t());
        if let Some(t) = tracker {
            db = db.with_tracker(t);
        }

        for subject in subjects {
            // Get the node's types for shape targeting
            let rdf_type = Sid::new(RDF, rdf_names::TYPE);
            let type_flakes = db
                .range(
                    fluree_db_core::IndexType::Spot,
                    fluree_db_core::RangeTest::Eq,
                    fluree_db_core::RangeMatch::subject_predicate(subject.clone(), rdf_type),
                )
                .await?;

            let node_types: Vec<Sid> = type_flakes
                .iter()
                .filter_map(|f| {
                    if let fluree_db_core::FlakeValue::Ref(type_sid) = &f.o {
                        Some(type_sid.clone())
                    } else {
                        None
                    }
                })
                .collect();

            // Validate this node. Predicate-target applicability is resolved
            // inside `validate_node` via post-state range queries — see the
            // SubjectsOf/ObjectsOf handling there for why hints can't be
            // reliably built from staged flakes alone.
            let report = engine.validate_node(db, subject, &node_types).await?;
            // Tag each result with the graph it was validated under so the
            // caller can route warn vs reject per-graph (see
            // `ShaclValidationOutcome`).
            all_results.extend(report.results.into_iter().map(|mut r| {
                r.graph_id = Some(*g_id);
                r
            }));
        }
    }

    // Check conformance
    let conforms = all_results
        .iter()
        .all(|r| r.severity != fluree_db_shacl::Severity::Violation);

    Ok(ValidationReport {
        conforms,
        results: all_results,
    })
}

/// Format a SHACL validation report as a human-readable string
#[cfg(feature = "shacl")]
fn format_shacl_report(report: &ValidationReport) -> String {
    use std::fmt::Write;

    let mut output = String::new();
    writeln!(
        &mut output,
        "SHACL validation failed with {} violation(s):",
        report.violation_count()
    )
    .ok();

    for (i, result) in report
        .results
        .iter()
        .filter(|r| r.severity == fluree_db_shacl::Severity::Violation)
        .enumerate()
    {
        writeln!(&mut output, "  {}. {}", i + 1, result.message).ok();
        writeln!(
            &mut output,
            "     Focus node: {}{}",
            result.focus_node.namespace_code, result.focus_node.name
        )
        .ok();
        if let Some(path) = &result.result_path {
            writeln!(
                &mut output,
                "     Path: {}{}",
                path.namespace_code, path.name
            )
            .ok();
        }
    }

    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::{TemplateTerm, TripleTemplate, Txn};
    use fluree_db_core::{FlakeValue, LedgerSnapshot, MemoryStorage, Sid};
    use fluree_db_novelty::Novelty;
    use fluree_db_query::parse::{UnresolvedTerm, UnresolvedTriplePattern};

    /// Helper to create an UnresolvedPattern::Triple for WHERE clauses in tests
    fn where_triple(s: UnresolvedTerm, p: &str, o: UnresolvedTerm) -> UnresolvedPattern {
        UnresolvedPattern::Triple(UnresolvedTriplePattern::new(s, UnresolvedTerm::iri(p), o))
    }

    #[test]
    fn column_needs_materialization_detects_each_encoded_variant() {
        // Already-concrete bindings — must NOT trigger rewrite.
        let concrete = vec![
            Binding::Sid(Sid::new(1, "a")),
            Binding::Unbound,
            Binding::Poisoned,
        ];
        assert!(!column_needs_materialization(&concrete));

        // Each Encoded* variant must trigger rewrite individually.
        assert!(column_needs_materialization(&[Binding::EncodedSid {
            s_id: 7
        }]));
        assert!(column_needs_materialization(&[Binding::EncodedPid {
            p_id: 3
        }]));
        assert!(column_needs_materialization(&[Binding::EncodedLit {
            o_kind: 0,
            o_key: 0,
            p_id: 0,
            dt_id: 0,
            lang_id: 0,
            i_val: 0,
            t: 0,
        }]));

        // A column with a single encoded entry among many concrete entries
        // must still trigger — early-exit on first hit.
        let mut mixed = vec![Binding::Sid(Sid::new(1, "a")); 8];
        mixed.push(Binding::EncodedSid { s_id: 1 });
        mixed.extend(std::iter::repeat_n(Binding::Unbound, 4));
        assert!(column_needs_materialization(&mixed));
    }

    #[tokio::test]
    async fn test_stage_simple_insert() {
        let db = LedgerSnapshot::genesis("test:main");
        let novelty = Novelty::new(0);
        let ledger = LedgerState::new(db, novelty);

        // Create a simple insert transaction
        let txn = Txn::insert().with_insert(TripleTemplate::new(
            TemplateTerm::Sid(Sid::new(1, "ex:alice")),
            TemplateTerm::Sid(Sid::new(1, "ex:name")),
            TemplateTerm::Value(FlakeValue::String("Alice".to_string())),
        ));

        let ns_registry = NamespaceRegistry::from_db(&ledger.snapshot);
        let (view, _ns_registry) = stage(ledger, txn, ns_registry, StageOptions::default())
            .await
            .unwrap();

        assert_eq!(view.staged_len(), 1);
    }

    #[tokio::test]
    async fn test_stage_insert_multiple_triples() {
        let db = LedgerSnapshot::genesis("test:main");
        let novelty = Novelty::new(0);
        let ledger = LedgerState::new(db, novelty);

        // Insert multiple triples
        let txn = Txn::insert()
            .with_insert(TripleTemplate::new(
                TemplateTerm::Sid(Sid::new(1, "ex:alice")),
                TemplateTerm::Sid(Sid::new(1, "ex:name")),
                TemplateTerm::Value(FlakeValue::String("Alice".to_string())),
            ))
            .with_insert(TripleTemplate::new(
                TemplateTerm::Sid(Sid::new(1, "ex:alice")),
                TemplateTerm::Sid(Sid::new(1, "ex:age")),
                TemplateTerm::Value(FlakeValue::Long(30)),
            ));

        let ns_registry = NamespaceRegistry::from_db(&ledger.snapshot);
        let (view, _) = stage(ledger, txn, ns_registry, StageOptions::default())
            .await
            .unwrap();

        assert_eq!(view.staged_len(), 2);
    }

    #[tokio::test]
    async fn test_stage_with_blank_nodes() {
        let db = LedgerSnapshot::genesis("test:main");
        let novelty = Novelty::new(0);
        let ledger = LedgerState::new(db, novelty);

        // Insert with blank node
        let txn = Txn::insert().with_insert(TripleTemplate::new(
            TemplateTerm::BlankNode("_:b1".to_string()),
            TemplateTerm::Sid(Sid::new(1, "ex:name")),
            TemplateTerm::Value(FlakeValue::String("Anonymous".to_string())),
        ));

        let ns_registry = NamespaceRegistry::from_db(&ledger.snapshot);
        let (view, ns_registry) = stage(ledger, txn, ns_registry, StageOptions::default())
            .await
            .unwrap();

        assert_eq!(view.staged_len(), 1);
        // Blank nodes use the predefined _: prefix (BLANK_NODE code), no new namespace allocation needed
        assert!(ns_registry.has_prefix("_:"));
    }

    #[tokio::test]
    async fn test_stage_backpressure_at_max() {
        use fluree_db_core::Flake;

        let db = LedgerSnapshot::genesis("test:main");

        // Create novelty that's at max size
        let mut novelty = Novelty::new(0);
        // Add a lot of flakes to exceed the limit
        for i in 0..1000 {
            let flake = Flake::new(
                Sid::new(1, format!("s{i}")),
                Sid::new(1, "p"),
                FlakeValue::Long(i),
                Sid::new(2, "long"),
                1,
                true,
                None,
            );
            novelty
                .apply_commit(vec![flake], 1, &HashMap::new())
                .unwrap();
        }

        let ledger = LedgerState::new(db, novelty);

        // Use a very small config to trigger backpressure
        let config = IndexConfig {
            reindex_min_bytes: 100,
            reindex_max_bytes: 500, // Small limit
        };

        let txn = Txn::insert().with_insert(TripleTemplate::new(
            TemplateTerm::Sid(Sid::new(1, "ex:alice")),
            TemplateTerm::Sid(Sid::new(1, "ex:name")),
            TemplateTerm::Value(FlakeValue::String("Alice".to_string())),
        ));

        // Stage should fail with NoveltyAtMax
        let ns_registry = NamespaceRegistry::from_db(&ledger.snapshot);
        let options = StageOptions::new().with_index_config(&config);
        let result = stage(ledger, txn, ns_registry, options).await;
        assert!(matches!(result, Err(TransactError::NoveltyAtMax)));
    }

    #[tokio::test]
    async fn test_insert_with_blank_node_always_succeeds() {
        // Blank nodes are always new, so insert should succeed even if
        // the blank node was used before (it gets a new skolemized ID)
        let db = LedgerSnapshot::genesis("test:main");
        let novelty = Novelty::new(0);
        let ledger = LedgerState::new(db, novelty);

        let txn = Txn::insert().with_insert(TripleTemplate::new(
            TemplateTerm::BlankNode("_:b1".to_string()),
            TemplateTerm::Sid(Sid::new(1, "ex:name")),
            TemplateTerm::Value(FlakeValue::String("Test".to_string())),
        ));

        // Should succeed - blank nodes don't trigger existence check
        let ns_registry = NamespaceRegistry::from_db(&ledger.snapshot);
        let result = stage(ledger, txn, ns_registry, StageOptions::default()).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_upsert_replaces_existing_values() {
        use crate::commit::{commit, CommitOpts};
        use fluree_db_core::content_store_for;
        use fluree_db_nameservice::memory::MemoryNameService;

        let storage = MemoryStorage::new();
        let db = LedgerSnapshot::genesis("test:main");
        let novelty = Novelty::new(0);
        let ledger = LedgerState::new(db, novelty);

        let nameservice = MemoryNameService::new();
        let config = IndexConfig::default();
        let cs = content_store_for(storage.clone(), "test:main");

        // First: insert ex:alice with name="Alice"
        let txn1 = Txn::insert().with_insert(TripleTemplate::new(
            TemplateTerm::Sid(Sid::new(1, "ex:alice")),
            TemplateTerm::Sid(Sid::new(1, "ex:name")),
            TemplateTerm::Value(FlakeValue::String("Alice".to_string())),
        ));

        let ns_registry = NamespaceRegistry::from_db(&ledger.snapshot);
        let (view1, ns_registry1) = stage(ledger, txn1, ns_registry, StageOptions::default())
            .await
            .unwrap();
        let (_receipt, state1) = commit(
            view1,
            ns_registry1,
            &cs,
            &nameservice,
            &config,
            CommitOpts::default(),
        )
        .await
        .unwrap();

        // Now: upsert ex:alice with name="Alicia" (should replace)
        let txn2 = Txn::upsert().with_insert(TripleTemplate::new(
            TemplateTerm::Sid(Sid::new(1, "ex:alice")),
            TemplateTerm::Sid(Sid::new(1, "ex:name")),
            TemplateTerm::Value(FlakeValue::String("Alicia".to_string())),
        ));

        let ns_registry2 = NamespaceRegistry::from_db(&state1.snapshot);
        let (view2, _ns_registry2) = stage(state1, txn2, ns_registry2, StageOptions::default())
            .await
            .unwrap();

        // Check that we have both a retraction and an assertion
        let (_base, staged) = view2.into_parts();

        // Should have 2 flakes: one retraction for "Alice", one assertion for "Alicia"
        assert_eq!(staged.len(), 2);

        // Find retraction
        let retraction = staged
            .iter()
            .find(|f| !f.op)
            .expect("should have retraction");
        assert_eq!(retraction.s.name.as_ref(), "ex:alice");
        assert_eq!(retraction.p.name.as_ref(), "ex:name");
        assert_eq!(retraction.o, FlakeValue::String("Alice".to_string()));

        // Find assertion
        let assertion = staged.iter().find(|f| f.op).expect("should have assertion");
        assert_eq!(assertion.s.name.as_ref(), "ex:alice");
        assert_eq!(assertion.p.name.as_ref(), "ex:name");
        assert_eq!(assertion.o, FlakeValue::String("Alicia".to_string()));
    }

    #[tokio::test]
    async fn test_upsert_on_nonexistent_subject() {
        // Upsert on a subject that doesn't exist should just insert
        let db = LedgerSnapshot::genesis("test:main");
        let novelty = Novelty::new(0);
        let ledger = LedgerState::new(db, novelty);

        let txn = Txn::upsert().with_insert(TripleTemplate::new(
            TemplateTerm::Sid(Sid::new(1, "ex:alice")),
            TemplateTerm::Sid(Sid::new(1, "ex:name")),
            TemplateTerm::Value(FlakeValue::String("Alice".to_string())),
        ));

        let ns_registry = NamespaceRegistry::from_db(&ledger.snapshot);
        let (view, _) = stage(ledger, txn, ns_registry, StageOptions::default())
            .await
            .unwrap();

        // Should have just one assertion (no retraction since nothing existed)
        assert_eq!(view.staged_len(), 1);
        let (_base, staged) = view.into_parts();
        assert!(staged[0].op); // assertion
    }

    #[tokio::test]
    async fn test_where_uses_ledger_t_not_db_t() {
        // Test that WHERE patterns see data in novelty (committed but not indexed),
        // not just data in the indexed db. This is the "time boundary" correctness test.
        use crate::commit::{commit, CommitOpts};
        use fluree_db_core::content_store_for;
        use fluree_db_nameservice::memory::MemoryNameService;

        let storage = MemoryStorage::new();
        let db = LedgerSnapshot::genesis("test:main");
        let novelty = Novelty::new(0);
        let ledger = LedgerState::new(db, novelty);

        let nameservice = MemoryNameService::new();
        let config = IndexConfig::default();
        let cs = content_store_for(storage.clone(), "test:main");

        // Commit 1: Insert schema:alice with schema:name="Alice"
        // Do NOT rely on pre-registered SCHEMA_ORG codes — this build intentionally keeps
        // the default namespace table minimal. Allocate via NamespaceRegistry.
        let mut ns_registry = NamespaceRegistry::from_db(&ledger.snapshot);
        let schema_alice = ns_registry.sid_for_iri("http://schema.org/alice");
        let schema_name = ns_registry.sid_for_iri("http://schema.org/name");
        let txn1 = Txn::insert().with_insert(TripleTemplate::new(
            TemplateTerm::Sid(schema_alice.clone()),
            TemplateTerm::Sid(schema_name.clone()),
            TemplateTerm::Value(FlakeValue::String("Alice".to_string())),
        ));

        let (view1, ns1) = stage(ledger, txn1, ns_registry, StageOptions::default())
            .await
            .unwrap();
        let (_r1, state1) = commit(
            view1,
            ns1,
            &cs,
            &nameservice,
            &config,
            CommitOpts::default(),
        )
        .await
        .unwrap();

        // state1 now has t=1 with data in NOVELTY (not indexed)
        assert_eq!(state1.t(), 1);
        // Novelty includes 1 txn flake + commit metadata flakes
        assert!(
            !state1.novelty.is_empty(),
            "novelty should have at least 1 transaction flake (Alice's name)"
        );

        // Commit 2: Update with WHERE pattern that should match data in novelty
        // This UPDATE should find schema:alice's name (in novelty) and change it
        let mut vars = VarRegistry::new();
        let name_var = vars.get_or_insert("?name");

        // WHERE pattern uses UnresolvedPattern with string IRIs.
        // The variable "?name" will be assigned the same VarId during lowering
        // as was registered for DELETE/INSERT templates.
        let txn2 = Txn::update()
            .with_where(where_triple(
                UnresolvedTerm::iri("http://schema.org/alice"),
                "http://schema.org/name",
                UnresolvedTerm::var("?name"),
            ))
            .with_delete(TripleTemplate::new(
                TemplateTerm::Sid(schema_alice.clone()),
                TemplateTerm::Sid(schema_name.clone()),
                TemplateTerm::Var(name_var),
            ))
            .with_insert(TripleTemplate::new(
                TemplateTerm::Sid(schema_alice),
                TemplateTerm::Sid(schema_name),
                TemplateTerm::Value(FlakeValue::String("Alicia".to_string())),
            ))
            .with_vars(vars);

        let mut ns_registry2 = NamespaceRegistry::from_db(&state1.snapshot);
        // Ensure schema.org prefix is present in the registry used for lowering.
        // (Should already be in LedgerSnapshot.namespace_codes via commit delta, but this makes the test robust.)
        let _ = ns_registry2.sid_for_iri("http://schema.org/alice");
        let _ = ns_registry2.sid_for_iri("http://schema.org/name");
        let (view2, _ns2) = stage(state1, txn2, ns_registry2, StageOptions::default())
            .await
            .unwrap();

        // The WHERE should have found "Alice" (in novelty), so we should have:
        // - A retraction for "Alice"
        // - An assertion for "Alicia"
        let (_base2, staged2) = view2.into_parts();
        assert_eq!(staged2.len(), 2);

        // Verify we got the retraction (proving WHERE saw the novelty data)
        let retraction = staged2.iter().find(|f| !f.op);
        assert!(
            retraction.is_some(),
            "WHERE should have found data in novelty"
        );
        assert_eq!(
            retraction.unwrap().o,
            FlakeValue::String("Alice".to_string())
        );
    }

    #[tokio::test]
    async fn test_multi_pattern_where_join() {
        // Test that multiple WHERE patterns are joined correctly.
        // This verifies that execute_where_with_overlay_at handles joins.
        use crate::commit::{commit, CommitOpts};
        use fluree_db_core::content_store_for;
        use fluree_db_nameservice::memory::MemoryNameService;

        let storage = MemoryStorage::new();
        let db = LedgerSnapshot::genesis("test:main");
        let novelty = Novelty::new(0);
        let ledger = LedgerState::new(db, novelty);

        let nameservice = MemoryNameService::new();
        let config = IndexConfig::default();
        let cs = content_store_for(storage.clone(), "test:main");

        // Commit 1: Insert schema:alice with name="Alice" and age=30
        let mut ns_registry = NamespaceRegistry::from_db(&ledger.snapshot);
        let schema_alice = ns_registry.sid_for_iri("http://schema.org/alice");
        let schema_name = ns_registry.sid_for_iri("http://schema.org/name");
        let schema_age = ns_registry.sid_for_iri("http://schema.org/age");
        let txn1 = Txn::insert()
            .with_insert(TripleTemplate::new(
                TemplateTerm::Sid(schema_alice.clone()),
                TemplateTerm::Sid(schema_name.clone()),
                TemplateTerm::Value(FlakeValue::String("Alice".to_string())),
            ))
            .with_insert(TripleTemplate::new(
                TemplateTerm::Sid(schema_alice.clone()),
                TemplateTerm::Sid(schema_age.clone()),
                TemplateTerm::Value(FlakeValue::Long(30)),
            ));

        let (view1, ns1) = stage(ledger, txn1, ns_registry, StageOptions::default())
            .await
            .unwrap();
        let (_r1, state1) = commit(
            view1,
            ns1,
            &cs,
            &nameservice,
            &config,
            CommitOpts::default(),
        )
        .await
        .unwrap();

        // Commit 2: Also insert schema:bob with only a name (no age)
        let mut ns_registry2 = NamespaceRegistry::from_db(&state1.snapshot);
        let schema_bob = ns_registry2.sid_for_iri("http://schema.org/bob");
        let schema_name2 = ns_registry2.sid_for_iri("http://schema.org/name");
        let txn2 = Txn::insert().with_insert(TripleTemplate::new(
            TemplateTerm::Sid(schema_bob.clone()),
            TemplateTerm::Sid(schema_name2.clone()),
            TemplateTerm::Value(FlakeValue::String("Bob".to_string())),
        ));

        let (view2, ns2) = stage(state1, txn2, ns_registry2, StageOptions::default())
            .await
            .unwrap();
        let (_r2, state2) = commit(
            view2,
            ns2,
            &cs,
            &nameservice,
            &config,
            CommitOpts::default(),
        )
        .await
        .unwrap();

        // Now: Multi-pattern UPDATE
        // WHERE { ?s schema:name ?name . ?s schema:age ?age }  <- requires BOTH patterns to match
        // DELETE { ?s schema:age ?age }
        // INSERT { ?s schema:age 31 }
        //
        // This should ONLY match schema:alice (who has both name and age).
        // schema:bob should NOT match (has name but no age).

        let mut vars = VarRegistry::new();
        let s_var = vars.get_or_insert("?s");
        let _name_var = vars.get_or_insert("?name");
        let age_var = vars.get_or_insert("?age");

        // WHERE patterns use UnresolvedPattern with string IRIs and variable names
        let txn3 = Txn::update()
            .with_where(where_triple(
                UnresolvedTerm::var("?s"),
                "http://schema.org/name",
                UnresolvedTerm::var("?name"),
            ))
            .with_where(where_triple(
                UnresolvedTerm::var("?s"),
                "http://schema.org/age",
                UnresolvedTerm::var("?age"),
            ))
            .with_delete(TripleTemplate::new(
                TemplateTerm::Var(s_var),
                TemplateTerm::Sid(schema_age.clone()),
                TemplateTerm::Var(age_var),
            ))
            .with_insert(TripleTemplate::new(
                TemplateTerm::Var(s_var),
                TemplateTerm::Sid(schema_age),
                TemplateTerm::Value(FlakeValue::Long(31)),
            ))
            .with_vars(vars);

        let mut ns_registry3 = NamespaceRegistry::from_db(&state2.snapshot);
        // Ensure schema.org prefix exists for lowering WHERE IRIs.
        let _ = ns_registry3.sid_for_iri("http://schema.org/age");
        let _ = ns_registry3.sid_for_iri("http://schema.org/name");
        let (view3, _ns3) = stage(state2, txn3, ns_registry3, StageOptions::default())
            .await
            .unwrap();

        // Should have exactly 2 flakes:
        // - Retraction of schema:alice schema:age 30
        // - Assertion of schema:alice schema:age 31
        let (_base3, staged3) = view3.into_parts();
        assert_eq!(
            staged3.len(),
            2,
            "Should have exactly 2 flakes (1 retraction + 1 assertion)"
        );

        // Verify the retraction is for alice's old age
        let retraction = staged3
            .iter()
            .find(|f| !f.op)
            .expect("should have retraction");
        assert_eq!(retraction.s.name.as_ref(), "alice");
        assert_eq!(retraction.p.name.as_ref(), "age");
        assert_eq!(retraction.o, FlakeValue::Long(30));

        // Verify the assertion is for alice's new age
        let assertion = staged3
            .iter()
            .find(|f| f.op)
            .expect("should have assertion");
        assert_eq!(assertion.s.name.as_ref(), "alice");
        assert_eq!(assertion.p.name.as_ref(), "age");
        assert_eq!(assertion.o, FlakeValue::Long(31));
    }

    #[tokio::test]
    async fn test_values_seeding_insert() {
        // Test that VALUES can seed bindings for INSERT templates.
        // This supports transactions like:
        //   VALUES ?s ?name { (ex:alice "Alice") (ex:bob "Bob") }
        //   INSERT { ?s ex:name ?name }
        // Which should create two triples with different subjects and names.
        use crate::ir::InlineValues;

        let db = LedgerSnapshot::genesis("test:main");
        let novelty = Novelty::new(0);
        let ledger = LedgerState::new(db, novelty);

        // Create a transaction with VALUES seeding - using named subjects
        let mut vars = VarRegistry::new();
        let s_var = vars.get_or_insert("?s");
        let name_var = vars.get_or_insert("?name");

        let values = InlineValues::new(
            vec![s_var, name_var],
            vec![
                vec![
                    TemplateTerm::Sid(Sid::new(1, "ex:alice")),
                    TemplateTerm::Value(FlakeValue::String("Alice".to_string())),
                ],
                vec![
                    TemplateTerm::Sid(Sid::new(1, "ex:bob")),
                    TemplateTerm::Value(FlakeValue::String("Bob".to_string())),
                ],
            ],
        );

        let txn = Txn::insert()
            .with_insert(TripleTemplate::new(
                TemplateTerm::Var(s_var),
                TemplateTerm::Sid(Sid::new(1, "ex:name")),
                TemplateTerm::Var(name_var),
            ))
            .with_values(values)
            .with_vars(vars);

        let ns_registry = NamespaceRegistry::from_db(&ledger.snapshot);
        let (view, _) = stage(ledger, txn, ns_registry, StageOptions::default())
            .await
            .unwrap();

        // Should have 2 assertions (one for "Alice", one for "Bob")
        let (_base, staged) = view.into_parts();
        assert_eq!(staged.len(), 2, "Should have 2 flakes from VALUES seeding");

        // Both should be assertions
        assert!(
            staged.iter().all(|f| f.op),
            "All flakes should be assertions"
        );

        // Verify we got both names with correct subjects
        let alice_flake = staged.iter().find(|f| f.s.name.as_ref() == "ex:alice");
        let bob_flake = staged.iter().find(|f| f.s.name.as_ref() == "ex:bob");

        assert!(alice_flake.is_some(), "Should have alice flake");
        assert!(bob_flake.is_some(), "Should have bob flake");

        assert_eq!(
            alice_flake.unwrap().o,
            FlakeValue::String("Alice".to_string())
        );
        assert_eq!(bob_flake.unwrap().o, FlakeValue::String("Bob".to_string()));
    }

    #[tokio::test]
    async fn test_values_seeding_with_where_join() {
        // Test VALUES seeding combined with WHERE patterns.
        // This verifies that VALUES can constrain which subjects are matched.
        use crate::commit::{commit, CommitOpts};
        use crate::ir::InlineValues;
        use fluree_db_core::content_store_for;
        use fluree_db_nameservice::memory::MemoryNameService;

        let storage = MemoryStorage::new();
        let db = LedgerSnapshot::genesis("test:main");
        let novelty = Novelty::new(0);
        let ledger = LedgerState::new(db, novelty);

        let nameservice = MemoryNameService::new();
        let config = IndexConfig::default();
        let cs = content_store_for(storage.clone(), "test:main");

        // Insert data: alice has age 30, bob has age 25
        let mut ns_registry = NamespaceRegistry::from_db(&ledger.snapshot);
        let schema_alice = ns_registry.sid_for_iri("http://schema.org/alice");
        let schema_bob = ns_registry.sid_for_iri("http://schema.org/bob");
        let schema_age = ns_registry.sid_for_iri("http://schema.org/age");
        let txn1 = Txn::insert()
            .with_insert(TripleTemplate::new(
                TemplateTerm::Sid(schema_alice.clone()),
                TemplateTerm::Sid(schema_age.clone()),
                TemplateTerm::Value(FlakeValue::Long(30)),
            ))
            .with_insert(TripleTemplate::new(
                TemplateTerm::Sid(schema_bob.clone()),
                TemplateTerm::Sid(schema_age.clone()),
                TemplateTerm::Value(FlakeValue::Long(25)),
            ));

        let (view1, ns1) = stage(ledger, txn1, ns_registry, StageOptions::default())
            .await
            .unwrap();
        let (_r1, state1) = commit(
            view1,
            ns1,
            &cs,
            &nameservice,
            &config,
            CommitOpts::default(),
        )
        .await
        .unwrap();

        // Verify state after first commit
        assert_eq!(state1.t(), 1);
        // Novelty includes 2 txn flakes + commit metadata flakes
        assert!(
            state1.novelty.len() >= 2,
            "novelty should have at least 2 transaction flakes (alice and bob's ages)"
        );

        // Now: Update with VALUES constraining to only alice
        // VALUES ?s { schema:alice }
        // WHERE { ?s schema:age ?age }
        // DELETE { ?s schema:age ?age }
        // INSERT { ?s schema:age 35 }
        let mut vars = VarRegistry::new();
        let s_var = vars.get_or_insert("?s");
        let age_var = vars.get_or_insert("?age");

        let values = InlineValues::new(
            vec![s_var],
            vec![vec![TemplateTerm::Sid(schema_alice.clone())]],
        );

        // WHERE pattern uses UnresolvedPattern with string variable names
        let txn2 = Txn::update()
            .with_where(where_triple(
                UnresolvedTerm::var("?s"),
                "http://schema.org/age",
                UnresolvedTerm::var("?age"),
            ))
            .with_delete(TripleTemplate::new(
                TemplateTerm::Var(s_var),
                TemplateTerm::Sid(schema_age.clone()),
                TemplateTerm::Var(age_var),
            ))
            .with_insert(TripleTemplate::new(
                TemplateTerm::Var(s_var),
                TemplateTerm::Sid(schema_age),
                TemplateTerm::Value(FlakeValue::Long(35)),
            ))
            .with_values(values)
            .with_vars(vars);

        let mut ns_registry2 = NamespaceRegistry::from_db(&state1.snapshot);
        let _ = ns_registry2.sid_for_iri("http://schema.org/age");
        let result = stage(state1, txn2, ns_registry2, StageOptions::default()).await;

        // Check if stage succeeded
        let (view2, _ns2) = result.expect("stage should succeed");

        // Should have exactly 2 flakes (retraction + assertion for alice only)
        let (_base2, staged2) = view2.into_parts();
        assert_eq!(
            staged2.len(),
            2,
            "Should have 2 flakes (alice only, not bob)"
        );

        // Verify only alice's age was affected
        let retraction = staged2
            .iter()
            .find(|f| !f.op)
            .expect("should have retraction");
        assert_eq!(retraction.s.name.as_ref(), "alice");
        assert_eq!(retraction.o, FlakeValue::Long(30));

        let assertion = staged2
            .iter()
            .find(|f| f.op)
            .expect("should have assertion");
        assert_eq!(assertion.s.name.as_ref(), "alice");
        assert_eq!(assertion.o, FlakeValue::Long(35));
    }
}
