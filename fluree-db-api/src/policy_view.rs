//! Policy-wrapped view abstraction
//!
//! This module provides `PolicyWrappedView`, a first-class "policy-wrapped db" type
//! that preserves the legacy policy-wrap flow.
//!
//! # Design Goals
//!
//! - **Wrap-first model**: Create policy-wrapped views once, then query them
//! - **Cheap metadata**: Wrapper is purely metadata - no DB state copied
//! - **Per-graph policy**: Each graph in a dataset can have its own policy enforcer
//! - Matches the flow of `restrict-db` → `wrap-policy` → execute
//!
//! # Usage
//!
//! ```ignore
//! use fluree_db_api::{Fluree, PolicyWrappedView, GovernanceOptions};
//!
//! let ledger = fluree.ledger("mydb:main").await?;
//! let opts = GovernanceOptions { identity: Some("did:example:user".into()), ..Default::default() };
//!
//! // Wrap the ledger view with policy
//! let wrapped = fluree.wrap_policy_view(&ledger, &opts).await?;
//!
//! // Query the wrapped view
//! let results = fluree.query_wrapped(&wrapped, &query).await?;
//! ```

use crate::dataset::GovernanceOptions;
use crate::error::Result;
use crate::policy_builder;
use fluree_db_core::ledger_config::LedgerConfig;
use fluree_db_core::{LedgerSnapshot, OverlayProvider};
use fluree_db_ledger::{HistoricalLedgerView, LedgerState};
use fluree_db_novelty::Novelty;
use fluree_db_policy::PolicyContext;
use fluree_db_query::policy::QueryPolicyEnforcer;
use std::sync::Arc;

/// A policy-wrapped view of a ledger.
///
/// This is a lightweight wrapper that attaches policy context and enforcer
/// to a ledger view. The wrapper doesn't copy any DB state - it just holds
/// references to the underlying view plus policy metadata.
///
/// # Type Parameters
///
/// - `'a`: Lifetime of the underlying ledger/overlay references
///
/// # Variants
///
/// Use the appropriate constructor based on your view type:
/// - `from_ledger_state()` for `LedgerState`
/// - `from_historical()` for `HistoricalLedgerView`
pub struct PolicyWrappedView<'a> {
    /// Reference to the database snapshot
    pub snapshot: &'a LedgerSnapshot,
    /// Overlay provider (novelty layer)
    pub overlay: &'a dyn OverlayProvider,
    /// Target transaction time
    pub to_t: i64,
    /// The policy context (identity, restrictions, policy-values)
    pub policy: Arc<PolicyContext>,
    /// The policy enforcer for async f:query support
    pub enforcer: Arc<QueryPolicyEnforcer>,
}

impl<'a> PolicyWrappedView<'a> {
    /// Create a policy-wrapped view from components.
    ///
    /// This is the low-level constructor. Prefer using `wrap_policy_view()`
    /// which handles policy context creation from options.
    pub fn new(
        snapshot: &'a LedgerSnapshot,
        overlay: &'a dyn OverlayProvider,
        to_t: i64,
        policy: Arc<PolicyContext>,
    ) -> Self {
        let enforcer = Arc::new(QueryPolicyEnforcer::new(Arc::clone(&policy)));
        Self {
            snapshot,
            overlay,
            to_t,
            policy,
            enforcer,
        }
    }

    /// Check if this is a root/unrestricted policy (bypasses all checks).
    pub fn is_root(&self) -> bool {
        self.enforcer.is_root()
    }

    /// Get the underlying policy context.
    pub fn policy(&self) -> &PolicyContext {
        &self.policy
    }

    /// Get the policy enforcer for use in query execution.
    pub fn enforcer(&self) -> &Arc<QueryPolicyEnforcer> {
        &self.enforcer
    }
}

impl<'a> PolicyWrappedView<'a> {
    /// Create a policy-wrapped view from a `LedgerState`.
    pub fn from_ledger_state(ledger: &'a LedgerState, policy: Arc<PolicyContext>) -> Self {
        Self::new(
            &ledger.snapshot,
            ledger.novelty.as_ref(),
            ledger.t(),
            policy,
        )
    }

    /// Create a policy-wrapped view from a `HistoricalLedgerView`.
    ///
    /// Note: The view itself is used as the overlay provider.
    pub fn from_historical(view: &'a HistoricalLedgerView, policy: Arc<PolicyContext>) -> Self {
        Self::new(&view.snapshot, view, view.to_t(), policy)
    }
}

// ============================================================================
// Builder functions
// ============================================================================

/// Wrap a ledger state with policy based on query connection options.
///
/// This is the main entry point for creating policy-wrapped views.
///
/// # Arguments
///
/// * `ledger` - The ledger state to wrap
/// * `opts` - Query connection options containing policy inputs
///
/// # Returns
///
/// A `PolicyWrappedView` if policy inputs are present, or an error if
/// policy building fails.
///
/// # Example
///
/// ```ignore
/// let opts = GovernanceOptions {
///     identity: Some("did:example:user".to_string()),
///     ..Default::default()
/// };
/// let wrapped = wrap_policy_view(&ledger, &opts).await?;
/// ```
pub async fn wrap_policy_view<'a>(
    ledger: &'a LedgerState,
    opts: &GovernanceOptions,
) -> Result<PolicyWrappedView<'a>> {
    let policy_graphs =
        resolve_policy_graphs_from_config(&ledger.snapshot, ledger.novelty.as_ref(), ledger.t())
            .await?;

    let policy_ctx = policy_builder::build_policy_context_from_opts(
        &ledger.snapshot,
        ledger.novelty.as_ref(),
        Some(ledger.novelty.as_ref()),
        ledger.t(),
        opts,
        &policy_graphs,
    )
    .await?;

    Ok(PolicyWrappedView::from_ledger_state(
        ledger,
        Arc::new(policy_ctx),
    ))
}

/// Wrap a historical ledger view with policy based on query connection options.
///
/// Similar to `wrap_policy_view` but for historical views.
pub async fn wrap_policy_view_historical<'a>(
    view: &'a HistoricalLedgerView,
    opts: &GovernanceOptions,
) -> Result<PolicyWrappedView<'a>> {
    let policy_graphs =
        resolve_policy_graphs_from_config(&view.snapshot, view, view.to_t()).await?;

    // Extract novelty from the view for stats computation (needed for f:onClass)
    let novelty_for_stats: Option<&Novelty> = view.overlay().map(std::convert::AsRef::as_ref);
    let policy_ctx = policy_builder::build_policy_context_from_opts(
        &view.snapshot,
        view,
        novelty_for_stats,
        view.to_t(),
        opts,
        &policy_graphs,
    )
    .await?;

    Ok(PolicyWrappedView::from_historical(
        view,
        Arc::new(policy_ctx),
    ))
}

/// Build a policy context from options without wrapping a view.
///
/// Reads the config graph to resolve `f:policySource` (if configured) so that
/// policy rules stored in named graphs are loaded correctly. Call sites that
/// don't go through `wrap_policy` / `GraphDb` (e.g., server transact handlers,
/// CLI insert) use this function and still get config-driven policy graphs.
///
/// Same-ledger only: a cross-ledger `f:policySource` (with `f:ledger`) fails
/// closed here. Callers with a `Fluree` handle should use
/// [`build_transact_policy_context`], which also merges config policy
/// defaults and resolves cross-ledger sources.
///
/// # Arguments
///
/// * `snapshot` - The database snapshot to query against
/// * `overlay` - Overlay provider for query execution
/// * `novelty_for_stats` - Optional novelty for computing current stats (needed for f:onClass)
/// * `to_t` - Time bound for queries
/// * `opts` - Query connection options with policy configuration
pub async fn build_policy_context(
    snapshot: &LedgerSnapshot,
    overlay: &dyn OverlayProvider,
    novelty_for_stats: Option<&Novelty>,
    to_t: i64,
    opts: &GovernanceOptions,
) -> Result<PolicyContext> {
    let policy_graphs = resolve_policy_graphs_from_config(snapshot, overlay, to_t).await?;

    policy_builder::build_policy_context_from_opts(
        snapshot,
        overlay,
        novelty_for_stats,
        to_t,
        opts,
        &policy_graphs,
    )
    .await
}

/// Resolve a cross-ledger `f:policySource` into policy restrictions
/// interned against the data ledger's term space.
///
/// Shared between `wrap_policy` (read path) and
/// [`build_transact_policy_context`] (write path) so both sides apply
/// identical semantics: the class-filter chain, the identity contract, and
/// the `ArtifactKind::PolicyRules` dispatch.
///
/// The filter contract: rules materialized from M are intersected (exact
/// IRI) against the first non-empty entry in the chain
///
///   `effective_opts.policy_class` → `config_policy_class` →
///   `{f:AccessPolicy}` (anonymous requests only).
///
/// `config_policy_class` is passed separately because `merge_policy_opts`
/// returns the request opts unchanged when the request carries any policy
/// input and override is permitted — an identity-only request would
/// otherwise never see the config's `f:policyClass`.
///
/// The identity contract: an identity on the request **binds `?$identity`
/// against D and never selects rules from M** — rule selection under
/// cross-ledger is exclusively the class filter (M contributes rules, D
/// contributes identity binding). Because the identity can't select rules,
/// an identity-carrying request with no policy class anywhere fails closed
/// rather than silently falling back to the `{f:AccessPolicy}` default: the
/// operator must name which classes govern.
///
/// `f:AccessPolicy` is the canonical / baseline policy class — declaring
/// `f:policySource` cross-ledger pulls those rules in automatically for
/// anonymous requests; custom-typed rules require an explicit
/// `f:policyClass` in D's config to be enforced. This is the safer default
/// than "load every structurally-policy-looking subject from M," which
/// would silently include rules the operator never opted into.
pub(crate) async fn resolve_cross_ledger_policy_restrictions(
    snapshot: &LedgerSnapshot,
    effective_opts: &GovernanceOptions,
    config_policy_class: Option<&[String]>,
    source: &fluree_db_core::ledger_config::GraphSourceRef,
    ctx: &mut crate::cross_ledger::ResolveCtx<'_>,
) -> Result<Vec<fluree_db_policy::PolicyRestriction>> {
    const DEFAULT_POLICY_CLASS_IRI: &str = fluree_vocab::policy_iris::ACCESS_POLICY;
    let filter: std::collections::HashSet<String> = if let Some(classes) = effective_opts
        .policy_class
        .as_ref()
        .filter(|v| !v.is_empty())
    {
        classes.iter().cloned().collect()
    } else if let Some(classes) = config_policy_class.filter(|v| !v.is_empty()) {
        classes.iter().cloned().collect()
    } else if effective_opts.identity.is_none() {
        [DEFAULT_POLICY_CLASS_IRI.to_string()].into_iter().collect()
    } else {
        return Err(crate::error::ApiError::config(
            "cross-ledger f:policySource with an identity requires an explicit \
             f:policyClass (on the request or in the ledger config) to select \
             which of the model ledger's rules apply; the identity only binds \
             ?$identity and never selects rules",
        ));
    };

    let resolved = crate::cross_ledger::resolve_graph_ref(
        source,
        crate::cross_ledger::ArtifactKind::PolicyRules,
        ctx,
    )
    .await?;
    let crate::cross_ledger::GovernanceArtifact::PolicyRules(wire) = &resolved.artifact else {
        // resolve_graph_ref dispatches on ArtifactKind, so requesting
        // PolicyRules must yield PolicyRules. Surfacing this as
        // TranslationFailed rather than panicking keeps the failure path
        // uniform for operators reading the response body.
        return Err(crate::error::ApiError::CrossLedger(
            crate::cross_ledger::CrossLedgerError::TranslationFailed {
                ledger_id: resolved.model_ledger_id.clone(),
                graph_iri: resolved.graph_iri.clone(),
                detail: "resolver returned a non-PolicyRules artifact for an \
                        ArtifactKind::PolicyRules request; this is a bug in \
                        the resolver dispatch"
                    .into(),
            },
        ));
    };

    // Wire artifacts can carry f:sparql policy queries; make sure the
    // executor's SPARQL hooks exist before these restrictions are evaluated.
    crate::sparql_lang::ensure_sparql_support_registered();
    crate::cypher_lang::ensure_cypher_support_registered();

    fluree_db_policy::wire_to_restrictions(wire, |iri| snapshot.encode_iri(iri), Some(&filter))
        .map_err(crate::error::ApiError::from)
}

/// Build the policy context for a write (or other non-view enforcement
/// point), honoring the ledger's `#config` graph the same way `wrap_policy`
/// does on the read path.
///
/// This is the write-side counterpart of `Fluree::wrap_policy`:
///
/// 1. Resolves the ledger config at `to_t` and merges config policy defaults
///    (`f:policyClass`, `f:defaultAllow`, override control) into `opts` via
///    `merge_policy_opts` — so config-declared policy governs writes even
///    when the request itself carries no policy inputs.
/// 2. A cross-ledger `f:policySource` (with `f:ledger`) is resolved live
///    against the model ledger (`ArtifactKind::PolicyRules`, latest committed
///    M) and its restrictions are interned into this ledger's term space.
/// 3. A same-ledger `f:policySource` resolves to concrete graph IDs via
///    `resolve_policy_source_g_ids` (fail-closed on unknown selectors).
///
/// Returns `Ok(None)` when neither the request nor the config supplies any
/// policy input — the transaction runs under root, matching the previous
/// behavior for unconfigured ledgers. A cross-ledger source always builds a
/// context (mirroring the read path, where the model ledger's rules apply
/// regardless of request inputs).
pub async fn build_transact_policy_context(
    fluree: &crate::Fluree,
    snapshot: &LedgerSnapshot,
    overlay: &dyn OverlayProvider,
    novelty_for_stats: Option<&Novelty>,
    to_t: i64,
    opts: &GovernanceOptions,
) -> Result<Option<PolicyContext>> {
    let raw_config =
        resolve_ledger_config_cached(fluree, snapshot, overlay, novelty_for_stats, to_t).await?;
    let resolved = raw_config
        .as_deref()
        .map(|c| crate::config_resolver::resolve_effective_config(c, None));

    let effective_opts = match &resolved {
        Some(r) => crate::config_resolver::merge_policy_opts(r, opts, None),
        None => opts.clone(),
    };

    let source = resolved
        .as_ref()
        .and_then(|r| r.policy.as_ref())
        .and_then(|p| p.policy_source.as_ref());

    // Cross-ledger ontology (f:schemaSource with f:ledger): resolve once so
    // f:onClass / f:onProperty expansion sees the model ledger's hierarchy.
    let cross_ledger_schema = match resolved
        .as_ref()
        .and_then(|r| r.reasoning.as_ref())
        .filter(|r| r.schema_source.as_ref().is_some_and(|s| s.ledger.is_some()))
    {
        Some(reasoning) => {
            let ledger_id: String = snapshot.ledger_id.to_string();
            let mut schema_ctx = crate::cross_ledger::ResolveCtx::new(&ledger_id, fluree);
            crate::cross_ledger::resolve_schema_closure_bundle(reasoning, snapshot, &mut schema_ctx)
                .await
                .map_err(|e| {
                    crate::error::ApiError::config(format!(
                        "cross-ledger f:schemaSource resolution failed: {e}"
                    ))
                })?
        }
        None => None,
    };

    if let Some(source) = source.filter(|s| s.ledger.is_some()) {
        let ledger_id: String = snapshot.ledger_id.to_string();
        let mut ctx = crate::cross_ledger::ResolveCtx::new(&ledger_id, fluree);
        let config_policy_class = resolved
            .as_ref()
            .and_then(|r| r.policy.as_ref())
            .and_then(|p| p.policy_class.as_deref());
        let restrictions = resolve_cross_ledger_policy_restrictions(
            snapshot,
            &effective_opts,
            config_policy_class,
            source,
            &mut ctx,
        )
        .await?;
        let policy_ctx = policy_builder::build_policy_context_from_opts_with_cross_ledger(
            snapshot,
            overlay,
            novelty_for_stats,
            to_t,
            &effective_opts,
            // Graph set for the identity subject-existence probe that binds
            // ?$identity (rule selection is the cross-ledger wire, not these
            // graphs). Under cross-ledger policy, identity records must live
            // in D's default graph — the probe searches [0] only.
            &[0],
            restrictions,
            cross_ledger_schema.clone(),
        )
        .await?;
        return Ok(Some(policy_ctx));
    }

    // Resolve (and validate) the same-ledger selector first, matching the read
    // path's fail-closed-on-unknown-selector contract (fluree_ext.rs resolves
    // unconditionally). Applying the no-inputs shortcut before this would let an
    // invalid config `f:policySource` silently run as root on writes while reads
    // fail closed — the read/write divergence this path exists to eliminate.
    let policy_graphs = policy_builder::resolve_policy_source_g_ids(source, snapshot)?;

    if !effective_opts.has_any_policy_inputs() {
        return Ok(None);
    }

    let policy_ctx = policy_builder::build_policy_context_from_opts_with_schema(
        snapshot,
        overlay,
        novelty_for_stats,
        to_t,
        &effective_opts,
        &policy_graphs,
        cross_ledger_schema,
    )
    .await?;
    Ok(Some(policy_ctx))
}

/// Resolve the raw ledger config for the write path, memoized per-ledger by the
/// novelty config-write marker (`Novelty::config_write_t`).
///
/// Reading the config graph on every write — including writes that carry no
/// policy inputs — is feature-necessary (you must read config to learn
/// `f:policySource` / config policy defaults), but for a configured ledger under
/// sustained writes it re-resolves state that has not changed. The marker
/// advances iff a commit touches the config graph, so a configured-but-static
/// ledger resolves config once per config change instead of once per write (and
/// once per stage/commit retry — retries triggered by unrelated data conflicts
/// leave the marker untouched and hit the cache).
///
/// Fail-safe by construction: the cache is consulted only at head, with a
/// readable marker and a loaded handle. Any deviation — time-travel (`to_t`
/// below head), a non-`Novelty` overlay, or no loaded handle — resolves fresh
/// against the passed snapshot/overlay. A cache miss or a marker reset (e.g.
/// after reindex) costs an extra resolve, never a stale (fail-open) read.
async fn resolve_ledger_config_cached(
    fluree: &crate::Fluree,
    snapshot: &LedgerSnapshot,
    overlay: &dyn OverlayProvider,
    novelty_for_stats: Option<&Novelty>,
    to_t: i64,
) -> Result<Option<Arc<LedgerConfig>>> {
    // The invalidation marker and head detection both come from the current
    // novelty overlay. Prefer the explicit stats handle; fall back to the
    // overlay when it is itself a `Novelty`.
    let novelty = novelty_for_stats.or_else(|| overlay.as_any().downcast_ref::<Novelty>());

    // Only cacheable at head (`to_t` == ledger head) with a readable marker.
    let cache_key = novelty.and_then(|nov| {
        let head_t = snapshot.t.max(nov.t);
        (to_t == head_t).then_some(nov.config_write_t)
    });

    if let Some(key) = cache_key {
        if let Some(mgr) = fluree.ledger_manager() {
            if let Some(handle) = mgr.get_loaded_handle(&snapshot.ledger_id).await {
                if let Some(hit) = handle.config_cache_get(key).await {
                    return Ok(hit);
                }
                let resolved = resolve_ledger_config_raw(snapshot, overlay, to_t).await?;
                handle.config_cache_put(key, resolved.clone()).await;
                return Ok(resolved);
            }
        }
    }

    resolve_ledger_config_raw(snapshot, overlay, to_t).await
}

/// Uncached resolve, `Arc`-wrapping the result and mapping the error into the
/// config-failure shape `build_transact_policy_context` reports.
async fn resolve_ledger_config_raw(
    snapshot: &LedgerSnapshot,
    overlay: &dyn OverlayProvider,
    to_t: i64,
) -> Result<Option<Arc<LedgerConfig>>> {
    match crate::config_resolver::resolve_ledger_config(snapshot, overlay, to_t).await {
        Ok(opt) => Ok(opt.map(Arc::new)),
        Err(e) => Err(crate::error::ApiError::config(format!(
            "Failed to load ledger config while resolving transaction policy: {e}"
        ))),
    }
}

/// Wrap a ledger with identity-based policy via `f:policyClass` lookup.
///
/// Convenience wrapper for identity-based policy wrapping.
/// Queries for policies via the identity's `f:policyClass` property.
///
/// # Arguments
///
/// * `ledger` - The ledger state to wrap
/// * `identity_iri` - IRI of the identity subject (will query `f:policyClass`)
/// * `default_allow` - Whether to allow when no policies match (default: false)
///
/// # Example
///
/// ```ignore
/// let wrapped = wrap_identity_policy_view(&ledger, "did:example:user", false).await?;
/// ```
pub async fn wrap_identity_policy_view<'a>(
    ledger: &'a LedgerState,
    identity_iri: &str,
    default_allow: bool,
) -> Result<PolicyWrappedView<'a>> {
    let opts = GovernanceOptions {
        identity: Some(identity_iri.to_string()),
        default_allow,
        ..Default::default()
    };
    wrap_policy_view(ledger, &opts).await
}

/// Read the config graph and resolve `f:policySource` to graph IDs.
///
/// Fails closed: if a config exists but cannot be loaded, or if a configured
/// `f:policySource` cannot be resolved (unknown graph selector, unsupported
/// cross-ledger / temporal / trust fields), the error is propagated instead
/// of silently falling back to the default graph.
///
/// Returns `[0]` (default graph) only when no config has been written to the
/// ledger yet (`Ok(None)`) or no `f:policySource` is configured — in both
/// cases the caller's policy rules, if any, live in the default graph.
pub(crate) async fn resolve_policy_graphs_from_config(
    snapshot: &LedgerSnapshot,
    overlay: &dyn OverlayProvider,
    to_t: i64,
) -> Result<Vec<fluree_db_core::GraphId>> {
    let config = match crate::config_resolver::resolve_ledger_config(snapshot, overlay, to_t).await
    {
        Ok(Some(c)) => c,
        Ok(None) => return Ok(vec![0]),
        Err(e) => {
            return Err(crate::error::ApiError::config(format!(
                "Failed to load ledger config while resolving f:policySource: {e}"
            )));
        }
    };
    let resolved = crate::config_resolver::resolve_effective_config(&config, None);
    let source = resolved
        .policy
        .as_ref()
        .and_then(|p| p.policy_source.as_ref());
    policy_builder::resolve_policy_source_g_ids(source, snapshot)
}
