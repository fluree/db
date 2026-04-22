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
//! use fluree_db_api::{Fluree, PolicyWrappedView, QueryConnectionOptions};
//!
//! let ledger = fluree.ledger("mydb:main").await?;
//! let opts = QueryConnectionOptions { identity: Some("did:example:user".into()), ..Default::default() };
//!
//! // Wrap the ledger view with policy
//! let wrapped = fluree.wrap_policy_view(&ledger, &opts).await?;
//!
//! // Query the wrapped view
//! let results = fluree.query_wrapped(&wrapped, &query).await?;
//! ```

use crate::dataset::QueryConnectionOptions;
use crate::error::Result;
use crate::policy_builder;
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
/// let opts = QueryConnectionOptions {
///     identity: Some("did:example:user".to_string()),
///     ..Default::default()
/// };
/// let wrapped = wrap_policy_view(&ledger, &opts).await?;
/// ```
pub async fn wrap_policy_view<'a>(
    ledger: &'a LedgerState,
    opts: &QueryConnectionOptions,
) -> Result<PolicyWrappedView<'a>> {
    let policy_graphs =
        resolve_policy_graphs_from_config(&ledger.snapshot, ledger.novelty.as_ref(), ledger.t())
            .await;

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
    opts: &QueryConnectionOptions,
) -> Result<PolicyWrappedView<'a>> {
    let policy_graphs = resolve_policy_graphs_from_config(&view.snapshot, view, view.to_t()).await;

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
    opts: &QueryConnectionOptions,
) -> Result<PolicyContext> {
    let policy_graphs = resolve_policy_graphs_from_config(snapshot, overlay, to_t).await;

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
    let opts = QueryConnectionOptions {
        identity: Some(identity_iri.to_string()),
        default_allow,
        ..Default::default()
    };
    wrap_policy_view(ledger, &opts).await
}

/// Read the config graph and resolve `f:policySource` to graph IDs.
///
/// Returns `[0]` (default graph) if the config graph is empty, unreadable,
/// or doesn't specify a policy source.
async fn resolve_policy_graphs_from_config(
    snapshot: &LedgerSnapshot,
    overlay: &dyn OverlayProvider,
    to_t: i64,
) -> Vec<fluree_db_core::GraphId> {
    let config = match crate::config_resolver::resolve_ledger_config(snapshot, overlay, to_t).await
    {
        Ok(Some(c)) => c,
        _ => return vec![0],
    };
    let resolved = crate::config_resolver::resolve_effective_config(&config, None);
    let source = resolved
        .policy
        .as_ref()
        .and_then(|p| p.policy_source.as_ref());
    match policy_builder::resolve_policy_source_g_ids(source, snapshot) {
        Ok(g_ids) => g_ids,
        Err(e) => {
            tracing::warn!(error = %e, "Failed to resolve f:policySource — using default graph");
            vec![0]
        }
    }
}
