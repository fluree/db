//! Per-ledger serving-posture gates (`f:servingDefaults`).
//!
//! A ledger's config graph can declare which serving tiers its origin server
//! offers: query execution (`f:serveQuery`) and raw block/replication content
//! (`f:serveBlocks`). These gates bind the origin (write-authority) serving
//! surface only — a read-only peer or mount that holds the ledger's blocks
//! always queries its own copy freely, so peer-role servers skip the query
//! gate entirely.
//!
//! Absent settings mean "allowed": an unconfigured ledger is fully served.
//! `f:publicVisibility` defaults to false and is reserved for the anonymous
//! access tier.

use crate::error::ServerError;
use fluree_db_api::{config_resolver, LedgerState};
use fluree_db_core::ledger_config::ServingDefaults;
use fluree_db_core::OverlayProvider;

/// Effective serving posture for one ledger on this server.
#[derive(Debug, Clone, Copy)]
pub(crate) struct EffectiveServing {
    /// Origin executes queries for this ledger.
    pub query: bool,
    /// Origin serves raw CAS blocks / replication content for this ledger.
    pub blocks: bool,
    /// Ledger is discoverable/readable without a token (anonymous tier).
    #[expect(dead_code)]
    // Kept for: the public/anonymous access tier of remote mounts.
    // Use when: extractors gain an anonymous path gated on f:publicVisibility.
    pub public: bool,
}

impl EffectiveServing {
    fn from_config(cfg: Option<&ServingDefaults>) -> Self {
        Self {
            query: cfg.and_then(|s| s.serve_query).unwrap_or(true),
            blocks: cfg.and_then(|s| s.serve_blocks).unwrap_or(true),
            public: cfg.and_then(|s| s.public_visibility).unwrap_or(false),
        }
    }

    /// Serving tiers as advertised in nameservice responses.
    pub fn advertised(&self) -> Vec<&'static str> {
        let mut tiers = Vec::with_capacity(2);
        if self.query {
            tiers.push("query");
        }
        if self.blocks {
            tiers.push("blocks");
        }
        tiers
    }
}

/// Resolve the serving posture from an already-loaded ledger state.
///
/// Reads the config graph as-of `state.t()` (novelty-inclusive), so a
/// committed-but-unindexed config change takes effect immediately.
pub(crate) async fn effective_serving_from_state(
    state: &LedgerState,
) -> Result<EffectiveServing, ServerError> {
    let overlay: &dyn OverlayProvider = &*state.novelty;
    let serving = config_resolver::resolve_serving_only(&state.snapshot, overlay, state.t())
        .await
        .map_err(|e| ServerError::internal(format!("Serving config resolution failed: {e}")))?;
    Ok(EffectiveServing::from_config(serving.as_ref()))
}

// Resolving serving posture by ledger alias goes through
// `AppState::effective_serving_cached`, which memoizes the posture per
// `(ledger, t)` — see fluree-db-server/src/state.rs.
