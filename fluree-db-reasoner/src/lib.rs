//! # Fluree DB Reasoner
//!
//! OWL2-RL reasoning engine for Fluree DB with caching and efficient owl:sameAs handling.
//!
//! This crate provides:
//! - Forward-chaining OWL2-RL materialization
//! - LRU caching of derived facts
//! - Union-find based owl:sameAs equivalence tracking
//! - Time/memory/fact budgets for termination control
//!
//! ## Key Types
//!
//! - [`ReasoningCache`]: LRU cache for derived facts, keyed by database state
//! - [`SameAsTracker`]: Mutable union-find for owl:sameAs during materialization
//! - [`FrozenSameAs`]: Immutable equivalence state for query-time use
//! - [`DerivedFactsOverlay`]: Implements `OverlayProvider` for composing with base overlays
//! - [`ReasoningBudget`]: Time/memory/fact limits for materialization
//!
//! ## Example
//!
//! ```ignore
//! use fluree_db_reasoner::{reason_owl2rl, ReasoningCache, ReasoningOptions};
//! use fluree_db_core::GraphDbRef;
//!
//! // Create a cache (typically application-scoped)
//! let cache = ReasoningCache::with_default_capacity();
//!
//! // Bundle snapshot + graph + overlay + time into GraphDbRef
//! let db = GraphDbRef::new(&snapshot, g_id, &overlay, to_t);
//!
//! // Run reasoning (will hit cache on subsequent calls with same state)
//! let result = reason_owl2rl(db, &opts, &cache).await?;
//!
//! // Use derived facts in query
//! let composite = CompositeOverlay::new(vec![&base_overlay, &result.overlay]);
//! ```

pub mod cache;
pub mod datalog;
pub mod error;
pub mod execute;
pub mod fixpoint;
pub mod ontology_rl;
pub mod overlay;
pub mod owl;
pub mod rdf_list;
pub mod restrictions;
pub mod same_as;
pub mod types;

// Re-exports for convenience
pub use cache::{
    ReasoningBudget, ReasoningCache, ReasoningCacheKey, ReasoningDiagnostics, ReasoningResult,
};
pub use datalog::{
    execute_rule_with_bindings, instantiate_pattern, BindingValue, Bindings, CompareOp,
    DatalogRule, DatalogRuleSet, RuleFilter, RuleTerm, RuleTriplePattern, RuleValue,
};
pub use error::{ReasonerError, Result};
pub use overlay::{DerivedFactsBuilder, DerivedFactsOverlay};
pub use owl::{find_owl_typed_entities, OwlSidRegistry};
pub use rdf_list::{
    collect_chain_elements, collect_list_elements, collect_list_values, resolve_property_expression,
};
pub use same_as::{FrozenSameAs, SameAsTracker};

use fluree_db_core::GraphDbRef;
use std::sync::Arc;
pub use types::{ChainElement, PropertyChain, PropertyExpression, ReasoningModes};

/// Options for OWL2-RL reasoning
#[derive(Clone, Debug, Default)]
pub struct ReasoningOptions {
    /// Budget constraints for materialization
    pub budget: ReasoningBudget,
    /// Which RL rules to enable (empty = all)
    pub enabled_rules: Vec<String>,
}

impl ReasoningOptions {
    /// Create options with default budget and all rules enabled
    pub fn new() -> Self {
        Self::default()
    }

    /// Create options with custom budget
    pub fn with_budget(budget: ReasoningBudget) -> Self {
        Self {
            budget,
            enabled_rules: Vec::new(),
        }
    }

    /// Compute a hash for cache key purposes
    pub fn config_hash(&self) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut h = DefaultHasher::new();
        self.budget.config_hash().hash(&mut h);
        for rule in &self.enabled_rules {
            rule.hash(&mut h);
        }
        h.finish()
    }
}

/// Main entry point for OWL2-RL reasoning
///
/// Computes derived facts from OWL ontology statements, caching results for reuse.
///
/// # Arguments
///
/// * `db` - Bundled database reference (snapshot, graph, overlay, as-of time)
/// * `opts` - Reasoning options (budget, enabled rules)
/// * `cache` - LRU cache for storing/retrieving results
///
/// # Returns
///
/// `Arc<ReasoningResult>` containing the derived facts overlay and diagnostics.
/// The Arc allows cheap cloning and sharing across queries.
///
/// # Cache Behavior
///
/// Results are cached by (ledger_id, db_epoch, to_t, overlay_epoch, ontology_epoch, config).
/// Cache hits return immediately without recomputation.
pub async fn reason_owl2rl(
    db: GraphDbRef<'_>,
    opts: &ReasoningOptions,
    cache: &ReasoningCache,
) -> Result<Arc<ReasoningResult>> {
    // Get ontology epoch from schema (or 0 if no schema)
    let ontology_epoch = db.snapshot.schema_epoch().unwrap_or(0);

    // Build cache key with real values from snapshot and execution context
    let key = ReasoningCacheKey {
        ledger_id: db.snapshot.ledger_id.as_str().into(),
        db_epoch: db.snapshot.t as u64,
        to_t: db.t,
        overlay_epoch: db.overlay.epoch(),
        ontology_epoch,
        reasoning_modes: ReasoningModes::default(),
        rule_config_hash: opts.config_hash(),
    };

    // Check cache
    if let Some(cached) = cache.get(&key) {
        return Ok(cached);
    }

    // Run fixpoint reasoning
    let (derived_flakes, same_as, diagnostics) = fixpoint::run_fixpoint(db, &opts.budget).await?;

    // Build the derived facts overlay
    let mut builder = DerivedFactsBuilder::with_capacity(derived_flakes.len());
    for flake in derived_flakes {
        builder.push(flake);
    }
    let derived_overlay = builder.build(same_as, db.overlay.epoch());

    let result = Arc::new(ReasoningResult::new(derived_overlay, diagnostics));

    // Store in cache
    cache.insert(key, result.clone());

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_reasoning_options_default() {
        let opts = ReasoningOptions::default();
        assert!(opts.enabled_rules.is_empty());
    }

    #[test]
    fn test_reasoning_options_config_hash() {
        let opts1 = ReasoningOptions::default();
        let opts2 = ReasoningOptions::default();

        // Same options should produce same hash
        assert_eq!(opts1.config_hash(), opts2.config_hash());

        // Different budget should produce different hash
        let opts3 = ReasoningOptions::with_budget(ReasoningBudget::unlimited());
        assert_ne!(opts1.config_hash(), opts3.config_hash());
    }
}
