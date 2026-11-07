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
pub mod compile;
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
    approx_flake_bytes, ReasoningBudget, ReasoningCache, ReasoningCacheKey, ReasoningDiagnostics,
    ReasoningResult,
};
pub use compile::{compile, ClassRuleKind, CompiledRules, PropertyRuleKind};
pub use error::{ReasonerError, Result};
pub use overlay::{DerivedFactsBuilder, DerivedFactsOverlay};
pub use owl::{find_owl_typed_entities, OwlSidRegistry};
pub use rdf_list::{collect_chain_elements, collect_list_elements, resolve_property_expression};
pub use same_as::{FrozenSameAs, SameAsTracker};

use fluree_db_core::{GraphDbRef, QueryCancellation, Tracker};
use std::sync::Arc;
pub use types::{ChainElement, PropertyChain, PropertyExpression};

/// Options for OWL2-RL reasoning
#[derive(Clone, Debug, Default)]
pub struct ReasoningOptions {
    /// Budget constraints for materialization
    pub budget: ReasoningBudget,
    /// Which RL rules to enable (empty = all).
    ///
    /// Names are the W3C OWL2-RL rule identifiers used in
    /// [`ReasoningDiagnostics::rules_fired`]: `prp-symp`, `prp-trp`, `prp-inv`,
    /// `prp-dom`, `prp-rng`, `prp-spo1`, `prp-spo2`, `prp-fp`, `prp-ifp`,
    /// `prp-key`, `cax-sco`, `cax-eqc`, `cls-hv1`, `cls-hv2`, `cls-svf1`,
    /// `cls-avf`, `cls-int1`, `cls-int2`, `cls-uni`, `cls-oo`, `cls-maxc2`,
    /// `cls-maxqc`.
    ///
    /// Where one implementation covers several spec rules, the spec IDs are
    /// accepted as aliases: `cax-eqc1`/`cax-eqc2` for `cax-eqc`, and
    /// `cls-maxqc3`/`cls-maxqc4` for `cls-maxqc`.
    ///
    /// `owl:sameAs` equality processing (`eq-*`) is canonicalization
    /// infrastructure and always runs.
    pub enabled_rules: Vec<String>,
    /// The cancellation handle of the request the closure is computed for.
    /// Once it reports cancellation, the run stops and fails.
    pub cancellation: QueryCancellation,
    /// The request's tracker: each fact a round processes is charged as its
    /// fuel.
    pub tracker: Tracker,
    /// Bounds on maintaining a closed state
    /// ([`fixpoint::run_fixpoint_incremental`]).
    pub maintenance: MaintenanceBounds,
}

/// How much a run maintaining a closed state may read and hold. Past any of
/// these it stops, and the caller computes the closure in full.
#[derive(Clone, Debug)]
pub struct MaintenanceBounds {
    /// Bytes of closed-state reads kept from one round to the next.
    pub cache_bytes: usize,
    /// Bytes of reads, candidate conclusions and derived facts held at once.
    pub working_bytes: usize,
    /// Facts one read may return.
    pub read_facts: usize,
}

impl Default for MaintenanceBounds {
    fn default() -> Self {
        Self {
            cache_bytes: 64 << 20,
            working_bytes: 1 << 30,
            read_facts: 1 << 20,
        }
    }
}

/// All rule identifiers accepted in [`ReasoningOptions::enabled_rules`]: the
/// implemented W3C OWL2-RL rule IDs plus the spec-ID aliases documented there.
pub const KNOWN_RULE_NAMES: &[&str] = &[
    "prp-symp",
    "prp-trp",
    "prp-inv",
    "prp-dom",
    "prp-rng",
    "prp-spo1",
    "prp-spo2",
    "prp-fp",
    "prp-ifp",
    "prp-key",
    "cax-sco",
    "cax-eqc",
    "cax-eqc1",
    "cax-eqc2",
    "cls-hv1",
    "cls-hv2",
    "cls-svf1",
    "cls-avf",
    "cls-int1",
    "cls-int2",
    "cls-uni",
    "cls-oo",
    "cls-maxc2",
    "cls-maxqc",
    "cls-maxqc3",
    "cls-maxqc4",
];

impl ReasoningOptions {
    /// Create options with default budget and all rules enabled
    pub fn new() -> Self {
        Self::default()
    }

    /// Reject unknown names in `enabled_rules`.
    ///
    /// The rule gates do plain string matching, so a misspelled name would
    /// otherwise match nothing and silently disable the intended rule.
    pub fn validate(&self) -> Result<()> {
        if let Some(unknown) = self
            .enabled_rules
            .iter()
            .find(|r| !KNOWN_RULE_NAMES.contains(&r.as_str()))
        {
            return Err(ReasonerError::UnknownRule(unknown.clone()));
        }
        Ok(())
    }

    /// Create options with custom budget
    pub fn with_budget(budget: ReasoningBudget) -> Self {
        Self {
            budget,
            ..Self::default()
        }
    }

    /// Whether a rule (by W3C OWL2-RL identifier) should run.
    ///
    /// An empty `enabled_rules` list enables every rule.
    pub fn rule_enabled(&self, name: &str) -> bool {
        self.rule_enabled_any(&[name])
    }

    /// Whether a rule known under any of the given identifiers should run.
    ///
    /// Used for gates whose single implementation covers several spec rules
    /// (e.g. `cax-eqc` implements `cax-eqc1` and `cax-eqc2`).
    pub fn rule_enabled_any(&self, names: &[&str]) -> bool {
        self.enabled_rules.is_empty()
            || self
                .enabled_rules
                .iter()
                .any(|r| names.iter().any(|n| r == n))
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
/// Results are cached by (ledger_id, db_epoch, to_t, overlay content version,
/// ontology_epoch, config). Cache hits return immediately without
/// recomputation. An overlay that cannot vouch for a content version is
/// materialized fresh and never cached.
pub async fn reason_owl2rl(
    db: GraphDbRef<'_>,
    opts: &ReasoningOptions,
    cache: &ReasoningCache,
) -> Result<Arc<ReasoningResult>> {
    opts.validate()?;

    // Get ontology epoch from schema (or 0 if no schema)
    let ontology_epoch = db.snapshot.schema_epoch().unwrap_or(0);

    // Build cache key with real values from snapshot and execution context
    let key = db
        .overlay
        .content_version()
        .map(|overlay_version| ReasoningCacheKey {
            ledger_id: db.snapshot.ledger_id.as_str().into(),
            db_epoch: db.snapshot.t as u64,
            to_t: db.t,
            overlay_version,
            ontology_epoch,
            rule_config_hash: opts.config_hash(),
        });

    // Check cache
    if let Some(cached) = key.as_ref().and_then(|key| cache.get(key)) {
        return Ok(cached);
    }

    // Run fixpoint reasoning
    let (derived_flakes, same_as, diagnostics) = fixpoint::run_fixpoint(db, opts).await?;

    // Build the derived facts overlay
    let mut builder = DerivedFactsBuilder::with_capacity(derived_flakes.len());
    for flake in derived_flakes {
        builder.push(flake);
    }
    let derived_overlay = builder.build(same_as, db.overlay.epoch());

    let result = Arc::new(ReasoningResult::new(derived_overlay, diagnostics));

    // Store in cache
    if let Some(key) = key {
        cache.insert(key, result.clone());
    }

    Ok(result)
}

/// OWL 2 RL closure maintenance under insertions: what the closure of `db`
/// gains when `db` holds a closed state plus `inserted`. `None` when the
/// program cannot be maintained this way (see
/// [`fixpoint::run_fixpoint_incremental`]). Never cached: the result describes
/// a change, not a state.
pub async fn reason_owl2rl_incremental(
    db: GraphDbRef<'_>,
    inserted: &[fluree_db_core::Flake],
    opts: &ReasoningOptions,
) -> Result<Option<Arc<ReasoningResult>>> {
    opts.validate()?;
    let Some((derived_flakes, diagnostics)) =
        fixpoint::run_fixpoint_incremental(db, inserted, opts).await?
    else {
        return Ok(None);
    };
    let start = fluree_db_core::clock::Instant::now();
    let mut builder = DerivedFactsBuilder::with_capacity(derived_flakes.len());
    for flake in derived_flakes {
        builder.push(flake);
    }
    let derived_overlay = builder.build(FrozenSameAs::empty(), db.overlay.epoch());
    tracing::debug!(
        overlay_ms = start.elapsed().as_millis() as u64,
        "OWL 2 RL incremental overlay"
    );
    Ok(Some(Arc::new(ReasoningResult::new(
        derived_overlay,
        diagnostics,
    ))))
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
    fn test_validate_rejects_unknown_rule_name() {
        let mut opts = ReasoningOptions::default();
        assert!(opts.validate().is_ok());

        opts.enabled_rules = vec!["prp-spo1".to_string(), "cax-eqc2".to_string()];
        assert!(opts.validate().is_ok());

        opts.enabled_rules.push("prp-sop1".to_string()); // typo
        let err = opts.validate().unwrap_err();
        assert!(matches!(err, ReasonerError::UnknownRule(ref n) if n == "prp-sop1"));
        assert!(err.to_string().contains("prp-spo1"));
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
