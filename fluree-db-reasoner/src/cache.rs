//! LRU cache for reasoning results
//!
//! This module provides caching infrastructure for OWL2-RL materialization results.
//! Caching is essential because reasoning can be expensive, and many queries against
//! the same database state should reuse computed derived facts.

use lru::LruCache;
use parking_lot::RwLock;
use std::hash::{Hash, Hasher};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use crate::overlay::DerivedFactsOverlay;
use crate::types::ReasoningModes;

/// Cache key for derived facts
///
/// MUST include all state that affects materialization to ensure cache correctness.
/// A cache hit should only occur when all these values match.
#[derive(Clone, Debug)]
pub struct ReasoningCacheKey {
    /// Ledger alias (identifier for the ledger)
    pub ledger_id: Arc<str>,
    /// Commit epoch (more robust than raw t for snapshot/time-travel)
    pub db_epoch: u64,
    /// Query "as-of" time (historical query support)
    pub to_t: i64,
    /// Novelty/staged overlay epoch for execute_with_overlay_*
    pub overlay_epoch: u64,
    /// Schema version that affects rules
    pub ontology_epoch: u64,
    /// Which reasoning modes are enabled (rdfs, owl2ql, owl2rl, etc.)
    pub reasoning_modes: ReasoningModes,
    /// Hash of rule-specific options
    ///
    /// INCLUDE: enabled RL rule subset, budgets (max_duration, max_facts, max_memory),
    ///          canonicalize-only vs expand-on-lookup policy, max sameAs expansion fanout
    /// EXCLUDE: purely observational toggles (collect rules_fired stats) unless they change output
    pub rule_config_hash: u64,
}

impl Hash for ReasoningCacheKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.ledger_id.hash(state);
        self.db_epoch.hash(state);
        self.to_t.hash(state);
        self.overlay_epoch.hash(state);
        self.ontology_epoch.hash(state);
        // ReasoningModes fields
        self.reasoning_modes.rdfs.hash(state);
        self.reasoning_modes.owl2ql.hash(state);
        self.reasoning_modes.datalog.hash(state);
        self.reasoning_modes.owl2rl.hash(state);
        self.reasoning_modes.explicit_none.hash(state);
        self.rule_config_hash.hash(state);
    }
}

impl PartialEq for ReasoningCacheKey {
    fn eq(&self, other: &Self) -> bool {
        self.ledger_id == other.ledger_id
            && self.db_epoch == other.db_epoch
            && self.to_t == other.to_t
            && self.overlay_epoch == other.overlay_epoch
            && self.ontology_epoch == other.ontology_epoch
            && self.reasoning_modes.rdfs == other.reasoning_modes.rdfs
            && self.reasoning_modes.owl2ql == other.reasoning_modes.owl2ql
            && self.reasoning_modes.datalog == other.reasoning_modes.datalog
            && self.reasoning_modes.owl2rl == other.reasoning_modes.owl2rl
            && self.reasoning_modes.explicit_none == other.reasoning_modes.explicit_none
            && self.rule_config_hash == other.rule_config_hash
    }
}

impl Eq for ReasoningCacheKey {}

/// Budget constraints for reasoning operations
///
/// Instead of a hard iteration cap (e.g. max=10), we use
/// time/memory/fact budgets that can be tuned for different workloads.
#[derive(Clone, Debug)]
pub struct ReasoningBudget {
    /// Max wall-clock time for materialization
    pub max_duration: Duration,
    /// Max derived facts before stopping
    pub max_facts: usize,
    /// Max memory estimate for overlay (bytes)
    pub max_memory_bytes: usize,
}

impl Default for ReasoningBudget {
    fn default() -> Self {
        Self {
            max_duration: Duration::from_secs(30),
            max_facts: 1_000_000,
            max_memory_bytes: 100 * 1024 * 1024, // 100MB
        }
    }
}

impl ReasoningBudget {
    /// Create a budget with custom limits
    pub fn new(max_duration: Duration, max_facts: usize, max_memory_bytes: usize) -> Self {
        Self {
            max_duration,
            max_facts,
            max_memory_bytes,
        }
    }

    /// Create an unlimited budget (for testing or small datasets)
    pub fn unlimited() -> Self {
        Self {
            max_duration: Duration::from_secs(3600), // 1 hour
            max_facts: usize::MAX,
            max_memory_bytes: usize::MAX,
        }
    }

    /// Compute a hash of budget settings for cache key
    pub fn config_hash(&self) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        let mut h = DefaultHasher::new();
        self.max_duration.as_millis().hash(&mut h);
        self.max_facts.hash(&mut h);
        self.max_memory_bytes.hash(&mut h);
        h.finish()
    }
}

/// Diagnostics from a reasoning operation
///
/// Always returned alongside the overlay so callers can understand
/// what happened during materialization.
#[derive(Clone, Debug, Default)]
pub struct ReasoningDiagnostics {
    /// Number of fixpoint iterations performed
    pub iterations: usize,
    /// Total number of facts derived
    pub facts_derived: usize,
    /// Whether reasoning was capped before reaching fixpoint
    pub capped: bool,
    /// Reason for capping, if applicable
    pub capped_reason: Option<String>,
    /// Wall-clock duration of reasoning
    pub duration: Duration,
    /// Count of how many times each rule fired
    pub rules_fired: hashbrown::HashMap<String, usize>,
}

impl ReasoningDiagnostics {
    /// Create diagnostics for a capped result
    pub fn capped(
        reason: impl Into<String>,
        iterations: usize,
        facts: usize,
        duration: Duration,
    ) -> Self {
        Self {
            iterations,
            facts_derived: facts,
            capped: true,
            capped_reason: Some(reason.into()),
            duration,
            rules_fired: hashbrown::HashMap::new(),
        }
    }

    /// Create diagnostics for a completed (uncapped) result
    pub fn completed(iterations: usize, facts: usize, duration: Duration) -> Self {
        Self {
            iterations,
            facts_derived: facts,
            capped: false,
            capped_reason: None,
            duration,
            rules_fired: hashbrown::HashMap::new(),
        }
    }

    /// Record that a rule fired
    pub fn record_rule_fired(&mut self, rule_name: &str) {
        *self.rules_fired.entry(rule_name.to_string()).or_insert(0) += 1;
    }
}

/// Combined result of reasoning: overlay + diagnostics
///
/// Always returned together so callers have full visibility into what happened.
#[derive(Debug)]
pub struct ReasoningResult {
    /// The derived facts overlay
    pub overlay: DerivedFactsOverlay,
    /// Diagnostics about the reasoning process
    pub diagnostics: ReasoningDiagnostics,
}

impl ReasoningResult {
    /// Create a new result
    pub fn new(overlay: DerivedFactsOverlay, diagnostics: ReasoningDiagnostics) -> Self {
        Self {
            overlay,
            diagnostics,
        }
    }
}

/// Thread-safe LRU cache for reasoning results
///
/// Uses a read-write lock to allow concurrent reads while serializing writes.
/// The cache stores `Arc<ReasoningResult>` for cheap cloning.
pub struct ReasoningCache {
    inner: RwLock<LruCache<ReasoningCacheKey, Arc<ReasoningResult>>>,
}

impl ReasoningCache {
    /// Create a new cache with the specified capacity
    ///
    /// Default capacity is 16 entries, which is reasonable for typical
    /// workloads where a small number of database states are queried frequently.
    pub fn new(capacity: usize) -> Self {
        let cap = NonZeroUsize::new(capacity).expect("capacity must be > 0");
        Self {
            inner: RwLock::new(LruCache::new(cap)),
        }
    }

    /// Create a cache with default capacity (16 entries)
    pub fn with_default_capacity() -> Self {
        Self::new(16)
    }

    /// Get a cached result if present
    ///
    /// This promotes the entry to most-recently-used.
    pub fn get(&self, key: &ReasoningCacheKey) -> Option<Arc<ReasoningResult>> {
        self.inner.write().get(key).cloned()
    }

    /// Peek at a cached result without updating LRU order
    pub fn peek(&self, key: &ReasoningCacheKey) -> Option<Arc<ReasoningResult>> {
        self.inner.read().peek(key).cloned()
    }

    /// Insert a result into the cache
    pub fn insert(&self, key: ReasoningCacheKey, result: Arc<ReasoningResult>) {
        self.inner.write().put(key, result);
    }

    /// Get the number of cached entries
    pub fn len(&self) -> usize {
        self.inner.read().len()
    }

    /// Check if the cache is empty
    pub fn is_empty(&self) -> bool {
        self.inner.read().is_empty()
    }

    /// Clear all cached entries
    pub fn clear(&self) {
        self.inner.write().clear();
    }

    /// Get the cache capacity
    pub fn capacity(&self) -> usize {
        self.inner.read().cap().get()
    }
}

impl Default for ReasoningCache {
    fn default() -> Self {
        Self::with_default_capacity()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_key(alias: &str, epoch: u64) -> ReasoningCacheKey {
        ReasoningCacheKey {
            ledger_id: alias.into(),
            db_epoch: epoch,
            to_t: 0,
            overlay_epoch: 0,
            ontology_epoch: 0,
            reasoning_modes: ReasoningModes::default(),
            rule_config_hash: 0,
        }
    }

    fn make_result() -> Arc<ReasoningResult> {
        Arc::new(ReasoningResult {
            overlay: DerivedFactsOverlay::empty(),
            diagnostics: ReasoningDiagnostics::default(),
        })
    }

    #[test]
    fn test_cache_insert_get() {
        let cache = ReasoningCache::new(4);
        let key = make_key("test", 1);
        let result = make_result();

        assert!(cache.get(&key).is_none());

        cache.insert(key.clone(), result.clone());
        assert!(cache.get(&key).is_some());
    }

    #[test]
    fn test_cache_lru_eviction() {
        let cache = ReasoningCache::new(2);

        // Insert 3 items into cache with capacity 2
        for i in 0..3 {
            cache.insert(make_key("test", i), make_result());
        }

        // First item should be evicted
        assert!(cache.get(&make_key("test", 0)).is_none());
        assert!(cache.get(&make_key("test", 1)).is_some());
        assert!(cache.get(&make_key("test", 2)).is_some());
    }

    #[test]
    fn test_cache_key_equality() {
        let key1 = make_key("test", 1);
        let key2 = make_key("test", 1);
        let key3 = make_key("test", 2);

        assert_eq!(key1, key2);
        assert_ne!(key1, key3);
    }

    #[test]
    fn test_budget_defaults() {
        let budget = ReasoningBudget::default();
        assert_eq!(budget.max_duration, Duration::from_secs(30));
        assert_eq!(budget.max_facts, 1_000_000);
        assert_eq!(budget.max_memory_bytes, 100 * 1024 * 1024);
    }
}
