//! Per-instance governance artifact cache.
//!
//! Lives on the `Fluree` handle so resolved artifacts are shareable
//! across requests and across every data ledger that references the
//! same `(ArtifactKind, model_ledger_id, graph_iri, resolved_t)`.
//! That is the property that makes "model edit propagates atomically
//! to every governed dataset" cheap — one cache entry is reused by
//! every D that points at the same M graph at the same M-t.
//!
//! Phase 1a uses a Moka TinyLFU cache bounded by entry count. Unifying
//! with `fluree-db-binary-index::LeafletCache`'s byte budget is a
//! follow-up: the binary-index crate sits below `fluree-db-api`,
//! `fluree-db-policy`, and the cross-ledger module, so caching typed
//! `ResolvedGraph` values there would be a layering inversion. The
//! follow-up adds an opaque-blob variant to `LeafletCache`, serializes
//! the artifact to bytes at this boundary, and re-uses the existing
//! `FLUREE_LEAFLET_CACHE_BYTES` budget. Deferred until the artifact
//! representation stabilizes.
//!
//! The cache is read- and write-only on the resolver hot path:
//!
//! - Resolver: per-request memo miss → cache lookup → cache miss
//!   triggers materialization → write back into both per-request memo
//!   and instance cache → return.
//! - Cache invalidation is implicit: new commits to M produce new
//!   `resolved_t` values and therefore new keys. Old entries age out
//!   under TinyLFU eviction. There is no watermark-on-write channel.

use super::types::{ResolutionKey, ResolvedGraph};
use moka::sync::Cache;
use std::sync::Arc;

/// Default maximum number of cache entries.
///
/// Sized for governance artifacts, which are coarser than leaflets —
/// a few thousand `(model, graph, t)` triples cover most realistic
/// fleets. Phase 1a doesn't expose a tuning knob; if memory pressure
/// becomes a concern the right fix is the byte-budget unification
/// described in the module doc.
const DEFAULT_MAX_ENTRIES: u64 = 4_096;

/// Per-instance cache of resolved cross-ledger governance artifacts.
///
/// Thin wrapper around `moka::sync::Cache` so the cache type is named
/// and discoverable. Cheap to clone via `Arc`.
#[derive(Debug)]
pub struct GovernanceCache {
    inner: Cache<ResolutionKey, Arc<ResolvedGraph>>,
}

impl GovernanceCache {
    /// Build a cache with the default capacity.
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_MAX_ENTRIES)
    }

    /// Build a cache with a custom entry-count limit. Useful for tests
    /// that want a small cache to exercise eviction, or for embedded
    /// builds that want a tighter ceiling.
    pub fn with_capacity(max_entries: u64) -> Self {
        Self {
            inner: Cache::builder().max_capacity(max_entries).build(),
        }
    }

    /// Look up a resolved artifact. Returns `Some(Arc)` on hit; the
    /// returned Arc is shared with the cache and any other live
    /// references, so a cache hit is a cheap pointer clone.
    pub fn get(&self, key: &ResolutionKey) -> Option<Arc<ResolvedGraph>> {
        self.inner.get(key)
    }

    /// Insert a resolved artifact. Idempotent — if a value already
    /// exists for the key (e.g., two concurrent requests both raced
    /// to materialize the same (M, graph, t)) the existing entry
    /// is replaced. The resolver constructs an `Arc::new` per
    /// materialization, so concurrent racers don't share an Arc —
    /// the second one's writeback wins, the first's Arc becomes
    /// reachable only through any prior `get` clones.
    pub fn insert(&self, key: ResolutionKey, value: Arc<ResolvedGraph>) {
        self.inner.insert(key, value);
    }

    /// Approximate count of entries currently cached. Exposed for
    /// tests and diagnostics — Moka's count is best-effort.
    pub fn entry_count(&self) -> u64 {
        self.inner.entry_count()
    }
}

impl Default for GovernanceCache {
    fn default() -> Self {
        Self::new()
    }
}
