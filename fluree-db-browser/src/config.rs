//! Tunables for the browser I/O layer.

use std::time::Duration;

/// Configuration for a browser peer's I/O layer.
///
/// Every field has a documented default; construct with
/// `BrowserIoConfig::default()` and override what matters.
#[derive(Debug, Clone)]
pub struct BrowserIoConfig {
    /// Per-request timeout for CAS block fetches, enforced by the driver
    /// with an `AbortController`. Matches the native peer's 60 s block
    /// timeout.
    pub fetch_timeout: Duration,
    /// Per-request timeout for nameservice lookups (native peer: 30 s).
    pub nameservice_timeout: Duration,
    /// Maximum number of CAS fetches in flight at once — the prefetch
    /// width. On a single-threaded target concurrency means overlapping
    /// requests, so this bounds the browser's per-host connection use.
    pub max_concurrent_fetches: usize,
    /// Byte budget for the in-memory residency tier (the bytes the sync
    /// read path can see). Pinned entries never count as evictable.
    pub residency_budget_bytes: usize,
    /// IndexedDB persistence settings.
    pub cache: CacheConfig,
}

impl Default for BrowserIoConfig {
    fn default() -> Self {
        Self {
            fetch_timeout: Duration::from_secs(60),
            nameservice_timeout: Duration::from_secs(30),
            max_concurrent_fetches: 8,
            residency_budget_bytes: 256 * 1024 * 1024,
            cache: CacheConfig::default(),
        }
    }
}

/// IndexedDB CAS cache settings.
///
/// The cache is keyed by CID and every entry is immutable, so eviction is
/// purely a resource policy — nothing is ever invalidated.
#[derive(Debug, Clone)]
pub struct CacheConfig {
    /// Whether to persist fetched blocks at all. When `false` the residency
    /// tier is the only cache and every session starts cold.
    pub enabled: bool,
    /// IndexedDB database name. CIDs are server-independent, so one
    /// database is shared across origins by default.
    pub db_name: String,
    /// Byte budget for persisted blocks.
    pub budget_bytes: u64,
    /// When the budget is exceeded, evict least-recently-used entries until
    /// usage falls to this fraction of the budget.
    pub low_water_ratio: f64,
    /// How often batched last-access timestamps are flushed to the
    /// database. Reads update access times in memory only; the flush is
    /// what makes LRU ordering survive a reload.
    pub access_flush_interval: Duration,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            db_name: "fluree-cas-v1".to_string(),
            budget_bytes: 512 * 1024 * 1024,
            low_water_ratio: 0.8,
            access_flush_interval: Duration::from_secs(30),
        }
    }
}

impl CacheConfig {
    /// The eviction target in bytes (`budget_bytes * low_water_ratio`),
    /// clamped to a sane range.
    pub fn low_water_bytes(&self) -> u64 {
        let ratio = self.low_water_ratio.clamp(0.0, 1.0);
        (self.budget_bytes as f64 * ratio) as u64
    }
}
