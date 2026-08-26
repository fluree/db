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
    /// Byte bound on blocks queued for the IndexedDB write-behind. When
    /// IndexedDB falls behind the network, fetch completion waits on this
    /// gauge instead of queueing unbounded block clones in memory.
    pub write_behind_budget_bytes: u64,
    /// How long a deferred residency insert waits for a release (a query
    /// finishing, bytes removed) before failing with the typed error. The
    /// safety net against waiting on one's own query guard.
    pub budget_wait: Duration,
    /// First SSE head-tracking reconnect delay (grows ×2 with ±25% jitter,
    /// resetting on a clean stream end — the native peer's policy).
    pub reconnect_initial: Duration,
    /// Reconnect delay ceiling.
    pub reconnect_max: Duration,
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
            write_behind_budget_bytes: 64 * 1024 * 1024,
            budget_wait: Duration::from_secs(10),
            reconnect_initial: Duration::from_secs(1),
            reconnect_max: Duration::from_secs(30),
            cache: CacheConfig::default(),
        }
    }
}

impl BrowserIoConfig {
    /// Derive every memory-drawing knob from one ceiling — the memory the
    /// embedding page grants the engine worker (ideally the module's linked
    /// `WebAssembly.Memory` maximum minus headroom).
    ///
    /// Split: 55% residency tier, 10% write-behind queue (clamped to
    /// 8–128 MiB), fetch width one slot per 64 MiB (clamped to 2–16, which
    /// also bounds transient fetch-body buffers). The remaining ~35% is
    /// headroom for what this crate cannot govern: the query engine's own
    /// operator memory, novelty, JS-side copies — and notably the forward
    /// pack readers, which pin pack bytes for the store's lifetime outside
    /// any budget here (a known engine-side gap tracked by the read-path
    /// work). The IndexedDB budget is disk, not memory, and is untouched.
    pub fn from_max_memory(max_memory_bytes: usize) -> Self {
        let write_behind =
            ((max_memory_bytes / 10) as u64).clamp(8 * 1024 * 1024, 128 * 1024 * 1024);
        let width = (max_memory_bytes / (64 * 1024 * 1024)).clamp(2, 16);
        Self {
            residency_budget_bytes: max_memory_bytes / 100 * 55,
            write_behind_budget_bytes: write_behind,
            max_concurrent_fetches: width,
            ..Default::default()
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

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;

    #[test]
    fn from_max_memory_derives_every_memory_knob_from_one_ceiling() {
        const GIB: usize = 1024 * 1024 * 1024;
        let config = BrowserIoConfig::from_max_memory(GIB);
        assert_eq!(config.residency_budget_bytes, GIB / 100 * 55);
        assert_eq!(config.write_behind_budget_bytes, (GIB / 10) as u64);
        assert_eq!(config.max_concurrent_fetches, 16);

        // A small ceiling clamps to the floors.
        let small = BrowserIoConfig::from_max_memory(64 * 1024 * 1024);
        assert_eq!(small.write_behind_budget_bytes, 8 * 1024 * 1024);
        assert_eq!(small.max_concurrent_fetches, 2);
    }
}
