//! Cache configuration and memory-based sizing.
//!
//! The Fluree API uses a single global cache budget in MB. This module provides
//! the default sizing rule: a tiered fraction of system RAM (native) with a
//! conservative fallback on platforms where memory detection is unavailable.
//!
//! Tiers (native):
//! - `< 4 GB`    → 30% (fixed runtime/txn overhead dominates on small hosts)
//! - `4 – 8 GB`  → 40%
//! - `≥ 8 GB`    → 50%

use tracing::info;

/// Default cache size in MB when memory detection is unavailable (WASM/JS)
pub const DEFAULT_CACHE_MB_FALLBACK: usize = 1000;

/// Calculate the default cache size in MB based on available system memory.
///
/// Uses a tiered fraction of total system memory:
/// - `< 4 GB`    → 30%
/// - `4 – 8 GB`  → 40%
/// - `≥ 8 GB`    → 50%
///
/// On WASM, returns a conservative 1000 MB default. If memory detection fails
/// (sandboxing, permissions), falls back to [`DEFAULT_CACHE_MB_FALLBACK`].
///
/// Returns the cache size in megabytes (minimum 100 MB).
#[cfg(feature = "native")]
pub fn default_cache_max_mb() -> usize {
    use sysinfo::{MemoryRefreshKind, System};

    let mut sys = System::new();
    sys.refresh_memory_specifics(MemoryRefreshKind::everything());

    let total_memory_bytes = sys.total_memory();

    if total_memory_bytes == 0 {
        info!(
            "Could not detect system memory, using fallback cache size of {}MB",
            DEFAULT_CACHE_MB_FALLBACK
        );
        return DEFAULT_CACHE_MB_FALLBACK;
    }

    let total_mb = (total_memory_bytes / (1024 * 1024)) as usize;
    let (numerator, denominator, pct) = if total_mb < 4 * 1024 {
        (3, 10, 30)
    } else if total_mb < 8 * 1024 {
        (2, 5, 40)
    } else {
        (1, 2, 50)
    };
    let cache_mb = (total_mb * numerator / denominator).max(100);

    info!(
        "Detected {}MB total memory, setting default cache to {}MB ({}%)",
        total_mb, cache_mb, pct
    );

    cache_mb
}

/// Calculate the default cache size in MB (WASM fallback).
#[cfg(not(feature = "native"))]
pub fn default_cache_max_mb() -> usize {
    DEFAULT_CACHE_MB_FALLBACK
}
