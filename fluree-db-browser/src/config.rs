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
    /// How long the driver waits for the IndexedDB open before giving up on
    /// persistence for this session.
    ///
    /// An open can hang with no event at all — not `success`, not `error`,
    /// not even `blocked` — when the database has been wedged. This bound is
    /// what guarantees the driver's "resolved" signal always arrives, so
    /// queued writes and their write-behind permits are never stranded. An
    /// open that has not landed by now is not going to help this session.
    pub cache_open_timeout: Duration,
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
            cache_open_timeout: Duration::from_secs(10),
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
            // Divide-before-multiply is deliberate: `* 55` first would
            // overflow a 32-bit usize for ceilings above ~78 MiB. The
            // 32 MiB floor keeps a tiny ceiling degraded (small cache,
            // more refetching) rather than bricked — below it, ordinary
            // leaflets already fail ObjectExceedsBudget.
            residency_budget_bytes: (max_memory_bytes / 100 * 55).max(32 * 1024 * 1024),
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

/// Milliseconds for a browser timer (`setTimeout`, and so every
/// `gloo_timers::TimeoutFuture` in this crate).
///
/// The saturating conversion this replaces was reaching for "effectively
/// never" and produced the exact opposite. A `setTimeout` delay is stored
/// in a SIGNED 32-bit int, so anything above `i32::MAX` overflows and the
/// timer fires IMMEDIATELY — a `u32::MAX` fallback is a zero-delay timer,
/// not an infinite one. (Observed, not theorized: substituting `u32::MAX`
/// for the cache-open bound made a wedged open resolve instantly.)
///
/// Clamping to `i32::MAX` (~24.8 days) keeps the intent: too long to
/// matter, and still a real delay. Durations that large are absurd for
/// every knob here, which is precisely why the failure would be so hard to
/// believe if it ever happened.
// Only the wasm driver modules call this in production; the clamp test below
// exercises it on host-test builds. `cfg(any(wasm32, test))` so a plain host
// lib build — where nothing calls it — does not see it as dead code.
#[cfg(any(target_arch = "wasm32", test))]
pub(crate) fn timer_millis(duration: Duration) -> u32 {
    const MAX_TIMER_MILLIS: u128 = i32::MAX as u128;
    duration.as_millis().min(MAX_TIMER_MILLIS) as u32
}

/// A declared `Content-Length` that exceeds `max` (returns the offending
/// length). Used to reject an oversized response body before it is
/// materialized into wasm linear memory. A missing or unparseable value
/// returns `None` — the pre-check does not apply, and the residency budget
/// still rejects the block after the fact. Pure, so it is unit-tested here
/// off-wasm; the wasm-only `driver::fetch` calls it.
// cfg(any(wasm32, test)): the only production caller is the wasm driver, so a
// host lib build would see it as dead code (the native test still exercises it).
#[cfg(any(target_arch = "wasm32", test))]
pub(crate) fn declared_length_over_cap(content_length: Option<String>, max: u64) -> Option<u64> {
    content_length
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|&len| len > max)
}

/// One step of the incremental response-body cap: fold `chunk_len` into
/// `running` (saturating — a chunk that would overflow `u64` is already far
/// past any real cap, so wrapping and comparing wrong is not a risk worth
/// taking) and report the new running total together with whether it now
/// exceeds `max`. Hot path: one saturating add, one compare — called once
/// per chunk read off the response stream. The counterpart to
/// [`declared_length_over_cap`] for a chunked response that never declares
/// a `Content-Length` at all, so the pre-check above has nothing to reject.
/// Pure, so it is unit-tested here off-wasm; the wasm-only `driver::fetch`
/// calls it once per chunk while draining the body.
#[cfg(any(target_arch = "wasm32", test))]
pub(crate) fn body_cap_step(running: u64, chunk_len: u64, max: u64) -> (u64, bool) {
    let total = running.saturating_add(chunk_len);
    (total, total > max)
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

    /// The clamp is the whole point: a `u32::MAX` fallback overflows
    /// `setTimeout`'s signed 32-bit delay and fires at once, so a bound
    /// meant to be unreachable becomes a bound that always trips.
    #[test]
    fn timer_millis_clamps_below_the_set_timeout_overflow() {
        assert_eq!(timer_millis(Duration::from_millis(0)), 0);
        assert_eq!(timer_millis(Duration::from_secs(10)), 10_000);
        // The largest delay a browser still treats as a delay.
        let max = i32::MAX as u32;
        assert_eq!(timer_millis(Duration::from_millis(u64::from(max))), max);
        // Anything past it clamps DOWN to that, never wrapping to a small
        // number and never reaching u32::MAX.
        for absurd in [
            Duration::from_millis(u64::from(max) + 1),
            Duration::from_secs(60 * 60 * 24 * 365),
            Duration::MAX,
        ] {
            let got = timer_millis(absurd);
            assert_eq!(got, max, "{absurd:?}");
            assert!(got <= max, "must never exceed a browser's signed delay");
        }
    }

    #[test]
    fn declared_length_over_cap_gates_oversized_bodies() {
        const CAP: u64 = 256 * 1024 * 1024;
        // Over the cap → rejected, reporting the offending length.
        assert_eq!(
            declared_length_over_cap(Some((CAP + 1).to_string()), CAP),
            Some(CAP + 1)
        );
        // At or under → allowed.
        assert_eq!(declared_length_over_cap(Some(CAP.to_string()), CAP), None);
        assert_eq!(
            declared_length_over_cap(Some("1024".to_string()), CAP),
            None
        );
        // Whitespace tolerated.
        assert_eq!(
            declared_length_over_cap(Some(format!(" {} ", CAP + 5)), CAP),
            Some(CAP + 5)
        );
        // Missing or unparseable → pre-check does not apply.
        assert_eq!(declared_length_over_cap(None, CAP), None);
        assert_eq!(
            declared_length_over_cap(Some("not-a-number".to_string()), CAP),
            None
        );
        assert_eq!(declared_length_over_cap(Some(String::new()), CAP), None);
    }

    #[test]
    fn body_cap_step_gates_the_running_total_not_any_one_chunk() {
        const CAP: u64 = 1024;
        // Individually tiny chunks that cross the cap only in aggregate —
        // exactly the shape a `declared_length_over_cap` pre-check cannot
        // see (no header ever claims the total up front).
        let (total, over) = body_cap_step(0, 600, CAP);
        assert_eq!((total, over), (600, false));
        let (total, over) = body_cap_step(total, 600, CAP);
        assert_eq!((total, over), (1200, true), "the chunk that tips it over");
        // At or under the cap: never flagged.
        let (total, over) = body_cap_step(0, CAP, CAP);
        assert_eq!((total, over), (CAP, false));

        // Saturating: a single absurd chunk length cannot wrap the total
        // back under the cap.
        let (total, over) = body_cap_step(u64::MAX - 10, 100, CAP);
        assert_eq!(total, u64::MAX);
        assert!(over);
    }

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

        // A tiny ceiling hits the residency floor: degraded (more
        // refetching), never bricked on ordinary leaflet sizes.
        let tiny = BrowserIoConfig::from_max_memory(16 * 1024 * 1024);
        assert_eq!(tiny.residency_budget_bytes, 32 * 1024 * 1024);
    }
}
