//! Bench profile, scale, and tokio runtime configuration.
//!
//! Three pieces driven by env vars so a contributor can run a bench at
//! different sizes without editing the bench file:
//!
//! | Env var | Type | Default | Effect |
//! |---|---|---|---|
//! | `FLUREE_BENCH_PROFILE` | `Quick` \| `Full` | `Quick` (local), `Full` (CI) | sample-count + warmup discipline |
//! | `FLUREE_BENCH_SCALE` | `Tiny` \| `Small` \| `Medium` \| `Large` | `Small` | per-bench input size |
//!
//! ## Profile semantics
//!
//! - **`Quick`** — fewer samples, no warmup, target wall-time ≤ 2s per bench.
//!   Used by `cargo bench` on a developer's machine and by the PR-gated CI job.
//! - **`Full`** — full criterion sample counts, full warmup, multi-size matrix.
//!   Used by the nightly job in `bench-nightly`.
//!
//! Benches read the profile via [`current_profile()`] and choose
//! `group.sample_size(...)` accordingly. The chassis does **not** override
//! criterion's defaults globally — each bench remains in control of its own
//! group config, and the env-driven values are advisory.
//!
//! ## Scale semantics
//!
//! Scale is a per-bench quantity. A contributor might map `Tiny` to 1k flakes,
//! `Small` to 10k, `Medium` to 100k, `Large` to 1M. The chassis exposes
//! [`BenchScale::elements_default()`] as a sensible starting curve; benches
//! may map differently when their workload demands it (e.g., vector benches
//! at 1k/5k/10k articles rather than 10k/100k flakes).

use std::sync::OnceLock;

// ---------------------------------------------------------------------------
// Profile
// ---------------------------------------------------------------------------

/// Bench profile: how aggressively criterion measures.
///
/// Read via [`current_profile()`]. Default is `Quick` if the env var is
/// unset or unrecognized.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BenchProfile {
    /// Few samples, no warmup, target wall-time ≤ 2s per bench. Local
    /// development and PR-gated CI.
    Quick,
    /// Full criterion sample counts, full warmup. Nightly CI.
    Full,
}

impl BenchProfile {
    /// Suggested criterion `sample_size`. Benches may override.
    ///
    /// `Quick` (PR-gated) keeps the sample tight (10) so the gate stays
    /// under its wall-clock budget; `Full` uses criterion's default
    /// (100) for a wider distribution suitable for the nightly. Both
    /// are starting points — once `bench-nightly` lands and we have
    /// flap data from real CI runs, we may need to bump `Full` higher
    /// (200+) to bring noise within the regression-budget thresholds.
    pub fn sample_size(self) -> usize {
        match self {
            BenchProfile::Quick => 10,
            BenchProfile::Full => 100,
        }
    }

    /// True when the bench should skip explicit warmup iterations and rely
    /// on criterion's default discipline.
    pub fn skip_explicit_warmup(self) -> bool {
        matches!(self, BenchProfile::Quick)
    }

    fn from_env_str(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "quick" | "q" | "fast" => Some(BenchProfile::Quick),
            "full" | "f" | "nightly" => Some(BenchProfile::Full),
            _ => None,
        }
    }
}

/// Read `FLUREE_BENCH_PROFILE` once and cache. Defaults to [`BenchProfile::Quick`].
pub fn current_profile() -> BenchProfile {
    static CACHED: OnceLock<BenchProfile> = OnceLock::new();
    *CACHED.get_or_init(|| {
        std::env::var("FLUREE_BENCH_PROFILE")
            .ok()
            .as_deref()
            .and_then(BenchProfile::from_env_str)
            .unwrap_or(BenchProfile::Quick)
    })
}

// ---------------------------------------------------------------------------
// Scale
// ---------------------------------------------------------------------------

/// Bench input scale. Each bench interprets these to its own units —
/// flakes, rows, articles, vectors, etc. — but the four-tier ladder is
/// uniform so a contributor running `FLUREE_BENCH_SCALE=medium` gets a
/// roughly comparable amount of work across benches.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BenchScale {
    /// ~1k elements. Smoke-test scale; suitable for PR-gated runs.
    Tiny,
    /// ~10k elements. Default. Useful for local iteration.
    Small,
    /// ~100k elements. Local pre-merge or nightly.
    Medium,
    /// ~1M elements. Nightly only; expects vendored or fetched fixtures.
    Large,
}

impl BenchScale {
    /// Suggested element count for benches that don't have a domain-specific
    /// curve. Vector / text benches typically pick smaller curves; this is
    /// a starting point.
    pub fn elements_default(self) -> u64 {
        match self {
            BenchScale::Tiny => 1_000,
            BenchScale::Small => 10_000,
            BenchScale::Medium => 100_000,
            BenchScale::Large => 1_000_000,
        }
    }

    /// Lowercase canonical name for use in `BenchmarkId` parameters and
    /// `regression-budget.json` keys. Benches should prefer this over
    /// hand-written strings so the budget keys stay consistent.
    pub fn as_str(self) -> &'static str {
        match self {
            BenchScale::Tiny => "tiny",
            BenchScale::Small => "small",
            BenchScale::Medium => "medium",
            BenchScale::Large => "large",
        }
    }

    fn from_env_str(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "tiny" | "xs" | "smoke" => Some(BenchScale::Tiny),
            "small" | "s" => Some(BenchScale::Small),
            "medium" | "m" | "med" => Some(BenchScale::Medium),
            "large" | "l" | "xl" => Some(BenchScale::Large),
            _ => None,
        }
    }
}

/// Read `FLUREE_BENCH_SCALE` once and cache. Defaults to [`BenchScale::Small`].
pub fn current_scale() -> BenchScale {
    static CACHED: OnceLock<BenchScale> = OnceLock::new();
    *CACHED.get_or_init(|| {
        std::env::var("FLUREE_BENCH_SCALE")
            .ok()
            .as_deref()
            .and_then(BenchScale::from_env_str)
            .unwrap_or(BenchScale::Small)
    })
}

// ---------------------------------------------------------------------------
// Tokio runtime
// ---------------------------------------------------------------------------

/// Build a tokio runtime for bench `b.iter(|| rt.block_on(async { ... }))`
/// usage.
///
/// **Single-threaded by default** to keep scheduling overhead off the
/// measurement and to match what every existing bench (insert_formats.rs,
/// vector_query.rs, fulltext_query.rs) was already using via
/// `tokio::runtime::Runtime::new()`.
///
/// Set `FLUREE_BENCH_RUNTIME=multi` to switch to a multi-threaded runtime
/// when measuring code that intrinsically depends on parallel scheduling
/// (e.g., the parallel bulk-import path).
pub fn bench_runtime() -> tokio::runtime::Runtime {
    let multi = std::env::var("FLUREE_BENCH_RUNTIME")
        .ok()
        .as_deref()
        .map(|s| s.eq_ignore_ascii_case("multi") || s.eq_ignore_ascii_case("multi_thread"))
        .unwrap_or(false);

    if multi {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("failed to construct multi-thread bench runtime")
    } else {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("failed to construct single-thread bench runtime")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn profile_parses_aliases() {
        assert_eq!(
            BenchProfile::from_env_str("quick"),
            Some(BenchProfile::Quick)
        );
        assert_eq!(BenchProfile::from_env_str("Q"), Some(BenchProfile::Quick));
        assert_eq!(BenchProfile::from_env_str("FULL"), Some(BenchProfile::Full));
        assert_eq!(
            BenchProfile::from_env_str("nightly"),
            Some(BenchProfile::Full)
        );
        assert_eq!(BenchProfile::from_env_str("garbage"), None);
    }

    #[test]
    fn scale_parses_aliases() {
        assert_eq!(BenchScale::from_env_str("tiny"), Some(BenchScale::Tiny));
        assert_eq!(BenchScale::from_env_str("XS"), Some(BenchScale::Tiny));
        assert_eq!(BenchScale::from_env_str("smoke"), Some(BenchScale::Tiny));
        assert_eq!(BenchScale::from_env_str("L"), Some(BenchScale::Large));
        assert_eq!(BenchScale::from_env_str("xl"), Some(BenchScale::Large));
        assert_eq!(BenchScale::from_env_str("xxl"), None);
    }

    #[test]
    fn scale_str_round_trip() {
        for s in [
            BenchScale::Tiny,
            BenchScale::Small,
            BenchScale::Medium,
            BenchScale::Large,
        ] {
            assert_eq!(BenchScale::from_env_str(s.as_str()), Some(s));
        }
    }

    #[test]
    fn elements_default_is_monotonic() {
        assert!(BenchScale::Tiny.elements_default() < BenchScale::Small.elements_default());
        assert!(BenchScale::Small.elements_default() < BenchScale::Medium.elements_default());
        assert!(BenchScale::Medium.elements_default() < BenchScale::Large.elements_default());
    }

    #[test]
    fn profile_sample_sizes_diverge() {
        assert!(BenchProfile::Quick.sample_size() < BenchProfile::Full.sample_size());
    }

    #[test]
    fn bench_runtime_constructs() {
        let rt = bench_runtime();
        let result = rt.block_on(async { 1 + 1 });
        assert_eq!(result, 2);
    }
}
