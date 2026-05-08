//! Bench-time tracing setup.
//!
//! Replaces the 18-line `init_tracing_for_bench()` block that was duplicated
//! verbatim across `insert_formats.rs`, `vector_query.rs`, and
//! `fulltext_query.rs` before this chassis existed. Same opt-in semantics:
//! tracing is **off** unless `FLUREE_BENCH_TRACING` is set, so PR-gated runs
//! keep wall-clock measurements clean.
//!
//! ## Usage
//!
//! ```rust,ignore
//! use fluree_bench_support::init_tracing_for_bench;
//!
//! fn bench_main(c: &mut Criterion) {
//!     init_tracing_for_bench();
//!     // ... rest of the bench ...
//! }
//! ```
//!
//! ## Env vars recognized
//!
//! | Var | Effect |
//! |---|---|
//! | unset / not `1`+ | Tracing **off**. No subscriber installed. Zero overhead. |
//! | `FLUREE_BENCH_TRACING=1` | Install a stderr subscriber filtered by `RUST_LOG` (defaults to `info` if `RUST_LOG` is unset). |
//! | `FLUREE_BENCH_TRACING=file:./out.json` | (Reserved for `BenchSpanLayer`; see TODO note below.) |
//!
//! The crate-level `Targets` filter from `fluree-db-server::telemetry` is not
//! invoked here because benches typically run only one or two crates at DEBUG
//! and the `RUST_LOG` env-filter shape is more flexible.

use std::sync::OnceLock;

static INIT: OnceLock<()> = OnceLock::new();

/// Install a tracing subscriber for the current bench process if
/// `FLUREE_BENCH_TRACING` is set. Idempotent — safe to call from every
/// `bench_*` entry point.
///
/// **Off by default.** Calling this without `FLUREE_BENCH_TRACING=1` is a
/// no-op so PR-gated wall-clock numbers are not polluted by tracing overhead.
pub fn init_tracing_for_bench() {
    INIT.get_or_init(|| {
        match std::env::var("FLUREE_BENCH_TRACING").ok().as_deref() {
            Some("1") => install_stderr_subscriber(),
            Some(other) if other.starts_with("file:") => {
                // TODO(bench-3): wire up `BenchSpanLayer` here. For now, fall back
                // to stderr so users get *something* when they ask for a file.
                install_stderr_subscriber();
                ::tracing::warn!(
                    target = %other,
                    "FLUREE_BENCH_TRACING=file:... is reserved for the BenchSpanLayer; \
                     emitting to stderr instead until that layer ships"
                );
            }
            _ => { /* tracing off: zero overhead */ }
        }
    });
}

fn install_stderr_subscriber() {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "info");
    }
    let filter = tracing_subscriber::EnvFilter::from_default_env();
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .with_level(true)
        .try_init();
}

// ---------------------------------------------------------------------------
// BenchSpanLayer (skeleton)
// ---------------------------------------------------------------------------

/// `tracing_subscriber::Layer` that captures span open/close events with
/// monotonic timestamps for later analysis.
///
/// **Status:** skeleton. The full implementation lands in a later commit
/// (see plan §5.2 item 1 + §4.5.2 docs). This stub exists so the public
/// surface is stable from `bench-1`; benches can reference it but should
/// not yet rely on it producing useful output.
///
/// Activated via `FLUREE_BENCH_TRACING=file:./out.json` once implemented;
/// today it is a no-op layer.
#[derive(Debug, Default, Clone, Copy)]
pub struct BenchSpanLayer;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn init_tracing_is_idempotent() {
        // Don't set the env var; this should be a no-op.
        init_tracing_for_bench();
        init_tracing_for_bench();
        init_tracing_for_bench();
        // No assertion — we're just confirming repeated calls don't panic.
    }
}
