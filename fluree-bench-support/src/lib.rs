//! Shared chassis for Fluree DB benchmarks.
//!
//! This crate is a `dev-dependency` of every `fluree-db-*` crate that hosts
//! benches. It exists so the duplicated patterns we observed across the five
//! pre-2026-05 benches — verbatim `init_tracing_for_bench()` blocks, atomic
//! ledger-alias counters, hand-rolled type aliases, header comment styles —
//! can live in one place. New benches start from a template
//! (`fluree-bench-support/templates/BENCH_TEMPLATE.rs`) that uses these
//! helpers, so contributors don't have to rediscover the patterns.
//!
//! See:
//! - `BENCHMARKING.md` (workspace root) — orientation: what benches exist,
//!   how to run them, env vars.
//! - `docs/contributing/benches.md` — the deep guide for adding a new bench.
//! - `fluree-bench-support/README.md` — API reference for this crate.
//! - `.claude/proposed-work/docs/plan-benchmark-infrastructure.md` §4.5 —
//!   the design rationale for why the chassis exists.
//!
//! ## What's here
//!
//! - [`tracing`] — opt-in tracing init via `FLUREE_BENCH_TRACING=1`.
//! - [`runtime`] — `bench_runtime()`, `BenchProfile`, `BenchScale` driven by
//!   `FLUREE_BENCH_PROFILE` and `FLUREE_BENCH_SCALE`.
//! - [`ledger`] — `next_ledger_alias()` for atomic unique-alias generation.
//! - [`gen`] — deterministic data generators reused across benches.
//! - [`fixtures`] — vendored / fetched fixture loaders.
//! - [`budget`] — regression-budget loader and validator.
//! - [`report`] — opt-in human-readable end-of-run summary tables.

#![forbid(unsafe_code)]

pub mod budget;
pub mod fixtures;
pub mod gen;
pub mod ledger;
pub mod report;
pub mod runtime;
pub mod tracing;

// Convenience re-exports for the common case. A bench that only needs the
// most-used helpers can `use fluree_bench_support::{init_tracing_for_bench,
// bench_runtime, current_profile, current_scale, next_ledger_alias,
// BenchProfile, BenchScale};` without touching individual modules.
pub use ledger::next_ledger_alias;
pub use runtime::{bench_runtime, current_profile, current_scale, BenchProfile, BenchScale};
pub use tracing::init_tracing_for_bench;
