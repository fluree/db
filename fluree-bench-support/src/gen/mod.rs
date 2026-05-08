//! Deterministic data generators reused across benches.
//!
//! Each submodule lifts a generator that was previously hand-rolled inside
//! one of the pre-2026-05 benches. Lifting them means new benches don't have
//! to reimplement the same realistic-looking-but-deterministic data shapes,
//! and results are comparable across benches that share a generator.
//!
//! ## What's here
//!
//! - [`people`] — Person + Company graph (lifted from `insert_formats.rs`).
//!   Used by insert benchmarks and any bench that wants a moderately-sized
//!   linked-data graph with refs and scalars.
//! - [`vectors`] — random `f64` vectors of arbitrary dimension. Two flavors:
//!   deterministic-from-seed (no RNG state — used by micro-benches) and
//!   RNG-driven (used by end-to-end vector benches).
//! - [`corpora`] — paragraph-length text + vocabulary. Used by full-text
//!   benchmarks.
//!
//! ## Determinism contract
//!
//! Every generator in this module produces **byte-identical output** across
//! runs given the same parameters. This is non-negotiable: bench
//! reproducibility and regression-budget validity both depend on it. Tests
//! in each submodule pin the contract.

pub mod corpora;
pub mod people;
pub mod vectors;
