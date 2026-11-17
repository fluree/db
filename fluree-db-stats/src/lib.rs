//! Mergeable statistical sketches and column profiles.
//!
//! One kernel for every place Fluree summarises a column of values: the
//! lake-table mapper probing a key, the quality assessor looking for a
//! per-group outlier, the resolver weighing how common a value is. Each
//! summary here is **mergeable**: two profiles built over disjoint shards
//! combine into the profile of the union, exactly for counts and moments,
//! within stated error for the sketches. That property is what lets one
//! implementation run over an Arrow row group, a ledger property at a
//! pinned `t`, or a resolution run's records, and fold the pieces however
//! the caller sharded them.
//!
//! | Need | Sketch | Error |
//! |---|---|---|
//! | distinct values | [`Hll`] | ~1.04/√m relative, m = 2^precision |
//! | mean / variance / min / max / sum | [`Moments`] | exact |
//! | median, percentiles | [`TDigest`] | tight at the tails, ~1% mid-range |
//! | most frequent values, prevalence | [`HeavyHitters`] | exact when distinct ≤ capacity, else bounded undercount |
//!
//! [`ColumnProfile`] composes the four over one column; [`GroupedProfile`]
//! keeps one per group key with bounded group count, which is the
//! per-(part, division) baseline a SHACL rule cannot express. The
//! [`findings`] module turns profiles into the statistics a quality rule
//! evaluates: baselines, concentration, constancy per group, scale drift.
//!
//! Nothing here reads storage or knows what a ledger is. Ingestion faces
//! live beside the sources they read.

pub mod findings;
pub mod grouped;
pub mod hash;
pub mod heavy_hitters;
pub mod hll;
pub mod moments;
pub mod profile;
#[cfg(feature = "tabular")]
pub mod tabular;
pub mod tdigest;

pub use grouped::{GroupSummary, GroupedProfile, GroupedSummary};
pub use hash::value_hash;
pub use heavy_hitters::{HeavyHitters, HitCount};
pub use hll::{Hll, Hll256, Hll4096};
pub use moments::Moments;
pub use profile::{
    ColumnProfile, ColumnSummary, NumericSummary, ProfileConfig, ProfileValue, TextSummary,
    TopValue, ValueKind,
};
pub use tdigest::TDigest;
