//! R2RML Graph Source Support
//!
//! This module provides query integration for R2RML-mapped Iceberg tables.
//! It exposes tabular data as RDF triples through the query engine.
//!
//! # Architecture
//!
//! - `R2rmlProvider`: Trait for loading compiled R2RML mappings
//! - `R2rmlPattern`: IR pattern for R2RML queries
//! - `R2rmlScanOperator`: Operator that executes R2RML scans
//! - `rewrite_patterns_for_r2rml`: Rewrites triple patterns to R2RML patterns
//!
//! # Usage
//!
//! R2RML patterns are typically generated during query planning when the
//! planner detects that a triple pattern can be satisfied by an R2RML
//! graph source. The operator loads the mapping, scans the underlying
//! Iceberg table, and materializes RDF terms according to the mapping.
//!
//! When a GRAPH pattern targets an R2RML graph source, the `GraphOperator`
//! uses `rewrite_patterns_for_r2rml` to convert contained triple patterns
//! to R2RML patterns before building the operator tree.

mod fused_aggregate;
mod operator;
mod provider;
mod rewrite;

pub use fused_aggregate::{detect_fused_r2rml_aggregate, FusedR2rmlAggregateOperator};
pub use operator::R2rmlScanOperator;
pub use provider::{
    ColumnBatchStream, NoOpR2rmlProvider, ObjectConstant, R2rmlProvider, R2rmlTableProvider,
    ScanCmpOp, ScanFilter, ScanValue,
};
pub use rewrite::{
    convert_triple_to_r2rml, rewrite_patterns_for_r2rml, unsupported_subscope_error,
    R2rmlRewriteResult,
};

/// Read an on/off environment switch that defaults to **on**. Only `0`, `false`,
/// `off`, or `no` (trimmed, case-insensitive) disable it — the single falsy
/// spelling set for the whole R2RML switch family, so individual switches can't
/// drift. (`env_flag_enabled` in `fluree-db-api`'s `graph_source::crawl` and the
/// `FLUREE_ICEBERG_FOOTER_FROM_CACHE` switch in `fluree-db-iceberg` mirror these
/// spellings; they can't share this symbol across the crate boundary.) Call
/// sites cache the result in a per-switch `OnceLock` — set switches at process
/// startup, not per query.
pub(crate) fn env_switch_enabled(name: &str) -> bool {
    match std::env::var(name) {
        Ok(v) => !matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "0" | "false" | "off" | "no"
        ),
        Err(_) => true,
    }
}

/// Whether a multi-table query may warm its per-table catalog contexts
/// (`loadTable` GET + metadata) CONCURRENTLY before the serial scan loop, so the
/// per-table GETs overlap instead of summing (PR-8 slice 1). Default on;
/// `FLUREE_R2RML_PARALLEL_CATALOG=0|false|off|no` restores serial resolution.
/// Cached in a `OnceLock` — set at process startup, not per query.
pub(crate) fn parallel_catalog_resolution_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| env_switch_enabled("FLUREE_R2RML_PARALLEL_CATALOG"))
}
