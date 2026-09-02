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
pub(crate) mod policy;
mod provider;
mod rewrite;

pub use fused_aggregate::{detect_fused_r2rml_aggregate, FusedR2rmlAggregateOperator};
pub use operator::{R2rmlParentMemo, R2rmlScanOperator};
pub use provider::{
    ColumnBatchStream, NoOpR2rmlProvider, ObjectConstant, R2rmlProvider, R2rmlTableProvider,
    ScanCmpOp, ScanFilter, ScanTopK, ScanValue, TableWatermark,
};
pub use rewrite::{
    convert_triple_to_r2rml, r2rml_unsupported_pattern_error, rewrite_patterns_for_r2rml,
    unsupported_outside_graph_scopes, unsupported_subscope_error, R2rmlRewriteResult,
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

/// F17: whether `UnionOperator` and `BindOperator` forward a top-of-tree `LIMIT`
/// row budget down toward the scan (UNION to each branch — a single branch may
/// supply all `k` rows; BIND straight to its child, being 1:1/order-preserving).
/// Both forwards are categorically sound; the switch exists for differential
/// hygiene, so an OFF run is byte-identical to pre-F17. Default on;
/// `FLUREE_R2RML_UNION_BUDGET=0|false|off|no` disables. Read once (process-wide).
pub(crate) fn union_budget_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| env_switch_enabled("FLUREE_R2RML_UNION_BUDGET"))
}

/// T1.3: whether `DatasetOperator` forwards a top-of-tree `LIMIT` row budget /
/// `ORDER BY … LIMIT` top-k into each member's inner subtree. Every chat SPARQL is
/// executed as a `FROM <ledger>` dataset query (the single-view path rejects
/// SPARQL), so it routes through `DatasetOperator`; without forwarding, a bare
/// `LIMIT 20` never reaches the R2RML scan and the whole table is materialized.
/// Mirrors `GraphOperator`'s wrapper forwarding (the same directive-threading
/// pattern) — the dataset wrapper was the one wrapper on that path that lacked it.
/// Categorically sound: the consuming `LIMIT` truncates the member concatenation
/// to `budget`, and each member's own `Sort`/`Distinct` still absorb the budget
/// (no-op) where present, so this only removes the wrapper's artificial block. The
/// switch exists for differential hygiene: OFF is byte-identical to pre-T1.3.
/// Default on; `FLUREE_R2RML_DATASET_BUDGET=0|false|off|no` disables. Read once.
pub(crate) fn dataset_budget_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| env_switch_enabled("FLUREE_R2RML_DATASET_BUDGET"))
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

/// Whether numeric (double / decimal) FILTER predicates may be pushed to the
/// Iceberg scan for file / row-group pruning (PR-7). Default on;
/// `FLUREE_ICEBERG_NUMERIC_STATS=0|false|off|no` reverts to leaving them with the
/// in-engine FILTER only, independently of the shipped int/date/string pushdown.
/// Gating at the single push site (`to_scan_value`) keeps the iceberg-side
/// widening inert when off — no numeric `LiteralValue` is ever produced, so the
/// new `stat_bounds` arms and FLBA-decimal relax are never exercised. Cached in a
/// `OnceLock` — set at process startup, not per query.
pub(crate) fn iceberg_numeric_stats_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| env_switch_enabled("FLUREE_ICEBERG_NUMERIC_STATS"))
}

/// Item 10 (F-AUD-11): whether `xsd:dateTime` FILTER predicates may be pushed to
/// the Iceberg scan for MANIFEST-level file pruning (mirrors
/// [`iceberg_numeric_stats_enabled`]). Default on;
/// `FLUREE_ICEBERG_TIMESTAMP_STATS=0|false|off|no` reverts to leaving them with the
/// in-engine FILTER only (no timestamp `ScanValue` is produced, so the iceberg-side
/// timestamp arms stay inert). Gating at the single push site (`to_scan_value`)
/// keeps the widening inert when off. Cached in a `OnceLock` — set at process
/// startup, not per query.
pub(crate) fn iceberg_timestamp_stats_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| env_switch_enabled("FLUREE_ICEBERG_TIMESTAMP_STATS"))
}

/// Item 7 (F-AUD-5): whether a bounded `FILTER … IN (…)` or single-var `VALUES`
/// set over a scalar-column object var is lowered to an Iceberg `Expression::In`
/// for file / row-group pruning (the backend already evaluates `In`; nothing
/// emitted it). Default on; `FLUREE_R2RML_IN_PUSHDOWN=0|false|off|no` reverts to
/// leaving the set to the in-engine FILTER / VALUES join (a full FACT scan). The
/// push is a strict superset (keeps a file iff any member could be in range), so
/// OFF is byte-identical in results, only slower. Read once (process-wide).
pub(crate) fn in_pushdown_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| env_switch_enabled("FLUREE_R2RML_IN_PUSHDOWN"))
}

/// Item 11 (F-AUD-7): whether `OPTIONAL` (left-outer-join) forwards a top-of-tree
/// `LIMIT` row budget to its REQUIRED (outer) side. Sound because each required
/// row yields ≥1 output row (matched or padded-with-null), so bounding the
/// required side to `k` still produces ≥`k` output for the `LIMIT` to truncate —
/// the inner/optional side is NOT budgeted (it must produce every match for a
/// given required row). This closes probe-04's 68,828× read amplification. Default
/// on; `FLUREE_R2RML_BUDGET_OPTIONAL=0|false|off|no` reverts to swallowing the
/// budget (byte-identical results, just the full outer scan). Read once.
pub(crate) fn optional_budget_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| env_switch_enabled("FLUREE_R2RML_BUDGET_OPTIONAL"))
}

/// D7 (Cluster E / E1): whether a variable-predicate R2RML "crawl wildcard" scan
/// with NO pruning key (no bound subject, no `class_filter`, no `class_prune_hint`,
/// no pinned `triples_map_iri`) — which resolves to EVERY TriplesMap, i.e. a
/// full-source scan — is cost-estimated as [`crate::planner`]'s `FULL_SCAN` so
/// `reorder_patterns` places it LAST among the query's co-subject R2RML scans.
/// That makes the wildcard the LIMIT-budgeted, correlated OUTER scan driven by the
/// selective INNER scan (the property-scoped browse crawl `{?s <prop> ?v}` +
/// `{?s ?p ?o}`), instead of the UNBUDGETED inner full-source scan it defaults to
/// when both scans estimate equal (both `DEFAULT_PROPERTY_SCAN_SELECTIVITY`) and
/// reorder keeps emit order — the D7 DNF. SOUND: reordering two co-subject scans
/// preserves the solution set (the top LIMIT truncates the same rows; every
/// driving subject has ≥1 triple so the budget never under-fills); it only lets
/// the existing budget reach a scan that then terminates. Default on;
/// `FLUREE_R2RML_BUDGET_PROPERTY_VAR=0|false|off|no` reverts to the equal estimate
/// (byte-identical results, the property-var browse then runs unbudgeted). Read
/// once (process-wide).
pub(crate) fn property_var_budget_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| env_switch_enabled("FLUREE_R2RML_BUDGET_PROPERTY_VAR"))
}

/// Item 8 (F-AUD-6): whether an ASCENDING single-column `ORDER BY … LIMIT k` over
/// an R2RML scan may offer a scan-side top-k directive. DESC top-k (PR-5) is
/// always offered; ASC is admitted only for a REQUIRED (non-nullable) column (the
/// provider re-checks — SPARQL orders unbound values first under ASC). Default on;
/// `FLUREE_R2RML_TOPK_ASC=0|false|off|no` reverts to the pre-item-8 DESC-only
/// behavior (byte-identical: an ASC sort then full-scans + sorts). Read once.
pub(crate) fn topk_asc_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| env_switch_enabled("FLUREE_R2RML_TOPK_ASC"))
}

/// Item 7 (F-AUD-5): the maximum set size lowered to `Expression::In`. A larger
/// set declines the pushdown (stays with the in-engine FILTER / VALUES join) so
/// manifest evaluation stays cheap — every member is bound-checked against every
/// candidate file, i.e. O(files × members). Default 64; override with
/// `FLUREE_R2RML_IN_PUSHDOWN_MAX=<n>` (a non-numeric or zero value falls back to
/// the default). Read once (process-wide).
pub(crate) fn in_pushdown_max() -> usize {
    static MAX: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *MAX.get_or_init(|| {
        std::env::var("FLUREE_R2RML_IN_PUSHDOWN_MAX")
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
            .filter(|n| *n > 0)
            .unwrap_or(64)
    })
}
