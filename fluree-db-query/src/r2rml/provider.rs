//! R2RML Provider Traits
//!
//! These traits define the interface for loading R2RML mappings and
//! accessing underlying Iceberg tables during query execution.

use crate::error::Result;
use async_trait::async_trait;
use fluree_db_tabular::ColumnBatch;
use futures::stream::Stream;
use std::fmt::Debug;
use std::pin::Pin;
use std::sync::Arc;

/// A streamed sequence of column batches from a table scan.
///
/// Batches arrive as data files are read and decoded, so a consumer that
/// materializes and aggregates incrementally holds only O(in-flight files)
/// in memory instead of the whole table. The stream is `'static` + `Send` so
/// an operator can own it across `next_batch` calls.
pub type ColumnBatchStream = Pin<Box<dyn Stream<Item = Result<ColumnBatch>> + Send + Sync>>;

// Re-export from fluree-db-r2rml for convenience
pub use fluree_db_r2rml::mapping::CompiledR2rmlMapping;

/// Comparison operator for a pushed-down scan filter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanCmpOp {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
}

/// A literal value for a pushed-down scan filter.
///
/// Limited to types that prune safely against Iceberg column min/max bounds and
/// that the Arrow row filter can evaluate: date/int/bool, strings (lexicographic,
/// e.g. equality on a name/code column), plus double and decimal — the last two
/// gated by `FLUREE_ICEBERG_NUMERIC_STATS` and only ever produced from a numeric
/// FILTER predicate (see `to_scan_value`). A missed push is never wrong: the
/// in-engine FILTER remains the authority.
#[derive(Debug, Clone, PartialEq)]
pub enum ScanValue {
    Bool(bool),
    Int(i64),
    /// Days since 1970-01-01 (matches Iceberg date storage).
    Date(i32),
    /// UTF-8 string (byte-lexicographic order matches Parquet stats + xsd:string).
    Str(String),
    /// An `xsd:double`/`xsd:float` value — pushed against a physically-`double`
    /// column only (see `build_iceberg_filter`). NaN bounds never over-prune (the
    /// Iceberg compare treats a NaN operand as incomparable → keep).
    Double(f64),
    /// A decimal value as its unscaled i128 + precision/scale (mirrors
    /// `LiteralValue::Decimal`). Carries the LITERAL's scale; the column's scale
    /// may differ and is normalized during comparison.
    Decimal {
        unscaled: i128,
        precision: u8,
        scale: i8,
    },
    /// A raw column value recovered by reversing a subject template (bound-subject
    /// pushdown). The physical type is unknown here — it is resolved against the
    /// Iceberg field type when the pushdown predicate is built, and the pushdown is
    /// skipped for field types not yet supported. The R2RML operator still enforces
    /// the subject equality, so a skipped or imperfect push is never wrong.
    TemplateKey(String),
}

/// A constant object in a triple pattern (`?s <pred> <const>`), enforced by the
/// R2RML operator so results are correct regardless of scan pushdown.
#[derive(Debug, Clone, PartialEq)]
pub enum ObjectConstant {
    /// A literal object — loose value match (string/integer/boolean/date). Emits
    /// a scan filter for row-group + row pruning in addition to operator enforcement.
    Scalar(ScanValue),
    /// A bound IRI object — exact IRI match, e.g. a reference to a parent entity
    /// (`?s edw:geography <geo/1>`). Compared against the materialized IRI; the
    /// column-level scan filter is not applied to these yet (a FK-key pushdown
    /// needs subject-template reversal), so only the operator enforces them.
    Iri(String),
    /// A decimal / arbitrary-precision integer object — numeric (scale-insensitive)
    /// match, so `9.99` matches a column materialized as `9.990`. Operator-enforced
    /// only (no scan pushdown yet, which would need decimal-aware Iceberg predicates).
    Decimal(bigdecimal::BigDecimal),
    /// A double (xsd:double / xsd:float) object — exact f64 value match.
    /// Operator-enforced only (no scan pushdown yet).
    Double(f64),
}

/// A predicate pushed down to the Iceberg scan for file pruning.
///
/// Resolved to a concrete table column (the R2RML operator maps the query
/// variable → predicate IRI → column). The provider turns this into an Iceberg
/// `Expression` for conservative file-level min/max pruning; the in-engine
/// FILTER still runs, so a missed push is only a perf loss, never wrong results.
#[derive(Debug, Clone)]
pub struct ScanFilter {
    pub column: String,
    pub op: ScanCmpOp,
    pub value: ScanValue,
}

/// A scan-side top-k directive for a single-column **DESCENDING** `ORDER BY …
/// LIMIT k` directly above a single-table R2RML scan (PR-5). The scan reads files
/// in `upper_bound(sort_column)`-DESC order, keeps a running k-th bound, and stops
/// once no unread file can beat it — reading far fewer than the whole table.
///
/// A pure perf optimization: the scan still streams a strict SUPERSET of the true
/// top-k (it only skips files that provably cannot contribute), and the
/// authoritative `SortOperator` above applies the exact (compound) order + LIMIT.
/// Ignored by the provider unless `sort_column` resolves to a pushable scalar
/// column of the scanned table.
#[derive(Debug, Clone, PartialEq)]
pub struct ScanTopK {
    /// The primary DESC sort column (an R2RML-mapped table column name).
    pub sort_column: String,
    /// How many top rows the bound must retain — the query's `LIMIT + OFFSET`.
    pub k: usize,
}

/// Provider for compiled R2RML mappings.
///
/// This trait is used by the R2RML operator to load mappings at query time.
/// Implementations typically consult the nameservice graph source records and cache
/// compiled mappings.
///
/// Note: Uses `?Send` for compatibility with Iceberg storage layer which
/// is designed for WASM compatibility. The query engine handles this by
/// executing R2RML operations within a single task.
#[async_trait]
pub trait R2rmlProvider: Debug + Send + Sync {
    /// Check if a graph source has an R2RML mapping.
    ///
    /// This is a lightweight check that doesn't load the full mapping.
    /// Used by GraphOperator to determine if patterns should be rewritten
    /// to R2RML scans.
    ///
    /// # Arguments
    ///
    /// * `graph_source_id` - The graph source alias (e.g., "openflights-gs:main")
    ///
    /// # Returns
    ///
    /// `true` if the graph source exists and has an R2RML mapping, `false` otherwise.
    async fn has_r2rml_mapping(&self, graph_source_id: &str) -> bool;

    /// Get the compiled R2RML mapping for a graph source alias.
    ///
    /// # Arguments
    ///
    /// * `graph_source_id` - The graph source alias (e.g., "openflights-gs:main")
    /// * `as_of_t` - The transaction time for time-travel queries.
    ///
    /// In dataset (multi-ledger) mode, there is no meaningful "dataset t".
    /// Callers should pass `None` unless the query provides an unambiguous
    /// as-of anchor.
    ///
    /// # Returns
    ///
    /// The compiled mapping, or an error if the graph source doesn't exist or
    /// the mapping couldn't be loaded.
    async fn compiled_mapping(
        &self,
        graph_source_id: &str,
        as_of_t: Option<i64>,
    ) -> Result<Arc<CompiledR2rmlMapping>>;
}

/// Provider for scanning Iceberg tables underlying R2RML graph sources.
///
/// This trait is separated from `R2rmlProvider` to allow different
/// implementations for mapping loading vs table access. In practice,
/// both may be implemented by the same struct.
///
/// Note: Uses `?Send` for compatibility with Iceberg storage layer which
/// is designed for WASM compatibility. The query engine handles this by
/// executing R2RML operations within a single task.
#[async_trait]
pub trait R2rmlTableProvider: Debug + Send + Sync {
    /// Scan an Iceberg table and return column batches.
    ///
    /// # Arguments
    ///
    /// * `graph_source_id` - The graph source alias
    /// * `table_name` - The logical table name from the R2RML mapping
    /// * `projection` - Column names to project (for pushdown)
    /// * `as_of_t` - Transaction time for snapshot selection.
    ///
    /// In dataset (multi-ledger) mode, there is no meaningful "dataset t".
    /// Callers should pass `None` unless the query provides an unambiguous
    /// as-of anchor.
    ///
    /// # Returns
    ///
    /// A [`ColumnBatchStream`] yielding column batches as data files are read,
    /// so a streaming consumer never holds the whole table in memory.
    /// `filters` are conservative pushdown predicates (resolved to columns) for
    /// Iceberg file pruning. Implementations may ignore them (correctness is
    /// preserved by the in-engine FILTER) but honoring them skips data files.
    /// `topk`, when set, is a single-column DESC `ORDER BY … LIMIT` directive
    /// (PR-5): the implementation MAY read only the files that can hold the top-k
    /// and stream a superset of them; ignoring it is always correct (the sort
    /// above is authoritative).
    async fn scan_table(
        &self,
        graph_source_id: &str,
        table_name: &str,
        projection: &[String],
        filters: &[ScanFilter],
        topk: Option<&ScanTopK>,
        as_of_t: Option<i64>,
    ) -> Result<ColumnBatchStream>;

    /// The table's exact live row count from Iceberg manifest metadata — **when,
    /// and only when, it provably equals a full-scan count** of the rows a bare
    /// `COUNT(*)` would produce. Lets the fused-aggregate COUNT shortcut answer
    /// from the manifest `record_count` sum instead of decoding every data file.
    ///
    /// `non_null_cols` are the columns that must be non-null for a row to be
    /// counted (the subject-template key columns + any projected object columns).
    /// Returns `Some(n)` only if: (1) the snapshot carries **no delete
    /// manifests** (a merge-on-read delete would make the record_count sum an
    /// over-count), and (2) **every** `non_null_col` is provably zero-null from
    /// the manifest stats — an absent/unknown null count is treated as unknown,
    /// NOT zero. Otherwise returns `Ok(None)` and the caller falls back to the
    /// scan (which is delete/null-correct). The default is `Ok(None)`, so a
    /// provider without manifest metadata (or a non-Iceberg source) always falls
    /// back to the scan.
    async fn table_row_count(
        &self,
        graph_source_id: &str,
        table_name: &str,
        non_null_cols: &[String],
        as_of_t: Option<i64>,
    ) -> Result<Option<u64>> {
        let _ = (graph_source_id, table_name, non_null_cols, as_of_t);
        Ok(None)
    }

    /// Warm the per-query catalog session + caches for a known set of tables
    /// CONCURRENTLY, so a following *serial* scan loop (which resolves one table
    /// per `scan_table`) overlaps the per-table `loadTable` GETs instead of
    /// summing them. Side-effect-only: returns nothing, and a resolution failure
    /// here MUST be swallowed by the implementation — the real scan re-resolves
    /// and surfaces any error — so this only ever removes latency, never changes
    /// results or error behavior. Callers gate it on
    /// [`super::parallel_catalog_resolution_enabled`]. The default is a no-op (a
    /// provider without a remote catalog has nothing to warm).
    async fn prefetch_tables(&self, graph_source_id: &str, table_names: &[String]) {
        let _ = (graph_source_id, table_names);
    }
}

// =============================================================================
// No-Op Providers (for when GraphSourcePublisher isn't available)
// =============================================================================

/// A no-op R2RML provider that always returns false/errors.
///
/// This is used when the nameservice doesn't support GraphSourcePublisher,
/// allowing queries to execute without R2RML support. If R2RML features are
/// actually needed, an error will be returned.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoOpR2rmlProvider;

impl NoOpR2rmlProvider {
    /// Create a new no-op provider.
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl R2rmlProvider for NoOpR2rmlProvider {
    async fn has_r2rml_mapping(&self, _graph_source_id: &str) -> bool {
        // Always return false - no R2RML mappings available
        false
    }

    async fn compiled_mapping(
        &self,
        graph_source_id: &str,
        _as_of_t: Option<i64>,
    ) -> Result<Arc<CompiledR2rmlMapping>> {
        Err(crate::error::QueryError::Internal(format!(
            "R2RML provider not available for graph source '{graph_source_id}'. \
             This Fluree instance does not support graph source operations."
        )))
    }
}

#[async_trait]
impl R2rmlTableProvider for NoOpR2rmlProvider {
    async fn scan_table(
        &self,
        graph_source_id: &str,
        _table_name: &str,
        _projection: &[String],
        _filters: &[ScanFilter],
        _topk: Option<&ScanTopK>,
        _as_of_t: Option<i64>,
    ) -> Result<ColumnBatchStream> {
        Err(crate::error::QueryError::Internal(format!(
            "R2RML table scanning not available for graph source '{graph_source_id}'. \
             This Fluree instance does not support graph source operations."
        )))
    }
}
