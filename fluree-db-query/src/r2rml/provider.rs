//! R2RML Provider Traits
//!
//! These traits define the interface for loading R2RML mappings and
//! accessing underlying Iceberg tables during query execution.

use crate::error::Result;
use async_trait::async_trait;
use fluree_db_tabular::ColumnBatch;
use std::fmt::Debug;
use std::sync::Arc;

// Re-export from fluree-db-r2rml for convenience
pub use fluree_db_r2rml::mapping::CompiledR2rmlMapping;

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
    /// An iterator/stream of column batches. The exact streaming mechanism
    /// depends on the implementation.
    async fn scan_table(
        &self,
        graph_source_id: &str,
        table_name: &str,
        projection: &[String],
        as_of_t: Option<i64>,
    ) -> Result<Vec<ColumnBatch>>;
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
        _as_of_t: Option<i64>,
    ) -> Result<Vec<ColumnBatch>> {
        Err(crate::error::QueryError::Internal(format!(
            "R2RML table scanning not available for graph source '{graph_source_id}'. \
             This Fluree instance does not support graph source operations."
        )))
    }
}
