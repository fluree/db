//! Graph Source Operations
//!
//! This module provides APIs for creating, managing, and querying graph sources.
//! Graph sources are derived indexes built from ledger data, such as BM25 full-text
//! search indexes.
//!
//! # Key Concepts
//!
//! - **Graph Source**: A derived index built from one or more source ledgers
//! - **BM25 Index**: A full-text search index using the BM25 scoring algorithm
//! - **Watermark**: The transaction time (`t`) up to which the index has been synced
//! - **Property Dependencies**: IRIs of properties that trigger reindexing when changed
//!
//! # BM25 Full-Text Search
//!
//! ## Creating an Index
//!
//! ```ignore
//! use fluree_db_api::{Fluree, Bm25CreateConfig};
//! use serde_json::json;
//!
//! let config = Bm25CreateConfig::new(
//!     "my-search",  // Graph source name
//!     "docs:main",  // Source ledger
//!     json!({
//!         "@context": {"ex": "http://example.org/"},
//!         "where": [{"@id": "?x", "@type": "ex:Article"}],
//!         "select": {"?x": ["@id", "ex:title", "ex:content"]}
//!     }),
//! )
//! .with_k1(1.2)   // Optional: term frequency saturation
//! .with_b(0.75);  // Optional: document length normalization
//!
//! let result = fluree.create_full_text_index(config).await?;
//! println!("Created index with {} documents at t={}", result.doc_count, result.index_t);
//! ```
//!
//! ## Querying
//!
//! Use `f:*` properties (with `"f": "https://ns.flur.ee/db#"` in `@context`) in your query's where clause:
//!
//! ```json
//! {
//!   "where": [{
//!     "f:graphSource": "my-search:main",
//!     "f:searchText": "rust programming",
//!     "f:searchResult": {
//!       "f:resultId": "?doc",
//!       "f:resultScore": "?score",
//!       "f:resultLedger": "?source"
//!     }
//!   }],
//!   "select": ["?doc", "?score"],
//!   "orderBy": [{"var": "?score", "order": "desc"}]
//! }
//! ```
//!
//! ## Syncing (Maintenance)
//!
//! Keep indexes up to date with ledger changes:
//!
//! ```ignore
//! // Manual sync to catch up with ledger head
//! let result = fluree.sync_bm25_index("my-search:main").await?;
//! println!("Synced {} documents", result.upserted);
//!
//! // Check staleness without syncing
//! let check = fluree.check_bm25_staleness("my-search:main").await?;
//! if check.is_stale {
//!     println!("Index is {} commits behind", check.lag);
//! }
//!
//! // Load with automatic sync (on-query catch-up)
//! let (index, sync_result) = fluree.load_bm25_index_with_sync("my-search:main", true).await?;
//! ```
//!
//! ## Time-Travel Queries
//!
//! Query at a specific historical time:
//!
//! ```ignore
//! // Sync to a specific t (for time-travel queries)
//! let result = fluree.sync_bm25_index_to("my-search:main", target_t, Some(5000)).await?;
//!
//! // Use FlureeIndexProvider for query execution
//! let provider = FlureeIndexProvider::new(&fluree);
//! let mut ctx = ExecutionContext::new(&db, &vars);
//! ctx.to_t = target_t;  // Time-travel target
//! ctx.bm25_provider = Some(&provider);
//! ```
//!
//! ## Multi-Ledger Support
//!
//! BM25 indexes support multiple source ledgers with per-ledger watermarks:
//!
//! - Same IRI in different ledgers = distinct documents (keyed by ledger alias + IRI)
//! - `effective_t()` = minimum watermark across all source ledgers
//! - Use `f:resultLedger` binding to disambiguate results in joins

/// Refusal for a `@t:` / `@commit:` pin on a graph source: those name Fluree
/// ledger states, which a virtual source has none of. The refusal names the
/// pins a source does honor; a pin must never be silently answered from the
/// source's current state.
pub(crate) const GRAPH_SOURCE_LEDGER_TIME_UNSUPPORTED: &str =
    "Graph sources have no transaction numbers or commit hashes. Pin the source's \
     table state with @time:<timestamp>, @recorded:<timestamp>, or @snapshot:<id>, \
     or remove the time specification to query at latest.";

/// Refusal for `@snapshot:` on a native ledger, which has no table snapshots.
pub(crate) const SNAPSHOT_SPEC_ON_LEDGER: &str =
    "@snapshot: selects a graph source's table snapshot; a ledger is addressed \
     with @t:, @time:, @recorded:, or @commit:.";

/// The table state a time-specified graph-source alias reads, or `None` for
/// its current state. `@time:` and `@recorded:` coincide: a table snapshot has
/// one time, the writer's commit time, and no separate event axis (the same
/// rule as a ledger that never used caller-supplied event times).
pub(crate) fn source_time_for(
    spec: &crate::TimeSpec,
) -> crate::Result<Option<fluree_db_query::r2rml::SourceTime>> {
    use fluree_db_query::r2rml::SourceTime;
    match spec {
        crate::TimeSpec::Latest => Ok(None),
        crate::TimeSpec::AtSnapshot(id) => Ok(Some(SourceTime::SnapshotId(*id))),
        crate::TimeSpec::AtTime(iso) | crate::TimeSpec::AtRecorded(iso) => {
            // Snapshot times are whole milliseconds and the selection is
            // "committed at or before", so sub-millisecond precision floors
            // (the ledger resolvers ceiling; see `iso_to_target_epoch_ms`).
            let dt = crate::time_resolve::parse_time_travel_iso(iso)?;
            Ok(Some(SourceTime::AsOfTimestampMs(dt.timestamp_millis())))
        }
        crate::TimeSpec::AtT(_) | crate::TimeSpec::AtCommit(_) => Err(
            crate::ApiError::invalid_query(GRAPH_SOURCE_LEDGER_TIME_UNSUPPORTED),
        ),
    }
}

/// Push every time-specified graph-source view's pin into the query's R2RML
/// table provider before execution. One call site per execution route, so a
/// route cannot run a pinned view against the source's current state.
///
/// A pin is per source for the whole query, so one source read at two states
/// — including pinned in one view and unpinned in another — is refused rather
/// than letting the pin decide for the unpinned view.
pub(crate) fn pin_graph_source_times<'v>(
    views: impl IntoIterator<Item = &'v crate::view::GraphDb>,
    table_provider: &dyn fluree_db_query::r2rml::R2rmlTableProvider,
) -> fluree_db_query::error::Result<()> {
    use fluree_db_query::r2rml::SourceTime;
    let mut times: std::collections::HashMap<&str, Option<SourceTime>> =
        std::collections::HashMap::new();
    for view in views {
        let Some(gs_id) = view.graph_source_id.as_deref() else {
            continue;
        };
        match times.insert(gs_id, view.graph_source_time) {
            Some(prior) if prior != view.graph_source_time => {
                return Err(fluree_db_query::QueryError::InvalidQuery(format!(
                    "graph source '{gs_id}' is read at two different states in one query \
                     ({}, {}); pin every reference of a source the same way",
                    describe_source_time(prior),
                    describe_source_time(view.graph_source_time),
                )));
            }
            _ => {}
        }
    }
    for (gs_id, time) in times {
        if let Some(time) = time {
            table_provider.pin_source_time(gs_id, time)?;
        }
    }
    Ok(())
}

fn describe_source_time(time: Option<fluree_db_query::r2rml::SourceTime>) -> String {
    match time {
        None => "latest".to_string(),
        Some(fluree_db_query::r2rml::SourceTime::SnapshotId(id)) => format!("@snapshot:{id}"),
        Some(fluree_db_query::r2rml::SourceTime::AsOfTimestampMs(ms)) => {
            format!("@time:{}", crate::time_resolve::epoch_ms_to_iso(ms))
        }
    }
}

// Internal modules
mod bm25;
mod cache;
mod config;
mod helpers;
mod provider;
mod result;

#[cfg(feature = "vector")]
mod vector;

#[cfg(feature = "iceberg")]
mod catalog_session;

#[cfg(feature = "iceberg")]
mod disk_catalog_cache;

#[cfg(feature = "iceberg")]
mod lazy_storage;

#[cfg(feature = "iceberg")]
pub(crate) mod crawl;

#[cfg(feature = "iceberg")]
pub(crate) mod r2rml;

#[cfg(feature = "iceberg")]
mod iceberg_catalog;

#[cfg(feature = "iceberg")]
mod iceberg_sample;

#[cfg(feature = "iceberg")]
mod iceberg_generate;

#[cfg(feature = "iceberg")]
mod iceberg_validate;

#[cfg(feature = "iceberg")]
mod ephemeral;

#[cfg(feature = "iceberg")]
mod r2rml_materialize;

#[cfg(feature = "sql")]
mod sql;

#[cfg(feature = "sql")]
pub use sql::{SqlCheckResult, SqlCreateConfig, SqlCreateResult};

// Re-export configuration types
pub use config::Bm25CreateConfig;

#[cfg(feature = "vector")]
pub use config::VectorCreateConfig;

#[cfg(feature = "iceberg")]
pub use config::{CatalogMode, IcebergConnectionConfig, IcebergCreateConfig, RestCatalogMode};

#[cfg(feature = "iceberg")]
pub use iceberg_catalog::{
    browse_iceberg_catalog, guard_iceberg_connection_urls, preview_iceberg_table,
    verify_storage_access, BrowseDepth, CatalogBrowse, ColumnInfo, ColumnStats, PartitionFieldInfo,
    SnapshotRef, SortFieldInfo, StatsCompleteness, StatsTier, StorageAccessReport, TableIdentifier,
    TablePreview, TableRef, TableSchema,
};

#[cfg(feature = "iceberg")]
pub use iceberg_sample::{sample_column_values, sample_iceberg_rows};

// Internal-only: the virtual-dataset `/info` row-count fetch (ledger_info.rs)
// reuses the shared REST-client cache key and the metadata→schema extraction that
// the scan / preview paths already own, so it shares one catalog client and never
// re-derives snapshot/row-count logic.
#[cfg(feature = "iceberg")]
pub(crate) use iceberg_catalog::table_schema_from_metadata;
#[cfg(feature = "iceberg")]
pub(crate) use r2rml::{mapping_source_of, rest_client_cache_key};

#[cfg(feature = "iceberg")]
pub use iceberg_generate::{
    Diagnostic, GenerateOptions, GenerateR2rmlRequest, GenerateR2rmlResponse,
    StructuredR2rmlMapping, SubjectStrategy, TableOverride,
};

#[cfg(feature = "iceberg")]
pub use iceberg_validate::ValidateR2rmlResponse;

#[cfg(feature = "iceberg")]
pub use config::{R2rmlCreateConfig, R2rmlMappingInput};

// Re-export result types
pub use result::Bm25CreateResult;
pub use result::Bm25DropResult;
pub use result::Bm25StalenessCheck;
pub use result::Bm25SyncResult;
pub use result::SnapshotSelection;

#[cfg(feature = "vector")]
pub use result::VectorCreateResult;

#[cfg(feature = "vector")]
pub use result::VectorDropResult;

#[cfg(feature = "vector")]
pub use result::VectorStalenessCheck;

#[cfg(feature = "vector")]
pub use result::VectorSyncResult;

#[cfg(feature = "iceberg")]
pub use result::IcebergCreateResult;

#[cfg(feature = "iceberg")]
pub use result::R2rmlCreateResult;

// Re-export cache types
pub use cache::R2rmlCache;
pub use cache::R2rmlCacheStats;

// Re-export providers
pub use provider::FlureeIndexProvider;

#[cfg(feature = "iceberg")]
pub use r2rml::FlureeR2rmlProvider;

#[cfg(feature = "iceberg")]
pub use r2rml_materialize::{MaterializeResult, PersistedMaterializeJob};

// Helper functions are used internally by bm25.rs via direct module path
