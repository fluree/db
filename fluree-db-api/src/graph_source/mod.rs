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

/// The graph-source readers compiled into a build. A value rather than a set
/// of `cfg!` checks at the use site, so the refusal it drives can be tested
/// for a build without a reader from a build that has them all.
#[derive(Debug, Clone, Copy)]
pub(crate) struct SourceReaders {
    pub(crate) iceberg: bool,
    pub(crate) sql: bool,
    pub(crate) delta: bool,
}

impl SourceReaders {
    /// The readers this build has.
    pub(crate) const BUILT: Self = Self {
        iceberg: cfg!(feature = "iceberg"),
        sql: cfg!(feature = "sql"),
        delta: cfg!(feature = "delta"),
    };

    /// Refuse a graph source no reader here exists for: a mapped family these
    /// readers lack, or a type this version does not recognize (one a newer
    /// build registered). Read by nothing, its genesis view would answer every
    /// query as an empty graph.
    pub(crate) fn refuse_unread(
        self,
        record: &fluree_db_nameservice::GraphSourceRecord,
    ) -> fluree_db_query::error::Result<()> {
        use fluree_db_nameservice::GraphSourceType as T;
        use std::borrow::Cow;
        let without = |feature: &str| -> Cow<'static, str> {
            format!("it was compiled without the '{feature}' feature").into()
        };
        let missing: Option<(Cow<'static, str>, Cow<'static, str>)> = match &record.source_type {
            T::Iceberg if !self.iceberg => Some(("an Iceberg".into(), without("iceberg"))),
            T::R2rml if !self.iceberg => Some(("an R2RML".into(), without("iceberg"))),
            T::Sql if !self.sql => Some(("a SQL".into(), without("sql"))),
            T::Delta if !self.delta => Some(("a Delta".into(), without("delta"))),
            T::Unknown(type_name) => Some((
                format!("a '{type_name}'").into(),
                "it does not recognize that type".into(),
            )),
            _ => None,
        };
        match missing {
            Some((family, cause)) => Err(fluree_db_query::QueryError::GraphSourceUnsupported {
                graph_source_id: record.graph_source_id.to_string(),
                family,
                cause,
            }),
            None => Ok(()),
        }
    }
}

/// Refuse an index graph source (BM25, vector, geo) named where a graph is
/// read. An index has no triples: its searches find it by the `f:graphSource`
/// id in their own pattern, with the source ledger as the dataset, so as a
/// query target or dataset member it would read as an empty graph.
pub(crate) fn refuse_index_source(
    record: &fluree_db_nameservice::GraphSourceRecord,
) -> fluree_db_query::error::Result<()> {
    use fluree_db_nameservice::GraphSourceType as T;
    const SEARCH: &str = "query it with an f:graphSource pattern, with its source ledger as \
                          the dataset";
    // By type, not `kind()`: the nameservice files a type it does not know as
    // an index, but nothing says it is one; `SourceReaders::refuse_unread`
    // refuses it instead.
    let (index, remedy) = match &record.source_type {
        T::Bm25 => ("a BM25 index".to_string(), SEARCH),
        T::Vector => ("a vector index".to_string(), SEARCH),
        // No `f:graphSource` pattern searches a geo index; geo search reads
        // the ledger's own index.
        T::Geo => ("a geo index".to_string(), "query its source ledger instead"),
        _ => return Ok(()),
    };
    Err(fluree_db_query::QueryError::GraphSourceNotAGraph {
        graph_source_id: record.graph_source_id.to_string(),
        index,
        remedy,
    })
}

/// Refuse a live graph-source record no query reads as a graph whatever its
/// config: an index source, or a mapped source whose reader is not compiled
/// in.
pub(crate) fn refuse_unservable(
    record: &fluree_db_nameservice::GraphSourceRecord,
) -> fluree_db_query::error::Result<()> {
    refuse_index_source(record)?;
    SourceReaders::BUILT.refuse_unread(record)
}

/// Refuse a live graph-source record this build cannot answer a query from as
/// a graph: an index source, a mapped source whose reader is not compiled in,
/// or one with no mapping. The genesis-view resolver refuses through here, so
/// no such source reaches a query as an empty graph.
pub(crate) fn require_readable(
    record: &fluree_db_nameservice::GraphSourceRecord,
) -> fluree_db_query::error::Result<()> {
    #[cfg(feature = "iceberg")]
    r2rml::served_mapping_of(record)?;
    #[cfg(not(feature = "iceberg"))]
    refuse_unservable(record)?;
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
// The maintenance workers are native-only.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod dependency_index;
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

#[cfg(feature = "delta")]
mod delta;

#[cfg(feature = "delta")]
mod delta_catalog;

#[cfg(feature = "delta")]
pub use delta::{DeltaAzureFields, DeltaCreateConfig, DeltaCreateResult, DeltaUnityFields};

#[cfg(feature = "delta")]
pub use delta_catalog::{
    DeltaColumnInfo, DeltaTableAccess, DeltaTablePreview, GenerateDeltaR2rmlRequest,
    GenerateDeltaR2rmlResponse,
};

// Re-export configuration types
pub use config::Bm25CreateConfig;

#[cfg(feature = "vector")]
pub use config::VectorCreateConfig;

#[cfg(feature = "iceberg")]
pub use config::{
    CatalogMode, CatalogModeArgs, CatalogModeError, IcebergConnectionConfig, IcebergCreateConfig,
    RestCatalogMode,
};

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
pub(crate) use r2rml::{catalog_table_metadata, mapping_source_of, shared_catalog_client};

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
pub use result::{ConnectionCheck, R2rmlCreateResult};

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

#[cfg(test)]
mod tests {
    use super::SourceReaders;
    use fluree_db_nameservice::{GraphSourceRecord, GraphSourceType as T};
    use fluree_db_query::QueryError;

    fn record(source_type: T) -> GraphSourceRecord {
        GraphSourceRecord::new("gs", "main", source_type, "{}", Vec::new())
    }

    /// A build without a mapped family's reader refuses that family by name,
    /// whatever features this test binary itself was compiled with.
    #[test]
    fn a_build_without_a_reader_refuses_its_family_by_name() {
        let none = SourceReaders {
            iceberg: false,
            sql: false,
            delta: false,
        };
        for (source_type, family, feature) in [
            (T::Iceberg, "an Iceberg", "iceberg"),
            (T::R2rml, "an R2RML", "iceberg"),
            (T::Sql, "a SQL", "sql"),
            (T::Delta, "a Delta", "delta"),
        ] {
            match none.refuse_unread(&record(source_type.clone())) {
                Err(e @ QueryError::GraphSourceUnsupported { .. }) => assert_eq!(
                    e.to_string(),
                    format!(
                        "graph source 'gs:main' is {family} graph source, which this build \
                         cannot read: it was compiled without the '{feature}' feature"
                    ),
                    "{source_type:?}"
                ),
                other => panic!("{source_type:?}: expected GraphSourceUnsupported, got {other:?}"),
            }
        }
        // An index source has no mapped reader to lack.
        for source_type in [T::Bm25, T::Vector, T::Geo] {
            assert!(none.refuse_unread(&record(source_type)).is_ok());
        }

        // Refusing an index source is a separate rule, and every build has it.
        let all = SourceReaders {
            iceberg: true,
            sql: true,
            delta: true,
        };
        for source_type in [T::Iceberg, T::R2rml, T::Sql, T::Delta] {
            assert!(all.refuse_unread(&record(source_type)).is_ok());
        }

        // A type this version does not recognize has no reader whatever the
        // features.
        for readers in [none, all] {
            match readers.refuse_unread(&record(T::Unknown("f:FutureSource".into()))) {
                Err(e @ QueryError::GraphSourceUnsupported { .. }) => assert_eq!(
                    e.to_string(),
                    "graph source 'gs:main' is a 'f:FutureSource' graph source, which this \
                     build cannot read: it does not recognize that type"
                ),
                other => panic!("expected GraphSourceUnsupported, got {other:?}"),
            }
        }
    }

    /// Each index kind is refused by name, with how a query reaches it; a
    /// mapped source is not an index.
    #[test]
    fn an_index_source_is_refused_as_a_graph_by_its_kind() {
        for (source_type, index, remedy) in [
            (T::Bm25, "a BM25 index", "f:graphSource pattern"),
            (T::Vector, "a vector index", "f:graphSource pattern"),
            (T::Geo, "a geo index", "its source ledger"),
        ] {
            match super::refuse_index_source(&record(source_type.clone())) {
                Err(e @ QueryError::GraphSourceNotAGraph { .. }) => {
                    let message = e.to_string();
                    assert!(
                        message.starts_with(&format!(
                            "graph source 'gs:main' is {index}, not a graph: "
                        )) && message.contains(remedy),
                        "{source_type:?}: {message}"
                    );
                }
                other => panic!("{source_type:?}: expected GraphSourceNotAGraph, got {other:?}"),
            }
        }
        // An unrecognized type is not known to be an index, whatever the
        // nameservice files it as; `refuse_unread` refuses it.
        for source_type in [
            T::Iceberg,
            T::R2rml,
            T::Sql,
            T::Delta,
            T::Unknown("f:FutureSource".into()),
        ] {
            assert!(super::refuse_index_source(&record(source_type)).is_ok());
        }
    }
}
