//! # Fluree DB API
//!
//! High-level API for Fluree DB operations, providing unified access to
//! connections, ledger state management, and query execution.
//!
//! This crate composes the lower-level crates:
//! - `fluree-db-connection` - Configuration and storage
//! - `fluree-db-ledger` - Ledger state (Db + Novelty)
//! - `fluree-db-query` - Query execution
//! - `fluree-db-nameservice` - Ledger discovery
//!
//! ## Quick Start
//!
//! ```ignore
//! use fluree_db_api::{FlureeBuilder, GraphDb};
//!
//! // Create a file-backed Fluree instance
//! let fluree = FlureeBuilder::file("/data/fluree").build()?;
//!
//! // Create a new ledger
//! let ledger = fluree.create_ledger("mydb").await?;
//!
//! // Insert data
//! let result = fluree.insert(ledger, &data).await?;
//! let ledger = result.ledger;
//!
//! // Query
//! let db = GraphDb::from_ledger_state(&ledger);
//! let results = fluree.query(&db, &query).await?;
//!
//! // Load an existing ledger
//! let ledger = fluree.ledger("mydb:main").await?;
//! let db = GraphDb::from_ledger_state(&ledger);
//! ```

pub mod admin;
pub mod block_fetch;
pub mod bm25_worker;
mod commit_data;
pub mod commit_transfer;
pub mod config_resolver;
#[cfg(feature = "credential")]
pub mod credential;
pub mod dataset;
mod error;
pub mod explain;
pub mod export;
pub mod export_builder;
pub mod format;
pub mod graph;
pub mod graph_commit_builder;
pub mod graph_query_builder;
pub mod graph_snapshot;
pub mod graph_source;
pub mod graph_transact_builder;
pub mod import;
mod indexer_fulltext_provider;
mod ledger;
pub mod ledger_info;
mod merge;
mod merge_preview;
pub mod nameservice_query;
pub(crate) mod ns_helpers;
pub mod ontology_imports;
mod overlay;
pub mod pack;
pub mod policy_builder;
pub mod policy_view;
mod query;
mod rebase;
pub mod remote_service;
mod revert;
mod revert_preview;
pub(crate) mod runtime_dicts;
pub mod server_defaults;
mod time_resolve;
pub mod tx;
pub mod tx_builder;
#[cfg(feature = "vector")]
pub mod vector_worker;
pub mod view;
pub mod wire;

// Ledger caching and management
pub mod ledger_manager;
pub mod ledger_view;

// Search service integration (embedded adapter, remote client)
pub mod search;

pub use admin::{
    BranchDropReport,
    DropMode,
    DropReport,
    DropStatus,
    GraphSourceDropReport,
    // Index maintenance
    IndexStatusResult,
    ReindexOptions,
    ReindexResult,
    TriggerIndexOptions,
    TriggerIndexResult,
};
pub use block_fetch::{
    BlockAccessScope, BlockContent, BlockFetchError, EnforcementMode, FetchedBlock,
    LedgerBlockContext,
};
pub use commit_transfer::{
    Base64Bytes, BulkImportResult, CommitImportResult, ExportCommitsRequest, ExportCommitsResponse,
    PushCommitsRequest, PushCommitsResponse,
};
pub use dataset::{
    sparql_dataset_ledger_ids, DatasetParseError, DatasetSpec, GraphSource, QueryConnectionOptions,
    TimeSpec,
};
pub use error::{ApiError, BuilderError, BuilderErrors, Result};
pub use fluree_db_core::ContentId;
pub use fluree_db_core::RemoteObject;
pub use fluree_db_core::{
    commit_to_summary, find_common_ancestor, walk_commit_summaries, CommitSummary, CommonAncestor,
    ConflictKey,
};
pub use format::{AgentJsonContext, FormatError, FormatterConfig, OutputFormat, QueryOutput};
pub use graph::Graph;
pub use graph_commit_builder::{CommitBuilder, CommitDetail, ResolvedFlake, ResolvedValue};
pub use graph_query_builder::{GraphQueryBuilder, GraphSnapshotQueryBuilder};
pub use graph_snapshot::GraphSnapshot;
pub use graph_source::{
    Bm25CreateConfig, Bm25CreateResult, Bm25DropResult, Bm25StalenessCheck, Bm25SyncResult,
    FlureeIndexProvider, SnapshotSelection,
};
pub use graph_transact_builder::{GraphTransactBuilder, StagedGraph};
pub use import::{
    scan_directory_format, CreateBuilder, DirectoryFormat, EffectiveImportSettings, ImportBuilder,
    ImportConfig, ImportError, ImportPhase, ImportResult, ImportSummary, RemoteSource,
};
pub use ledger_info::LedgerInfoBuilder;
pub use ledger_manager::{
    FreshnessCheck, FreshnessSource, LedgerHandle, LedgerManager, LedgerManagerConfig,
    LedgerWriteGuard, NotifyResult, NsNotify, RefreshOpts, RefreshResult, RemoteWatermark,
    UpdatePlan,
};
pub use ledger_view::{CommitRef, LedgerView};
pub use merge::MergeReport;
pub use merge_preview::{
    AncestorRef, BranchDelta, ConflictDetail, ConflictResolutionPreview, ConflictSummary,
    MergePreview, MergePreviewOpts,
};
pub use pack::{
    compute_missing_index_artifacts, full_ledger_pack_request, validate_pack_request, PackChunk,
    PackStreamError, PackStreamResult,
};
pub use policy_builder::identity_has_no_policies;
pub use policy_view::{
    build_policy_context, wrap_identity_policy_view, wrap_policy_view, wrap_policy_view_historical,
    PolicyWrappedView,
};
pub use query::builder::{
    DatasetQueryBuilder, FromQueryBuilder, GraphSourceMode, ViewQueryBuilder,
};
pub use query::nameservice_builder::NameserviceQueryBuilder;
pub use query::{QueryResult, TrackedErrorResponse, TrackedQueryResponse};
pub use rebase::{ConflictStrategy, RebaseConflict, RebaseFailure, RebaseReport};
pub use revert::RevertReport;
pub use revert_preview::{RevertConflictSummary, RevertPreview, RevertPreviewOpts};
pub use tx::{
    IndexingMode, IndexingStatus, StageResult, TrackedTransactionInput, TransactResult,
    TransactResultRef,
};
pub use tx_builder::{OwnedTransactBuilder, RefTransactBuilder, Staged};
pub use view::{DataSetDb, GraphDb, QueryInput, ReasoningModePrecedence};

#[cfg(feature = "iceberg")]
pub use graph_source::{
    CatalogMode, FlureeR2rmlProvider, IcebergCreateConfig, IcebergCreateResult, R2rmlCreateConfig,
    R2rmlCreateResult, R2rmlMappingInput, RestCatalogMode,
};

pub use bm25_worker::{
    Bm25MaintenanceWorker, Bm25WorkerConfig, Bm25WorkerHandle, Bm25WorkerState, Bm25WorkerStats,
};

#[cfg(feature = "vector")]
pub use vector_worker::{
    VectorMaintenanceWorker, VectorWorkerConfig, VectorWorkerHandle, VectorWorkerState,
    VectorWorkerStats,
};

#[cfg(feature = "vector")]
pub use graph_source::{
    VectorCreateConfig, VectorCreateResult, VectorDropResult, VectorStalenessCheck,
    VectorSyncResult,
};

// Re-export search provider adapter
pub use search::EmbeddedBm25SearchProvider;

// Re-export indexer types for background indexing setup
pub use fluree_db_indexer::{
    current_index_request_correlation, with_index_request_correlation, BackgroundIndexerWorker,
    IndexCompletion, IndexOutcome, IndexPhase, IndexRequestCorrelation, IndexStatusSnapshot,
    IndexerConfig, IndexerHandle,
};

// Re-export commonly used types from child crates
pub use fluree_db_connection::{ConnectionConfig, StorageType};
pub use fluree_db_core::commit::codec::verify_commit_blob;
#[cfg(feature = "native")]
pub use fluree_db_core::FileStorage;
pub use fluree_db_core::{
    ContentAddressedWrite, ContentKind, ContentWriteResult, MemoryStorage, OverlayProvider,
    Storage, StorageMethod, StorageRead, StorageWrite,
};
pub use fluree_db_ledger::{
    HistoricalLedgerView, IndexConfig, LedgerState, StagedLedger, TypeErasedStore,
};
pub use fluree_db_nameservice::{
    ConfigCasResult, ConfigPayload, ConfigPublisher, ConfigValue, GraphSourceLookup,
    GraphSourcePublisher, NameService, NsRecord, Publisher,
};
pub use fluree_db_novelty::Novelty;
pub use fluree_db_query::{
    execute, execute_pattern, Batch, ContextConfig, ExecutableQuery, NoOpR2rmlProvider, Pattern,
    ReasoningConfig, VarRegistry,
};
// Re-export for lower-level pattern-based queries (internal/advanced use)
pub use fluree_db_query::{Term, TriplePattern};
// Re-export parse types for query results
pub use fluree_db_query::ir::Query;
pub use fluree_db_query::parse::ParseError;
pub use fluree_db_transact::{
    lower_sparql_update, lower_sparql_update_ast, CommitOpts, CommitReceipt,
    LowerError as SparqlUpdateLowerError, NamespaceRegistry, TransactError, TxnOpts, TxnType,
};

// Re-export SPARQL types (product feature; always enabled)
pub use fluree_db_sparql::{
    lower_sparql, parse_sparql, validate as validate_sparql, Capabilities as SparqlCapabilities,
    Diagnostic as SparqlDiagnostic, LowerError as SparqlLowerError,
    ParseOutput as SparqlParseOutput, Prologue as SparqlPrologue, QueryBody as SparqlQueryBody,
    Severity as SparqlSeverity, SourceSpan as SparqlSourceSpan, SparqlAst,
    UpdateOperation as SparqlUpdateOperation,
};

// Re-export policy types for access control
pub use fluree_db_policy::{
    build_policy_set, build_policy_values_clause, filter_by_required, is_schema_flake,
    NoOpQueryExecutor, PolicyAction, PolicyContext, PolicyError, PolicyQuery, PolicyQueryExecutor,
    PolicyRestriction, PolicySet, PolicyValue, PolicyWrapper, TargetMode,
};

// Re-export tracking types for query/transaction metrics
pub use fluree_db_core::{FuelExceededError, PolicyStats, Tracker, TrackingOptions, TrackingTally};

use async_trait::async_trait;
use fluree_db_core::{ContentStore, StorageBackend};
#[cfg(feature = "native")]
use fluree_db_nameservice::file::FileNameService;
use fluree_db_nameservice::memory::MemoryNameService;
#[cfg(feature = "aws")]
use fluree_db_nameservice::StorageNameService;
use std::sync::Arc;

// Re-export encryption types for convenient access
pub use fluree_db_crypto::{EncryptedStorage, EncryptionKey, StaticKeyProvider};
pub use fluree_graph_json_ld::ParsedContext;

// ============================================================================
// Dynamic runtime wrappers (single JSON-LD "source of truth")
// ============================================================================

// Re-export the combined read-write nameservice trait from the nameservice crate.
pub use fluree_db_nameservice::NameServicePublisher;

/// Runtime nameservice selection.
///
/// Encodes whether this Fluree instance has full read-write nameservice access
/// or is a read-only proxy that forwards writes to a remote transaction server.
///
/// Analogous to [`StorageBackend`] for storage.
#[derive(Clone)]
pub enum NameServiceMode {
    /// Full read-write nameservice (File, Memory, S3, DynamoDB).
    ReadWrite(Arc<dyn NameServicePublisher>),
    /// Read-only proxy nameservice.
    /// Writes are forwarded to the remote transaction server via HTTP.
    ReadOnly(Arc<dyn NameService>),
}

impl std::fmt::Debug for NameServiceMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ReadWrite(ns) => f.debug_tuple("ReadWrite").field(ns).finish(),
            Self::ReadOnly(ns) => f.debug_tuple("ReadOnly").field(ns).finish(),
        }
    }
}

impl NameServiceMode {
    /// Get read-only nameservice access (always available).
    pub fn reader(&self) -> &dyn NameService {
        match self {
            Self::ReadWrite(ns) => ns.as_ref(),
            Self::ReadOnly(ns) => ns.as_ref(),
        }
    }

    /// Owned `Arc<dyn NameService>` view, for handing off to long-lived
    /// subsystems (e.g. the indexer's full-text config provider) that
    /// need to outlive a single borrow of the mode enum.
    pub fn as_arc_reader(&self) -> Arc<dyn NameService> {
        match self {
            Self::ReadWrite(ns) => Arc::clone(ns) as Arc<dyn NameService>,
            Self::ReadOnly(ns) => Arc::clone(ns),
        }
    }

    /// Get read-write nameservice access (only for ReadWrite mode).
    pub fn publisher(&self) -> Option<&dyn NameServicePublisher> {
        match self {
            Self::ReadWrite(ns) => Some(ns.as_ref()),
            Self::ReadOnly(_) => None,
        }
    }

    /// Get a cloned `Arc` to the read-write nameservice (only for ReadWrite mode).
    ///
    /// Useful when callers need an owned `Arc<dyn NameServicePublisher>` for
    /// passing into subsystems like `SyncDriver`.
    pub fn publisher_arc(&self) -> Option<Arc<dyn NameServicePublisher>> {
        match self {
            Self::ReadWrite(ns) => Some(Arc::clone(ns)),
            Self::ReadOnly(_) => None,
        }
    }

    /// Whether this is a read-only (proxy) instance.
    pub fn is_read_only(&self) -> bool {
        matches!(self, Self::ReadOnly(_))
    }
}

#[async_trait]
impl fluree_db_nameservice::NameService for NameServiceMode {
    async fn lookup(
        &self,
        ledger_id: &str,
    ) -> std::result::Result<
        Option<fluree_db_nameservice::NsRecord>,
        fluree_db_nameservice::NameServiceError,
    > {
        self.reader().lookup(ledger_id).await
    }

    async fn all_records(
        &self,
    ) -> std::result::Result<
        Vec<fluree_db_nameservice::NsRecord>,
        fluree_db_nameservice::NameServiceError,
    > {
        self.reader().all_records().await
    }

    async fn create_branch(
        &self,
        ledger_name: &str,
        new_branch: &str,
        source_branch: &str,
        at_commit: Option<(fluree_db_core::ContentId, i64)>,
    ) -> std::result::Result<(), fluree_db_nameservice::NameServiceError> {
        self.reader()
            .create_branch(ledger_name, new_branch, source_branch, at_commit)
            .await
    }

    async fn drop_branch(
        &self,
        ledger_id: &str,
    ) -> std::result::Result<Option<u32>, fluree_db_nameservice::NameServiceError> {
        self.reader().drop_branch(ledger_id).await
    }

    async fn reset_head(
        &self,
        ledger_id: &str,
        snapshot: fluree_db_nameservice::NsRecordSnapshot,
    ) -> std::result::Result<(), fluree_db_nameservice::NameServiceError> {
        self.reader().reset_head(ledger_id, snapshot).await
    }
}

#[async_trait]
impl fluree_db_nameservice::GraphSourceLookup for NameServiceMode {
    async fn lookup_graph_source(
        &self,
        graph_source_id: &str,
    ) -> std::result::Result<
        Option<fluree_db_nameservice::GraphSourceRecord>,
        fluree_db_nameservice::NameServiceError,
    > {
        self.reader().lookup_graph_source(graph_source_id).await
    }

    async fn lookup_any(
        &self,
        resource_id: &str,
    ) -> std::result::Result<
        fluree_db_nameservice::NsLookupResult,
        fluree_db_nameservice::NameServiceError,
    > {
        self.reader().lookup_any(resource_id).await
    }

    async fn all_graph_source_records(
        &self,
    ) -> std::result::Result<
        Vec<fluree_db_nameservice::GraphSourceRecord>,
        fluree_db_nameservice::NameServiceError,
    > {
        self.reader().all_graph_source_records().await
    }
}

#[async_trait]
impl fluree_db_nameservice::GraphSourcePublisher for NameServiceMode {
    async fn publish_graph_source(
        &self,
        name: &str,
        branch: &str,
        source_type: fluree_db_nameservice::GraphSourceType,
        config: &str,
        dependencies: &[String],
    ) -> std::result::Result<(), fluree_db_nameservice::NameServiceError> {
        match self {
            Self::ReadWrite(ns) => {
                ns.publish_graph_source(name, branch, source_type, config, dependencies)
                    .await
            }
            Self::ReadOnly(_) => Err(fluree_db_nameservice::NameServiceError::Storage(
                "publish_graph_source not available on read-only nameservice".into(),
            )),
        }
    }

    async fn publish_graph_source_index(
        &self,
        name: &str,
        branch: &str,
        index_id: &fluree_db_core::ContentId,
        index_t: i64,
    ) -> std::result::Result<(), fluree_db_nameservice::NameServiceError> {
        match self {
            Self::ReadWrite(ns) => {
                ns.publish_graph_source_index(name, branch, index_id, index_t)
                    .await
            }
            Self::ReadOnly(_) => Err(fluree_db_nameservice::NameServiceError::Storage(
                "publish_graph_source_index not available on read-only nameservice".into(),
            )),
        }
    }

    async fn retract_graph_source(
        &self,
        name: &str,
        branch: &str,
    ) -> std::result::Result<(), fluree_db_nameservice::NameServiceError> {
        match self {
            Self::ReadWrite(ns) => ns.retract_graph_source(name, branch).await,
            Self::ReadOnly(_) => Err(fluree_db_nameservice::NameServiceError::Storage(
                "retract_graph_source not available on read-only nameservice".into(),
            )),
        }
    }
}

#[async_trait]
impl fluree_db_nameservice::AdminPublisher for NameServiceMode {
    async fn publish_index_allow_equal(
        &self,
        ledger_id: &str,
        index_t: i64,
        index_id: &fluree_db_core::ContentId,
    ) -> std::result::Result<(), fluree_db_nameservice::NameServiceError> {
        match self {
            Self::ReadWrite(ns) => {
                ns.publish_index_allow_equal(ledger_id, index_t, index_id)
                    .await
            }
            Self::ReadOnly(_) => Err(fluree_db_nameservice::NameServiceError::Storage(
                "publish_index_allow_equal not available on read-only nameservice".into(),
            )),
        }
    }
}

#[async_trait]
impl fluree_db_nameservice::ConfigLookup for NameServiceMode {
    async fn get_config(
        &self,
        ledger_id: &str,
    ) -> std::result::Result<
        Option<fluree_db_nameservice::ConfigValue>,
        fluree_db_nameservice::NameServiceError,
    > {
        self.reader().get_config(ledger_id).await
    }
}

#[async_trait]
impl fluree_db_nameservice::ConfigPublisher for NameServiceMode {
    async fn push_config(
        &self,
        ledger_id: &str,
        expected: Option<&fluree_db_nameservice::ConfigValue>,
        new: &fluree_db_nameservice::ConfigValue,
    ) -> std::result::Result<
        fluree_db_nameservice::ConfigCasResult,
        fluree_db_nameservice::NameServiceError,
    > {
        match self {
            Self::ReadWrite(ns) => ns.push_config(ledger_id, expected, new).await,
            Self::ReadOnly(_) => Err(fluree_db_nameservice::NameServiceError::Storage(
                "push_config not available on read-only nameservice".into(),
            )),
        }
    }
}

#[async_trait]
impl fluree_db_nameservice::StatusLookup for NameServiceMode {
    async fn get_status(
        &self,
        ledger_id: &str,
    ) -> std::result::Result<
        Option<fluree_db_nameservice::StatusValue>,
        fluree_db_nameservice::NameServiceError,
    > {
        self.reader().get_status(ledger_id).await
    }
}

#[async_trait]
impl fluree_db_nameservice::StatusPublisher for NameServiceMode {
    async fn push_status(
        &self,
        ledger_id: &str,
        expected: Option<&fluree_db_nameservice::StatusValue>,
        new: &fluree_db_nameservice::StatusValue,
    ) -> std::result::Result<
        fluree_db_nameservice::StatusCasResult,
        fluree_db_nameservice::NameServiceError,
    > {
        match self {
            Self::ReadWrite(ns) => ns.push_status(ledger_id, expected, new).await,
            Self::ReadOnly(_) => Err(fluree_db_nameservice::NameServiceError::Storage(
                "push_status not available on read-only nameservice".into(),
            )),
        }
    }
}

#[async_trait]
impl fluree_db_nameservice::RefLookup for NameServiceMode {
    async fn get_ref(
        &self,
        ledger_id: &str,
        kind: fluree_db_nameservice::RefKind,
    ) -> std::result::Result<
        Option<fluree_db_nameservice::RefValue>,
        fluree_db_nameservice::NameServiceError,
    > {
        self.reader().get_ref(ledger_id, kind).await
    }
}

#[async_trait]
impl fluree_db_nameservice::RefPublisher for NameServiceMode {
    async fn compare_and_set_ref(
        &self,
        ledger_id: &str,
        kind: fluree_db_nameservice::RefKind,
        expected: Option<&fluree_db_nameservice::RefValue>,
        new: &fluree_db_nameservice::RefValue,
    ) -> std::result::Result<
        fluree_db_nameservice::CasResult,
        fluree_db_nameservice::NameServiceError,
    > {
        match self {
            Self::ReadWrite(ns) => ns.compare_and_set_ref(ledger_id, kind, expected, new).await,
            Self::ReadOnly(_) => Err(fluree_db_nameservice::NameServiceError::Storage(
                "compare_and_set_ref not available on read-only nameservice".into(),
            )),
        }
    }
}

#[async_trait]
impl fluree_db_nameservice::Publisher for NameServiceMode {
    async fn publish_ledger_init(
        &self,
        ledger_id: &str,
    ) -> std::result::Result<(), fluree_db_nameservice::NameServiceError> {
        match self {
            Self::ReadWrite(ns) => ns.publish_ledger_init(ledger_id).await,
            Self::ReadOnly(_) => Err(fluree_db_nameservice::NameServiceError::Storage(
                "publish_ledger_init not available on read-only nameservice".into(),
            )),
        }
    }

    async fn publish_commit(
        &self,
        ledger_id: &str,
        commit_t: i64,
        commit_id: &fluree_db_core::ContentId,
    ) -> std::result::Result<(), fluree_db_nameservice::NameServiceError> {
        match self {
            Self::ReadWrite(ns) => ns.publish_commit(ledger_id, commit_t, commit_id).await,
            Self::ReadOnly(_) => Err(fluree_db_nameservice::NameServiceError::Storage(
                "publish_commit not available on read-only nameservice".into(),
            )),
        }
    }

    async fn publish_index(
        &self,
        ledger_id: &str,
        index_t: i64,
        index_id: &fluree_db_core::ContentId,
    ) -> std::result::Result<(), fluree_db_nameservice::NameServiceError> {
        match self {
            Self::ReadWrite(ns) => ns.publish_index(ledger_id, index_t, index_id).await,
            Self::ReadOnly(_) => Err(fluree_db_nameservice::NameServiceError::Storage(
                "publish_index not available on read-only nameservice".into(),
            )),
        }
    }

    async fn retract(
        &self,
        ledger_id: &str,
    ) -> std::result::Result<(), fluree_db_nameservice::NameServiceError> {
        match self {
            Self::ReadWrite(ns) => ns.retract(ledger_id).await,
            Self::ReadOnly(_) => Err(fluree_db_nameservice::NameServiceError::Storage(
                "retract not available on read-only nameservice".into(),
            )),
        }
    }

    async fn purge(
        &self,
        ledger_id: &str,
    ) -> std::result::Result<(), fluree_db_nameservice::NameServiceError> {
        match self {
            Self::ReadWrite(ns) => ns.purge(ledger_id).await,
            Self::ReadOnly(_) => Err(fluree_db_nameservice::NameServiceError::Storage(
                "purge not available on read-only nameservice".into(),
            )),
        }
    }

    fn publishing_ledger_id(&self, ledger_id: &str) -> Option<String> {
        match self {
            Self::ReadWrite(ns) => ns.publishing_ledger_id(ledger_id),
            Self::ReadOnly(_) => None,
        }
    }
}

/// Tiered storage router used for `commitStorage` vs `indexStorage`.
///
/// This routes writes/reads based on the address path:
/// - `.../commit/...` and `.../txn/...` -> commit storage
/// - everything else -> index storage
#[derive(Clone, Debug)]
pub struct TieredStorage<S> {
    commit: S,
    index: S,
}

impl<S> TieredStorage<S> {
    pub fn new(commit: S, index: S) -> Self {
        Self { commit, index }
    }

    fn route_to_commit(address: &str) -> bool {
        // Extract the path portion after :// if present (fluree:*://path)
        let path = address.split("://").nth(1).unwrap_or(address);

        // Commit blobs + txn blobs go to commit storage.
        path.contains("/commit/") || path.contains("/txn/")
    }
}

#[async_trait]
impl<S> StorageRead for TieredStorage<S>
where
    S: StorageRead + Send + Sync,
{
    async fn read_bytes(
        &self,
        address: &str,
    ) -> std::result::Result<Vec<u8>, fluree_db_core::Error> {
        if Self::route_to_commit(address) {
            self.commit.read_bytes(address).await
        } else {
            self.index.read_bytes(address).await
        }
    }

    async fn read_bytes_hint(
        &self,
        address: &str,
        hint: fluree_db_core::ReadHint,
    ) -> std::result::Result<Vec<u8>, fluree_db_core::Error> {
        if Self::route_to_commit(address) {
            self.commit.read_bytes_hint(address, hint).await
        } else {
            self.index.read_bytes_hint(address, hint).await
        }
    }

    async fn read_byte_range(
        &self,
        address: &str,
        range: std::ops::Range<u64>,
    ) -> std::result::Result<Vec<u8>, fluree_db_core::Error> {
        if Self::route_to_commit(address) {
            self.commit.read_byte_range(address, range).await
        } else {
            self.index.read_byte_range(address, range).await
        }
    }

    async fn exists(&self, address: &str) -> std::result::Result<bool, fluree_db_core::Error> {
        if Self::route_to_commit(address) {
            self.commit.exists(address).await
        } else {
            self.index.exists(address).await
        }
    }

    async fn list_prefix(
        &self,
        prefix: &str,
    ) -> std::result::Result<Vec<String>, fluree_db_core::Error> {
        // Route based on prefix - commit/txn prefixes go to commit storage
        if Self::route_to_commit(prefix) {
            self.commit.list_prefix(prefix).await
        } else {
            self.index.list_prefix(prefix).await
        }
    }
}

#[async_trait]
impl<S> StorageWrite for TieredStorage<S>
where
    S: StorageWrite + Send + Sync,
{
    async fn write_bytes(
        &self,
        address: &str,
        bytes: &[u8],
    ) -> std::result::Result<(), fluree_db_core::Error> {
        if Self::route_to_commit(address) {
            self.commit.write_bytes(address, bytes).await
        } else {
            self.index.write_bytes(address, bytes).await
        }
    }

    async fn delete(&self, address: &str) -> std::result::Result<(), fluree_db_core::Error> {
        if Self::route_to_commit(address) {
            self.commit.delete(address).await
        } else {
            self.index.delete(address).await
        }
    }
}

#[async_trait]
impl<S> ContentAddressedWrite for TieredStorage<S>
where
    S: ContentAddressedWrite + Send + Sync,
{
    async fn content_write_bytes_with_hash(
        &self,
        kind: ContentKind,
        ledger_id: &str,
        content_hash_hex: &str,
        bytes: &[u8],
    ) -> std::result::Result<ContentWriteResult, fluree_db_core::Error> {
        // Commit blobs + txn blobs go to commit storage.
        match kind {
            ContentKind::Commit | ContentKind::Txn => {
                self.commit
                    .content_write_bytes_with_hash(kind, ledger_id, content_hash_hex, bytes)
                    .await
            }
            _ => {
                self.index
                    .content_write_bytes_with_hash(kind, ledger_id, content_hash_hex, bytes)
                    .await
            }
        }
    }

    async fn content_write_bytes(
        &self,
        kind: ContentKind,
        ledger_id: &str,
        bytes: &[u8],
    ) -> std::result::Result<ContentWriteResult, fluree_db_core::Error> {
        // Commit blobs + txn blobs go to commit storage.
        match kind {
            ContentKind::Commit | ContentKind::Txn => {
                self.commit
                    .content_write_bytes(kind, ledger_id, bytes)
                    .await
            }
            _ => self.index.content_write_bytes(kind, ledger_id, bytes).await,
        }
    }
}

impl<S: StorageMethod> StorageMethod for TieredStorage<S> {
    fn storage_method(&self) -> &str {
        // Use the index storage method as the canonical method — both tiers
        // use the same method in practice (both S3, both file, etc.)
        self.index.storage_method()
    }
}

// ============================================================================
// Address Identifier Resolver Storage
// ============================================================================

/// Storage wrapper that routes reads based on address identifiers.
///
/// This enables identifier-based routing where addresses like
/// `fluree:<identifier>:<method>://path` are routed to a specific storage backend.
///
/// # Routing Rules
/// - If address contains an identifier that exists in the map -> route to that storage
/// - If address contains an unknown identifier -> fallback to default storage
/// - If address has no identifier -> route to default storage
/// - **Writes always go to default storage** (no identifier-based routing for writes)
///
/// # Example JSON-LD Config
/// ```json
/// {
///   "addressIdentifiers": {
///     "commit-storage": {"@id": "commitS3"},
///     "index-storage": {"@id": "indexS3"}
///   }
/// }
/// ```
#[derive(Clone, Debug)]
pub struct AddressIdentifierResolverStorage {
    /// Default storage for unmatched identifiers and all writes
    default: Arc<dyn Storage>,
    /// Map of identifier -> storage for routing reads
    identifier_map: std::sync::Arc<std::collections::HashMap<String, Arc<dyn Storage>>>,
}

impl AddressIdentifierResolverStorage {
    /// Create a new resolver storage.
    ///
    /// # Arguments
    /// - `default`: Storage to use for unmatched identifiers and all writes
    /// - `identifier_map`: Map of identifier string -> storage
    pub fn new(
        default: Arc<dyn Storage>,
        identifier_map: std::collections::HashMap<String, Arc<dyn Storage>>,
    ) -> Self {
        Self {
            default,
            identifier_map: std::sync::Arc::new(identifier_map),
        }
    }

    /// Route an address to the appropriate storage for reads.
    fn route(&self, address: &str) -> &Arc<dyn Storage> {
        if let Some(identifier) = fluree_db_core::extract_identifier(address) {
            if let Some(storage) = self.identifier_map.get(identifier) {
                return storage;
            }
        }
        // Fallback to default for unknown identifiers or no identifier
        &self.default
    }
}

#[async_trait]
impl StorageRead for AddressIdentifierResolverStorage {
    async fn read_bytes(
        &self,
        address: &str,
    ) -> std::result::Result<Vec<u8>, fluree_db_core::Error> {
        self.route(address).read_bytes(address).await
    }

    async fn read_bytes_hint(
        &self,
        address: &str,
        hint: fluree_db_core::ReadHint,
    ) -> std::result::Result<Vec<u8>, fluree_db_core::Error> {
        self.route(address).read_bytes_hint(address, hint).await
    }

    async fn read_byte_range(
        &self,
        address: &str,
        range: std::ops::Range<u64>,
    ) -> std::result::Result<Vec<u8>, fluree_db_core::Error> {
        self.route(address).read_byte_range(address, range).await
    }

    async fn exists(&self, address: &str) -> std::result::Result<bool, fluree_db_core::Error> {
        self.route(address).exists(address).await
    }

    /// List always uses the default storage
    async fn list_prefix(
        &self,
        prefix: &str,
    ) -> std::result::Result<Vec<String>, fluree_db_core::Error> {
        self.default.list_prefix(prefix).await
    }

    fn resolve_local_path(&self, address: &str) -> Option<std::path::PathBuf> {
        self.route(address).resolve_local_path(address)
    }
}

#[async_trait]
impl StorageWrite for AddressIdentifierResolverStorage {
    /// Writes always go to the default storage (MVP: no identifier-based write routing)
    async fn write_bytes(
        &self,
        address: &str,
        bytes: &[u8],
    ) -> std::result::Result<(), fluree_db_core::Error> {
        self.default.write_bytes(address, bytes).await
    }

    /// Deletes always go to the default storage
    async fn delete(&self, address: &str) -> std::result::Result<(), fluree_db_core::Error> {
        self.default.delete(address).await
    }
}

#[async_trait]
impl ContentAddressedWrite for AddressIdentifierResolverStorage {
    /// Content writes always go to the default storage
    async fn content_write_bytes_with_hash(
        &self,
        kind: ContentKind,
        ledger_id: &str,
        content_hash_hex: &str,
        bytes: &[u8],
    ) -> std::result::Result<ContentWriteResult, fluree_db_core::Error> {
        self.default
            .content_write_bytes_with_hash(kind, ledger_id, content_hash_hex, bytes)
            .await
    }

    async fn content_write_bytes(
        &self,
        kind: ContentKind,
        ledger_id: &str,
        bytes: &[u8],
    ) -> std::result::Result<ContentWriteResult, fluree_db_core::Error> {
        self.default
            .content_write_bytes(kind, ledger_id, bytes)
            .await
    }
}

impl StorageMethod for AddressIdentifierResolverStorage {
    fn storage_method(&self) -> &str {
        self.default.storage_method()
    }
}

/// Type-erased Fluree runtime type returned by `FlureeBuilder::build_client()`.
///
/// Now that `Fluree` no longer has a type parameter, this is a simple alias
/// kept for backward compatibility.
pub type FlureeClient = Fluree;

fn decode_encryption_key_base64(key_str: &str) -> Result<[u8; 32]> {
    use base64::Engine;
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(key_str)
        .map_err(|e| ApiError::config(format!("Invalid base64 encryption key: {e}")))?;

    if decoded.len() != 32 {
        return Err(ApiError::config(format!(
            "Encryption key must be 32 bytes, got {} bytes",
            decoded.len()
        )));
    }

    let mut key = [0u8; 32];
    key.copy_from_slice(&decoded);
    Ok(key)
}

/// Build an S3 storage instance from a StorageConfig.
///
/// Returns an `Arc<dyn Storage>` which may be either:
/// - `S3Storage` directly (if no encryption key is configured)
/// - `EncryptedStorage<S3Storage, ...>` (if `aes256_key` is set)
#[cfg(feature = "aws")]
async fn build_s3_storage_from_config(
    storage_config: &fluree_db_connection::config::StorageConfig,
) -> Result<Arc<dyn Storage>> {
    use fluree_db_connection::config::StorageType;
    use fluree_db_storage_aws::{S3Config as RawS3Config, S3Storage};

    let StorageType::S3(s3_config) = &storage_config.storage_type else {
        return Err(ApiError::config("Expected S3 storage config"));
    };

    let sdk_config = fluree_db_connection::aws::get_or_init_sdk_config()
        .await
        .map_err(|e| ApiError::config(format!("Failed to get AWS SDK config: {e}")))?;

    let raw_config = RawS3Config {
        bucket: s3_config.bucket.to_string(),
        prefix: s3_config
            .prefix
            .as_ref()
            .map(std::string::ToString::to_string),
        endpoint: s3_config
            .endpoint
            .as_ref()
            .map(std::string::ToString::to_string),
        // Consolidate per-op timeouts to a single SDK operation timeout.
        // Use the maximum to avoid unexpectedly shortening slower operations.
        timeout_ms: {
            let mut max_ms: Option<u64> = None;
            for ms in [
                s3_config.read_timeout_ms,
                s3_config.write_timeout_ms,
                s3_config.list_timeout_ms,
            ]
            .into_iter()
            .flatten()
            {
                max_ms = Some(max_ms.map(|cur| cur.max(ms)).unwrap_or(ms));
            }
            max_ms
        },
        max_retries: s3_config.max_retries.map(|n| n as u32),
        retry_base_delay_ms: s3_config.retry_base_delay_ms,
        retry_max_delay_ms: s3_config.retry_max_delay_ms,
    };

    let storage = S3Storage::new(sdk_config, raw_config)
        .await
        .map_err(|e| ApiError::config(format!("Failed to create S3 storage: {e}")))?;

    // Wrap with encryption if key is configured
    if let Some(key_str) = storage_config.aes256_key.as_ref() {
        let key = decode_encryption_key_base64(key_str.as_ref())?;
        let encryption_key = EncryptionKey::new(key, 0);
        let key_provider = StaticKeyProvider::new(encryption_key);
        Ok(Arc::new(EncryptedStorage::new(storage, key_provider)))
    } else {
        Ok(Arc::new(storage))
    }
}

/// Build a local (memory/file) storage instance from a StorageConfig.
#[cfg(feature = "native")]
fn build_local_storage_from_config(
    storage_config: &fluree_db_connection::config::StorageConfig,
) -> Result<Arc<dyn Storage>> {
    use fluree_db_connection::config::StorageType;

    match &storage_config.storage_type {
        StorageType::Memory => Ok(Arc::new(MemoryStorage::new())),
        StorageType::File => {
            let path = storage_config
                .path
                .as_ref()
                .ok_or_else(|| ApiError::config("File storage requires filePath"))?;
            let storage = FileStorage::new(path.as_ref());
            if let Some(key_str) = storage_config.aes256_key.as_ref() {
                let key = decode_encryption_key_base64(key_str.as_ref())?;
                let encryption_key = EncryptionKey::new(key, 0);
                let key_provider = StaticKeyProvider::new(encryption_key);
                Ok(Arc::new(EncryptedStorage::new(storage, key_provider)))
            } else {
                Ok(Arc::new(storage))
            }
        }
        StorageType::S3(_) => Err(ApiError::config(
            "S3 storage in addressIdentifiers is only supported with 'aws' feature",
        )),
        StorageType::Unsupported { type_iri, .. } => Err(ApiError::config(format!(
            "Unsupported storage type in addressIdentifiers: {type_iri}"
        ))),
    }
}

/// Build a memory storage instance from a StorageConfig (non-native fallback).
#[cfg(not(feature = "native"))]
fn build_local_storage_from_config(
    storage_config: &fluree_db_connection::config::StorageConfig,
) -> Result<Arc<dyn Storage>> {
    use fluree_db_connection::config::StorageType;

    match &storage_config.storage_type {
        StorageType::Memory => Ok(Arc::new(MemoryStorage::new())),
        StorageType::File => Err(ApiError::config(
            "File storage in addressIdentifiers requires 'native' feature",
        )),
        StorageType::S3(_) => Err(ApiError::config(
            "S3 storage in addressIdentifiers requires 'aws' feature",
        )),
        StorageType::Unsupported { type_iri, .. } => Err(ApiError::config(format!(
            "Unsupported storage type in addressIdentifiers: {}",
            type_iri
        ))),
    }
}

/// Check if background indexing is enabled in the parsed connection config.
///
/// Defaults to `true` when not explicitly configured — indexing is core
/// functionality and should require an explicit opt-out (e.g. a peer/transactor
/// that delegates indexing to a separate process).
fn is_indexing_enabled(config: &ConnectionConfig) -> bool {
    config
        .defaults
        .as_ref()
        .and_then(|d| d.indexing.as_ref())
        .and_then(|i| i.indexing_enabled)
        .unwrap_or(true)
}

/// Build IndexerConfig from connection defaults, falling back to defaults.
fn build_indexer_config(config: &ConnectionConfig) -> fluree_db_indexer::IndexerConfig {
    let mut indexer_config = fluree_db_indexer::IndexerConfig::default();

    // Apply gc_max_old_indexes from config if present
    if let Some(max_old) = config
        .defaults
        .as_ref()
        .and_then(|d| d.indexing.as_ref())
        .and_then(|i| i.max_old_indexes)
    {
        indexer_config.gc_max_old_indexes = max_old.min(u32::MAX as u64) as u32;
    }

    // Apply gc_min_time_mins from config if present
    if let Some(min_time) = config
        .defaults
        .as_ref()
        .and_then(|d| d.indexing.as_ref())
        .and_then(|i| i.gc_min_time_mins)
    {
        indexer_config.gc_min_time_mins = min_time.min(u32::MAX as u64) as u32;
    }

    indexer_config
}

/// Derive `IndexConfig` from `ConnectionConfig` defaults (or fall back to compiled defaults).
///
/// Used by `FlureeBuilder::from_json_ld()` to extract indexing thresholds from the
/// parsed JSON-LD connection config.
fn derive_index_config(config: &ConnectionConfig) -> IndexConfig {
    let indexing = config.defaults.as_ref().and_then(|d| d.indexing.as_ref());

    IndexConfig {
        reindex_min_bytes: indexing
            .and_then(|i| i.reindex_min_bytes)
            .map(|v| v as usize)
            .unwrap_or(server_defaults::DEFAULT_REINDEX_MIN_BYTES),
        reindex_max_bytes: indexing
            .and_then(|i| i.reindex_max_bytes)
            .map(|v| v as usize)
            .unwrap_or_else(server_defaults::default_reindex_max_bytes),
    }
}

/// Builder for creating Fluree instances
///
/// Provides a fluent API for configuring storage, cache, and nameservice options.
///
/// This is the **single construction path** for all Fluree instances. Both
/// programmatic (Rust embedder) and config-based (JSON-LD) construction go
/// through this builder.
///
/// ## Typed vs Dynamic Builds
///
/// - **Typed builds** (`build()`, `build_memory()`, `build_s3()`) return concrete
///   `Fluree` types — best for Rust embedders who know the storage backend
///   at compile time.
/// - **Dynamic build** (`build_client()`) returns `FlureeClient` (type-erased) —
///   used when the storage backend is determined at runtime from config.
#[derive(Debug, Clone, Default)]
pub struct FlureeBuilder {
    config: ConnectionConfig,
    #[cfg(feature = "native")]
    storage_path: Option<String>,
    /// Optional encryption key (base64-encoded or raw 32 bytes)
    encryption_key: Option<[u8; 32]>,
    /// Optional ledger cache configuration (enables LedgerManager)
    ledger_cache_config: Option<LedgerManagerConfig>,
    /// Optional background indexing configuration.
    /// When set, `build()` will spawn a `BackgroundIndexerWorker`.
    indexing_config: Option<IndexingBuilderConfig>,
    /// Optional novelty backpressure thresholds (independent of background indexing).
    ///
    /// When set, these override the thresholds from `indexing_config` for
    /// `derive_indexing()`. Use `with_novelty_thresholds()` to set this without
    /// enabling background indexing — useful for CLI or embedded scenarios where
    /// the process is too short-lived for a background indexer.
    novelty_thresholds: Option<IndexConfig>,
    /// Remote Fluree connection registry for SERVICE federation.
    remote_connections: remote_service::RemoteConnectionRegistry,
}

/// Configuration for background indexing in `FlureeBuilder`.
#[derive(Debug, Clone)]
pub struct IndexingBuilderConfig {
    /// Controls index building parameters (leaf sizes, GC, memory budget).
    pub indexer_config: IndexerConfig,
    /// Controls novelty backpressure thresholds.
    pub index_config: IndexConfig,
}

/// Default `IndexingBuilderConfig` for persistent storage (`file`, `s3`, `ipfs`).
///
/// Uses [`server_defaults::default_index_config`] for the novelty thresholds
/// (RAM-tiered hard threshold), so persistent Fluree instances built
/// programmatically get the same production-sized backpressure as the server
/// binary. `IndexConfig` itself has no `Default` impl — configuration policy
/// lives here in the API layer, not in the lower-level `fluree-db-ledger` crate.
fn default_indexing_builder_config() -> IndexingBuilderConfig {
    IndexingBuilderConfig {
        indexer_config: IndexerConfig::default(),
        index_config: server_defaults::default_index_config(),
    }
}

fn make_leaflet_cache(
    config: &fluree_db_connection::ConnectionConfig,
) -> std::sync::Arc<fluree_db_binary_index::LeafletCache> {
    std::sync::Arc::new(fluree_db_binary_index::LeafletCache::with_max_mb(
        config.cache.max_mb as u64,
    ))
}

/// Build a `RemoteServiceExecutor` from the connection registry, if any connections are registered.
fn build_remote_service(
    registry: remote_service::RemoteConnectionRegistry,
) -> Option<Arc<dyn fluree_db_query::remote_service::RemoteServiceExecutor>> {
    if registry.is_empty() {
        return None;
    }
    #[cfg(feature = "search-remote-client")]
    {
        Some(Arc::new(remote_service::HttpRemoteService::new(Arc::new(
            registry,
        ))))
    }
    #[cfg(not(feature = "search-remote-client"))]
    {
        tracing::warn!(
            "Remote connections registered but 'search-remote-client' feature is not enabled. \
             Remote SERVICE queries will fail at runtime."
        );
        None
    }
}

/// Runtime components assembled by each `build_*` path before `Fluree` construction.
///
/// Grouped so the private `finalize`/`finalize_with_backend` helpers don't exceed clippy's
/// `too_many_arguments` threshold as the runtime grows. Builder-derived settings
/// (`ledger_cache_config`, `config`, `remote_connections`) stay as separate params
/// since they come directly from `self`.
struct RuntimeParts {
    backend: StorageBackend,
    nameservice: NameServiceMode,
    event_bus: Arc<fluree_db_nameservice::LedgerEventBus>,
    indexing_mode: tx::IndexingMode,
    index_config: IndexConfig,
}

impl FlureeBuilder {
    /// Create a new builder with default settings (memory storage).
    ///
    /// Equivalent to [`FlureeBuilder::memory()`] — ledger caching is enabled
    /// by default.
    pub fn new() -> Self {
        Self::memory()
    }

    /// Configure for file-based storage
    ///
    /// The path should be the root directory containing ledger data.
    ///
    /// Background indexing is enabled by default. Call [`without_indexing`] to
    /// opt out — typically only appropriate when a separate process (peer or
    /// dedicated indexer) owns index maintenance for this storage.
    ///
    /// [`without_indexing`]: FlureeBuilder::without_indexing
    #[cfg(feature = "native")]
    pub fn file(path: impl Into<String>) -> Self {
        let path = path.into();
        Self {
            config: ConnectionConfig::file(path.clone()),
            storage_path: Some(path),
            encryption_key: None,
            ledger_cache_config: Some(LedgerManagerConfig::default()),
            indexing_config: Some(default_indexing_builder_config()),
            novelty_thresholds: None,
            remote_connections: remote_service::RemoteConnectionRegistry::new(),
        }
    }

    /// Configure for memory-based storage
    pub fn memory() -> Self {
        Self {
            config: ConnectionConfig::memory(),
            #[cfg(feature = "native")]
            storage_path: None,
            encryption_key: None,
            ledger_cache_config: Some(LedgerManagerConfig::default()),
            indexing_config: None,
            novelty_thresholds: None,
            remote_connections: remote_service::RemoteConnectionRegistry::new(),
        }
    }

    /// Configure for S3-backed storage.
    ///
    /// This sets **index storage** to S3 and configures a **storage-backed nameservice**
    /// using the same S3 storage.
    ///
    /// Notes:
    /// - Requires the `aws` feature on `fluree-db-api`.
    /// - Region/credentials are resolved via the standard AWS SDK chain.
    /// - `endpoint` is required for parity (LocalStack/MinIO/AWS endpoints).
    #[cfg(feature = "aws")]
    pub fn s3(bucket: impl Into<String>, endpoint: impl Into<String>) -> Self {
        use fluree_db_connection::config::{PublisherConfig, PublisherType, S3StorageConfig};
        use fluree_db_connection::StorageConfig;

        let bucket = bucket.into();
        let endpoint = endpoint.into();

        let storage_id: Arc<str> = Arc::from("s3Storage");
        let s3 = S3StorageConfig {
            bucket: Arc::from(bucket),
            prefix: None,
            endpoint: Some(Arc::from(endpoint)),
            read_timeout_ms: None,
            write_timeout_ms: None,
            list_timeout_ms: None,
            max_retries: None,
            retry_base_delay_ms: None,
            retry_max_delay_ms: None,
            address_identifier: None,
        };

        let storage = StorageConfig {
            id: Some(storage_id.clone()),
            storage_type: StorageType::S3(s3),
            path: None,
            aes256_key: None,
            address_identifier: None,
        };

        let publisher = PublisherConfig {
            id: Some(Arc::from("primaryPublisher")),
            publisher_type: PublisherType::Storage {
                storage: storage.clone(),
            },
        };

        let config = ConnectionConfig {
            index_storage: storage,
            commit_storage: None,
            primary_publisher: Some(publisher),
            ..Default::default()
        };

        Self {
            config,
            #[cfg(feature = "native")]
            storage_path: None,
            encryption_key: None,
            ledger_cache_config: Some(LedgerManagerConfig::default()),
            indexing_config: Some(default_indexing_builder_config()),
            novelty_thresholds: None,
            remote_connections: remote_service::RemoteConnectionRegistry::new(),
        }
    }

    /// Set S3 key prefix (e.g. `"ledgers/prod"`).
    #[cfg(feature = "aws")]
    pub fn s3_prefix(mut self, prefix: impl Into<String>) -> Self {
        if let StorageType::S3(s3) = &mut self.config.index_storage.storage_type {
            s3.prefix = Some(Arc::from(prefix.into()));
        }
        self
    }

    /// Set S3 read timeout in milliseconds.
    #[cfg(feature = "aws")]
    pub fn s3_read_timeout_ms(mut self, ms: u64) -> Self {
        if let StorageType::S3(s3) = &mut self.config.index_storage.storage_type {
            s3.read_timeout_ms = Some(ms);
        }
        self
    }

    /// Set S3 write timeout in milliseconds.
    #[cfg(feature = "aws")]
    pub fn s3_write_timeout_ms(mut self, ms: u64) -> Self {
        if let StorageType::S3(s3) = &mut self.config.index_storage.storage_type {
            s3.write_timeout_ms = Some(ms);
        }
        self
    }

    /// Set S3 list timeout in milliseconds.
    #[cfg(feature = "aws")]
    pub fn s3_list_timeout_ms(mut self, ms: u64) -> Self {
        if let StorageType::S3(s3) = &mut self.config.index_storage.storage_type {
            s3.list_timeout_ms = Some(ms);
        }
        self
    }

    /// Set S3 max retries (retries after the initial attempt).
    #[cfg(feature = "aws")]
    pub fn s3_max_retries(mut self, n: u64) -> Self {
        if let StorageType::S3(s3) = &mut self.config.index_storage.storage_type {
            s3.max_retries = Some(n);
        }
        self
    }

    /// Set S3 retry base delay in milliseconds.
    #[cfg(feature = "aws")]
    pub fn s3_retry_base_delay_ms(mut self, ms: u64) -> Self {
        if let StorageType::S3(s3) = &mut self.config.index_storage.storage_type {
            s3.retry_base_delay_ms = Some(ms);
        }
        self
    }

    /// Set S3 retry max delay in milliseconds.
    #[cfg(feature = "aws")]
    pub fn s3_retry_max_delay_ms(mut self, ms: u64) -> Self {
        if let StorageType::S3(s3) = &mut self.config.index_storage.storage_type {
            s3.retry_max_delay_ms = Some(ms);
        }
        self
    }

    /// Set the encryption key for storage encryption.
    ///
    /// When set, all data will be encrypted using AES-256-GCM before being
    /// written to storage. The key must be exactly 32 bytes.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let key = [0u8; 32]; // Use a secure key in production
    /// let fluree = FlureeBuilder::file("/data")
    ///     .with_encryption_key(key)
    ///     .build_encrypted()?;
    /// ```
    pub fn with_encryption_key(mut self, key: [u8; 32]) -> Self {
        self.encryption_key = Some(key);
        self
    }

    /// Set the encryption key from a base64-encoded string.
    ///
    /// This is useful when loading keys from environment variables or config files.
    /// The decoded key must be exactly 32 bytes.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let fluree = FlureeBuilder::file("/data")
    ///     .with_encryption_key_base64("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")?
    ///     .build_encrypted()?;
    /// ```
    pub fn with_encryption_key_base64(mut self, base64_key: &str) -> Result<Self> {
        use base64::Engine;
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(base64_key)
            .map_err(|e| ApiError::config(format!("Invalid base64 encryption key: {e}")))?;

        if decoded.len() != 32 {
            return Err(ApiError::config(format!(
                "Encryption key must be 32 bytes, got {} bytes",
                decoded.len()
            )));
        }

        let mut key = [0u8; 32];
        key.copy_from_slice(&decoded);
        self.encryption_key = Some(key);
        Ok(self)
    }

    /// Create a builder from JSON-LD configuration.
    ///
    /// Parses a JSON-LD configuration document and extracts all settings including
    /// storage path, cache settings, encryption key, indexing config, address
    /// identifiers, and publisher/nameservice config.
    ///
    /// The returned builder can be further customized before calling a terminal
    /// build method (`build()`, `build_memory()`, `build_client()`, etc.).
    ///
    /// # Encryption Key from Environment
    ///
    /// The encryption key can be specified directly or via environment variable:
    ///
    /// ```json
    /// {
    ///   "@context": {"@vocab": "https://ns.flur.ee/system#"},
    ///   "@graph": [{
    ///     "@type": "Connection",
    ///     "indexStorage": {
    ///       "@type": "Storage",
    ///       "filePath": "/data/fluree",
    ///       "AES256Key": {"envVar": "FLUREE_ENCRYPTION_KEY"}
    ///     }
    ///   }]
    /// }
    /// ```
    ///
    /// The key should be base64-encoded, 32 bytes when decoded.
    pub fn from_json_ld(json: &serde_json::Value) -> Result<Self> {
        let config = ConnectionConfig::from_json_ld(json)
            .map_err(|e| ApiError::config(format!("Invalid JSON-LD config: {e}")))?;

        // Extract path from storage config (used by typed file builds)
        #[cfg(feature = "native")]
        let storage_path = config
            .index_storage
            .path
            .as_ref()
            .map(std::string::ToString::to_string);

        // Extract encryption key if configured
        let encryption_key = if let Some(key_str) = &config.index_storage.aes256_key {
            Some(Self::decode_encryption_key(key_str)?)
        } else {
            None
        };

        // Extract indexing config if enabled in JSON-LD defaults
        let indexing_config = if is_indexing_enabled(&config) {
            let indexer_config = build_indexer_config(&config);
            let index_config = derive_index_config(&config);
            Some(IndexingBuilderConfig {
                indexer_config,
                index_config,
            })
        } else {
            None
        };

        Ok(Self {
            config,
            #[cfg(feature = "native")]
            storage_path,
            encryption_key,
            ledger_cache_config: Some(LedgerManagerConfig::default()),
            indexing_config,
            novelty_thresholds: None,
            remote_connections: remote_service::RemoteConnectionRegistry::new(),
        })
    }

    /// Decode a base64-encoded encryption key to 32 bytes.
    fn decode_encryption_key(key_str: &str) -> Result<[u8; 32]> {
        use base64::Engine;
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(key_str)
            .map_err(|e| ApiError::config(format!("Invalid base64 encryption key: {e}")))?;

        if decoded.len() != 32 {
            return Err(ApiError::config(format!(
                "Encryption key must be 32 bytes, got {} bytes",
                decoded.len()
            )));
        }

        let mut key = [0u8; 32];
        key.copy_from_slice(&decoded);
        Ok(key)
    }

    /// Set the maximum cache size in MB.
    ///
    /// By default, cache size is calculated automatically based on 50% of
    /// available system memory. Use this method to override with a specific MB limit.
    pub fn cache_max_mb(mut self, max_mb: usize) -> Self {
        self.config.cache = fluree_db_connection::CacheConfig::with_max_mb(max_mb);
        self
    }

    /// Set the parallelism level
    pub fn parallelism(mut self, parallelism: usize) -> Self {
        self.config.parallelism = parallelism;
        self
    }

    /// Disable ledger caching.
    ///
    /// By default, ledger caching is enabled. Loaded ledgers are cached at
    /// the connection level, avoiding per-request ledger reloading. This
    /// is useful for long-running processes (servers, MCP, etc.).
    ///
    /// For one-shot CLI commands or short-lived processes, disabling the
    /// cache avoids unnecessary overhead (sweep task, bookkeeping).
    pub fn without_ledger_caching(mut self) -> Self {
        self.ledger_cache_config = None;
        self
    }

    /// Configure ledger caching with custom settings.
    ///
    /// Ledger caching is enabled by default with sensible defaults (30min TTL,
    /// 1min sweep). Use this to fine-tune:
    /// - `idle_ttl`: How long a ledger stays cached after last access
    /// - `sweep_interval`: How often the cache is checked for idle entries
    pub fn with_ledger_cache_config(mut self, config: LedgerManagerConfig) -> Self {
        self.ledger_cache_config = Some(config);
        self
    }

    /// Enable background indexing with default settings.
    ///
    /// Persistent builders (`file`, `s3`, `ipfs`) enable indexing by default,
    /// so this is mostly useful after [`without_indexing`] or on the `memory`
    /// builder. `build()` will spawn a `BackgroundIndexerWorker` that
    /// automatically indexes ledgers when novelty exceeds the soft threshold.
    /// Must be called within a tokio runtime context.
    ///
    /// [`without_indexing`]: FlureeBuilder::without_indexing
    pub fn with_indexing(mut self) -> Self {
        self.indexing_config = Some(default_indexing_builder_config());
        self
    }

    /// Disable background indexing on this builder.
    ///
    /// Persistent builders (`file`, `s3`, `ipfs`) enable indexing by default;
    /// call this to opt out. The only production reason to do so is when a
    /// separate process (a peer or dedicated indexer) owns index maintenance
    /// for the same storage — the transactor writes commits, the other process
    /// produces the index roots. Running without an indexer anywhere will
    /// accumulate novelty until the hard ceiling blocks writes.
    pub fn without_indexing(mut self) -> Self {
        self.indexing_config = None;
        self
    }

    /// Enable background indexing with custom novelty thresholds.
    ///
    /// - `min_bytes`: soft threshold — triggers background indexing
    /// - `max_bytes`: hard threshold — blocks commits until indexed
    pub fn with_indexing_thresholds(mut self, min_bytes: usize, max_bytes: usize) -> Self {
        let index_config = IndexConfig {
            reindex_min_bytes: min_bytes,
            reindex_max_bytes: max_bytes,
        };
        self.indexing_config = Some(IndexingBuilderConfig {
            indexer_config: self
                .indexing_config
                .map(|c| c.indexer_config)
                .unwrap_or_default(),
            index_config,
        });
        self
    }

    /// Set novelty backpressure thresholds without enabling background indexing.
    ///
    /// Use this for short-lived processes (CLI, one-shot scripts) that need
    /// the correct commit-blocking limits but exit before a background indexer
    /// could finish. The thresholds take priority over any values set via
    /// `with_indexing()` or `with_indexing_thresholds()`.
    pub fn with_novelty_thresholds(mut self, min_bytes: usize, max_bytes: usize) -> Self {
        self.novelty_thresholds = Some(IndexConfig {
            reindex_min_bytes: min_bytes,
            reindex_max_bytes: max_bytes,
        });
        self
    }

    /// Register a remote Fluree connection for SERVICE federation.
    ///
    /// The `name` is used in SPARQL queries as `SERVICE <fluree:remote:name/ledger> { ... }`.
    pub fn remote_connection(
        mut self,
        name: impl Into<String>,
        base_url: impl Into<String>,
        token: Option<String>,
    ) -> Self {
        self.remote_connections
            .register(name, remote_service::RemoteConnection::new(base_url, token));
        self
    }

    /// Build a file-backed Fluree instance
    ///
    /// Returns an error if storage_path is not set.
    ///
    /// Indexing is enabled by default for `file`-constructed builders; call
    /// `without_indexing()` to opt out (see that method for when that's
    /// appropriate). When indexing is enabled, a `BackgroundIndexerWorker` is
    /// spawned on the tokio runtime, so `build()` must be called within a
    /// tokio context.
    #[cfg(feature = "native")]
    pub fn build(mut self) -> Result<Fluree> {
        let path = self
            .storage_path
            .take()
            .ok_or_else(|| ApiError::config("File storage requires a path"))?;

        let storage = FileStorage::new(&path);
        let nameservice = FileNameService::new(&path);
        let event_bus = Arc::new(fluree_db_nameservice::LedgerEventBus::new(1024));
        let notifying =
            fluree_db_nameservice::NotifyingNameService::new(nameservice, event_bus.clone());
        let backend = StorageBackend::Managed(Arc::new(storage));
        let index_config = self.derive_indexing();
        let ns_mode = NameServiceMode::ReadWrite(Arc::new(notifying.clone()));
        let indexing_mode = self.start_background_indexing(&backend, &notifying);
        Ok(Self::finalize_with_backend(
            self.ledger_cache_config,
            self.config,
            RuntimeParts {
                backend,
                nameservice: ns_mode,
                event_bus,
                indexing_mode,
                index_config,
            },
            self.remote_connections,
        ))
    }

    /// Build a Fluree instance with custom storage and nameservice.
    ///
    /// Use this when you need a storage or nameservice backend that isn't
    /// covered by the built-in `build()` / `build_memory()` / `build_s3()`
    /// methods (e.g. proxy storage for peer mode).
    ///
    /// Honors the builder's cache and indexing settings.
    pub fn build_with(
        self,
        storage: impl Storage + 'static,
        nameservice: NameServiceMode,
    ) -> Fluree {
        let event_bus = Arc::new(fluree_db_nameservice::LedgerEventBus::new(1024));
        let index_config = self.derive_indexing();
        Self::finalize_with_backend(
            self.ledger_cache_config,
            self.config,
            RuntimeParts {
                backend: StorageBackend::Managed(Arc::new(storage)),
                nameservice,
                event_bus,
                indexing_mode: tx::IndexingMode::Disabled,
                index_config,
            },
            self.remote_connections,
        )
    }

    /// Build a file-backed Fluree instance with AES-256-GCM encryption.
    ///
    /// Uses the provided `key` argument for encryption. Any key previously set on the
    /// builder via `with_encryption_key()` or JSON-LD config is ignored.
    ///
    /// To use a key configured on the builder, use [`build_encrypted_from_config()`] instead.
    ///
    /// # Arguments
    ///
    /// * `key` - 32-byte AES-256 encryption key
    ///
    /// # Example
    ///
    /// ```ignore
    /// let key = [0u8; 32]; // Use a secure key in production
    /// let fluree = FlureeBuilder::file("/path/to/data")
    ///     .build_encrypted(key)?;
    /// ```
    ///
    /// # Security
    ///
    /// - Key material stored in `EncryptionKey` is zeroized on drop
    /// - Uses AES-256-GCM (authenticated encryption with integrity protection)
    /// - Each write uses a fresh random nonce
    /// - Encrypted data is portable between storage backends
    ///
    /// Note: The input `[u8; 32]` passed to this method is not automatically zeroized;
    /// callers should zeroize their own key copies if needed.
    #[cfg(feature = "native")]
    pub fn build_encrypted(self, key: [u8; 32]) -> Result<Fluree> {
        // Always use the explicitly provided key
        self.build_encrypted_internal(key)
    }

    /// Build a file-backed Fluree instance with encryption using the configured key.
    ///
    /// The encryption key must have been set via `with_encryption_key()`,
    /// `with_encryption_key_base64()`, or parsed from JSON-LD config.
    ///
    /// # Errors
    ///
    /// Returns an error if no encryption key has been configured.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // From JSON-LD config with environment variable
    /// let config = json!({
    ///     "@context": {"@vocab": "https://ns.flur.ee/system#"},
    ///     "@graph": [{
    ///         "@type": "Connection",
    ///         "indexStorage": {
    ///             "@type": "Storage",
    ///             "filePath": "/data/fluree",
    ///             "AES256Key": {"envVar": "FLUREE_ENCRYPTION_KEY"}
    ///         }
    ///     }]
    /// });
    /// let fluree = FlureeBuilder::from_json_ld(&config)?
    ///     .build_encrypted_from_config()?;
    /// ```
    #[cfg(feature = "native")]
    pub fn build_encrypted_from_config(self) -> Result<Fluree> {
        let key = self.encryption_key.ok_or_else(|| {
            ApiError::config("No encryption key configured. Set via with_encryption_key(), with_encryption_key_base64(), or AES256Key in JSON-LD config")
        })?;
        self.build_encrypted_internal(key)
    }

    /// Internal helper to build encrypted storage
    #[cfg(feature = "native")]
    fn build_encrypted_internal(mut self, key: [u8; 32]) -> Result<Fluree> {
        let path = self
            .storage_path
            .take()
            .ok_or_else(|| ApiError::config("File storage requires a path"))?;

        let file_storage = FileStorage::new(&path);
        let encryption_key = EncryptionKey::new(key, 0);
        let key_provider = StaticKeyProvider::new(encryption_key);
        let storage = EncryptedStorage::new(file_storage, key_provider);
        let nameservice = FileNameService::new(&path);
        let event_bus = Arc::new(fluree_db_nameservice::LedgerEventBus::new(1024));
        let notifying =
            fluree_db_nameservice::NotifyingNameService::new(nameservice, event_bus.clone());
        let index_config = self.derive_indexing();
        let backend = StorageBackend::Managed(Arc::new(storage));
        let ns_mode = NameServiceMode::ReadWrite(Arc::new(notifying.clone()));
        let indexing_mode = self.start_background_indexing(&backend, &notifying);
        Ok(Self::finalize_with_backend(
            self.ledger_cache_config,
            self.config,
            RuntimeParts {
                backend,
                nameservice: ns_mode,
                event_bus,
                indexing_mode,
                index_config,
            },
            self.remote_connections,
        ))
    }

    /// Check if this builder has an encryption key configured.
    pub fn has_encryption_key(&self) -> bool {
        self.encryption_key.is_some()
    }

    /// Build a memory-backed Fluree instance
    ///
    /// Background indexing is **always disabled** for this builder path
    /// regardless of `with_indexing()` — memory storage is intended for
    /// short-lived tests and scratch use where a background worker would
    /// outlive the `Fluree` handle. Use `set_indexing_mode` after building
    /// if you need it, or switch to a persistent builder (`file`, `s3`,
    /// `ipfs`), which enable indexing by default.
    pub fn build_memory(self) -> Fluree {
        let storage = MemoryStorage::new();
        let nameservice = MemoryNameService::new();
        let event_bus = Arc::new(fluree_db_nameservice::LedgerEventBus::new(1024));
        let notifying =
            fluree_db_nameservice::NotifyingNameService::new(nameservice, event_bus.clone());
        let ns_mode = NameServiceMode::ReadWrite(Arc::new(notifying));
        let index_config = self.derive_indexing();
        Self::finalize_with_backend(
            self.ledger_cache_config,
            self.config,
            RuntimeParts {
                backend: StorageBackend::Managed(Arc::new(storage)),
                nameservice: ns_mode,
                event_bus,
                indexing_mode: tx::IndexingMode::Disabled,
                index_config,
            },
            self.remote_connections,
        )
    }

    /// Build a memory-backed Fluree instance with AES-256-GCM encryption
    ///
    /// Useful for testing encryption without touching the filesystem.
    ///
    /// # Arguments
    ///
    /// * `key` - 32-byte AES-256 encryption key
    pub fn build_memory_encrypted(self, key: [u8; 32]) -> Fluree {
        let mem_storage = MemoryStorage::new();
        let encryption_key = EncryptionKey::new(key, 0);
        let key_provider = StaticKeyProvider::new(encryption_key);
        let storage = EncryptedStorage::new(mem_storage, key_provider);
        let nameservice = MemoryNameService::new();
        let event_bus = Arc::new(fluree_db_nameservice::LedgerEventBus::new(1024));
        let notifying =
            fluree_db_nameservice::NotifyingNameService::new(nameservice, event_bus.clone());
        let ns_mode = NameServiceMode::ReadWrite(Arc::new(notifying));
        let index_config = self.derive_indexing();
        Self::finalize_with_backend(
            self.ledger_cache_config,
            self.config,
            RuntimeParts {
                backend: StorageBackend::Managed(Arc::new(storage)),
                nameservice: ns_mode,
                event_bus,
                indexing_mode: tx::IndexingMode::Disabled,
                index_config,
            },
            self.remote_connections,
        )
    }

    /// Build an IPFS-backed Fluree instance with an in-memory nameservice.
    ///
    /// Stores content-addressed data (commits, indexes) in IPFS via the Kubo
    /// HTTP RPC API. The nameservice is in-memory only — ledger heads and
    /// branch metadata do not persist across restarts. For persistent
    /// nameservice, compose your own with [`build_with`] using
    /// [`fluree_db_storage_ipfs::IpfsStorage`].
    ///
    /// # Arguments
    ///
    /// * `api_url` - Kubo RPC API base URL (e.g., `"http://127.0.0.1:5001"`)
    ///
    /// # Notes
    ///
    /// - Requires the `ipfs` feature.
    /// - Background indexing is supported. GC unpins replaced CIDs so Kubo's
    ///   garbage collector can reclaim them.
    /// - Admin operations that require prefix listing (e.g., fast-path ledger
    ///   drop) fall back to CID-walking, which is slower but correct.
    ///
    /// [`build_with`]: FlureeBuilder::build_with
    #[cfg(feature = "ipfs")]
    pub fn build_ipfs(self, api_url: impl Into<String>) -> Fluree {
        use fluree_db_storage_ipfs::{IpfsConfig, IpfsStorage};
        let ipfs_store = IpfsStorage::new(IpfsConfig {
            api_url: api_url.into(),
            pin_on_put: true,
        });
        let backend = StorageBackend::Permanent(Arc::new(ipfs_store));
        let nameservice = MemoryNameService::new();
        let event_bus = Arc::new(fluree_db_nameservice::LedgerEventBus::new(1024));
        let notifying =
            fluree_db_nameservice::NotifyingNameService::new(nameservice, event_bus.clone());
        let ns_mode = NameServiceMode::ReadWrite(Arc::new(notifying.clone()));
        let index_config = self.derive_indexing();
        let indexing_mode = self.start_background_indexing(&backend, &notifying);
        Self::finalize_with_backend(
            self.ledger_cache_config,
            self.config,
            RuntimeParts {
                backend,
                nameservice: ns_mode,
                event_bus,
                indexing_mode,
                index_config,
            },
            self.remote_connections,
        )
    }

    /// Build an S3-backed Fluree instance (storage-backed nameservice).
    ///
    /// Convenience wrapper around JSON-LD config for S3-backed storage.
    ///
    /// Notes:
    /// - Requires the `aws` feature.
    /// - Uses the AWS default credential/region chain.
    /// - Ledger caching is enabled when `ledger_cache_config` is set on the builder.
    #[cfg(feature = "aws")]
    pub async fn build_s3(self) -> Result<Fluree> {
        use fluree_db_connection::aws;
        use fluree_db_connection::config::S3StorageConfig;
        use fluree_db_storage_aws::{S3Config, S3Storage};

        let s3_cfg: &S3StorageConfig = match &self.config.index_storage.storage_type {
            StorageType::S3(s3) => s3,
            _ => {
                return Err(ApiError::config(
                    "build_s3 requires FlureeBuilder::s3(...) or an S3 indexStorage config",
                ))
            }
        };

        let timeout_ms = s3_cfg
            .read_timeout_ms
            .into_iter()
            .chain(s3_cfg.write_timeout_ms)
            .chain(s3_cfg.list_timeout_ms)
            .max();

        let sdk_config = aws::get_or_init_sdk_config().await?;

        let storage = S3Storage::new(
            sdk_config,
            S3Config {
                bucket: s3_cfg.bucket.to_string(),
                prefix: s3_cfg.prefix.as_ref().map(std::string::ToString::to_string),
                endpoint: s3_cfg
                    .endpoint
                    .as_ref()
                    .map(std::string::ToString::to_string),
                timeout_ms,
                max_retries: s3_cfg.max_retries.map(|n| n as u32),
                retry_base_delay_ms: s3_cfg.retry_base_delay_ms,
                retry_max_delay_ms: s3_cfg.retry_max_delay_ms,
            },
        )
        .await
        .map_err(|e| ApiError::config(format!("Failed to create S3 storage: {e}")))?;

        // Empty prefix: S3Storage already applies its own key prefix.
        let nameservice = StorageNameService::new(storage.clone(), "");
        let event_bus = Arc::new(fluree_db_nameservice::LedgerEventBus::new(1024));
        let notifying =
            fluree_db_nameservice::NotifyingNameService::new(nameservice, event_bus.clone());
        let ns_mode = NameServiceMode::ReadWrite(Arc::new(notifying.clone()));
        let index_config = self.derive_indexing();
        let backend = StorageBackend::Managed(Arc::new(storage));
        let indexing_mode = self.start_background_indexing(&backend, &notifying);
        Ok(Self::finalize_with_backend(
            self.ledger_cache_config,
            self.config,
            RuntimeParts {
                backend,
                nameservice: ns_mode,
                event_bus,
                indexing_mode,
                index_config,
            },
            self.remote_connections,
        ))
    }

    /// Build an S3-backed Fluree instance with AES-256-GCM encryption.
    ///
    /// All data written to S3 is transparently encrypted before upload,
    /// and decrypted on read.
    ///
    /// Notes:
    /// - Requires the `aws` feature.
    /// - Uses the AWS default credential/region chain.
    /// - Ledger caching is enabled when `ledger_cache_config` is set on the builder.
    ///
    /// # Arguments
    ///
    /// * `key` - 32-byte AES-256 encryption key
    #[cfg(feature = "aws")]
    pub async fn build_s3_encrypted(self, key: [u8; 32]) -> Result<Fluree> {
        use fluree_db_connection::aws;
        use fluree_db_connection::config::S3StorageConfig;
        use fluree_db_storage_aws::{S3Config, S3Storage};

        let s3_cfg: &S3StorageConfig = match &self.config.index_storage.storage_type {
            StorageType::S3(s3) => s3,
            _ => return Err(ApiError::config(
                "build_s3_encrypted requires FlureeBuilder::s3(...) or an S3 indexStorage config",
            )),
        };

        let timeout_ms = s3_cfg
            .read_timeout_ms
            .into_iter()
            .chain(s3_cfg.write_timeout_ms)
            .chain(s3_cfg.list_timeout_ms)
            .max();

        let sdk_config = aws::get_or_init_sdk_config().await?;

        let s3_storage = S3Storage::new(
            sdk_config,
            S3Config {
                bucket: s3_cfg.bucket.to_string(),
                prefix: s3_cfg.prefix.as_ref().map(std::string::ToString::to_string),
                endpoint: s3_cfg
                    .endpoint
                    .as_ref()
                    .map(std::string::ToString::to_string),
                timeout_ms,
                max_retries: s3_cfg.max_retries.map(|n| n as u32),
                retry_base_delay_ms: s3_cfg.retry_base_delay_ms,
                retry_max_delay_ms: s3_cfg.retry_max_delay_ms,
            },
        )
        .await
        .map_err(|e| ApiError::config(format!("Failed to create S3 storage: {e}")))?;

        // Wrap with encryption
        let encryption_key = EncryptionKey::new(key, 0);
        let key_provider = StaticKeyProvider::new(encryption_key);
        let storage = EncryptedStorage::new(s3_storage, key_provider);

        // Empty prefix: S3Storage already applies its own key prefix.
        let nameservice = StorageNameService::new(storage.clone(), "");
        let event_bus = Arc::new(fluree_db_nameservice::LedgerEventBus::new(1024));
        let notifying =
            fluree_db_nameservice::NotifyingNameService::new(nameservice, event_bus.clone());
        let ns_mode = NameServiceMode::ReadWrite(Arc::new(notifying.clone()));
        let index_config = self.derive_indexing();
        let backend = StorageBackend::Managed(Arc::new(storage));
        let indexing_mode = self.start_background_indexing(&backend, &notifying);
        Ok(Self::finalize_with_backend(
            self.ledger_cache_config,
            self.config,
            RuntimeParts {
                backend,
                nameservice: ns_mode,
                event_bus,
                indexing_mode,
                index_config,
            },
            self.remote_connections,
        ))
    }

    // ========================================================================
    // Shared finalization — single source of truth for caching + assembly
    // ========================================================================

    /// Extract the `IndexConfig` from builder settings (no runtime required).
    ///
    /// Priority: `novelty_thresholds` > `indexing_config` thresholds > defaults.
    fn derive_indexing(&self) -> IndexConfig {
        if let Some(ref thresholds) = self.novelty_thresholds {
            return thresholds.clone();
        }
        self.indexing_config
            .as_ref()
            .map(|c| c.index_config.clone())
            .unwrap_or_else(server_defaults::default_index_config)
    }

    /// Spawn the background indexer worker if configured.
    ///
    /// Must be called within a tokio runtime context.
    fn start_background_indexing<N>(
        &self,
        backend: &StorageBackend,
        nameservice: &N,
    ) -> tx::IndexingMode
    where
        N: NameService + fluree_db_nameservice::Publisher + Clone + 'static,
    {
        self.start_background_indexing_dyn(backend, Arc::new(nameservice.clone()))
    }

    /// Spawn the background indexer with an already-`Arc`'d nameservice.
    ///
    /// Used by AWS paths where the nameservice is already type-erased behind
    /// an `Arc<dyn ReadWriteNameService>`.
    fn start_background_indexing_dyn(
        &self,
        backend: &StorageBackend,
        nameservice: Arc<dyn fluree_db_nameservice::ReadWriteNameService>,
    ) -> tx::IndexingMode {
        if let Some(ref idx_config) = self.indexing_config {
            // Attach an api-side full-text config provider so each index
            // build refreshes `fulltext_configured_properties` from the
            // live ledger. Without this, background / CLI incremental
            // runs would silently drop configured plain-string values
            // committed after the connection started. 64MB cache matches
            // the default `LeafletCache` budget in `load_per_graph_arenas`.
            let ns_for_provider: Arc<dyn fluree_db_nameservice::NameService> =
                Arc::clone(&nameservice) as _;
            let provider = Arc::new(
                crate::indexer_fulltext_provider::ApiFulltextConfigProvider {
                    backend: backend.clone(),
                    nameservice: ns_for_provider,
                    leaflet_cache: Arc::new(fluree_db_binary_index::LeafletCache::with_max_mb(64)),
                    cache_dir: self
                        .ledger_cache_config
                        .as_ref()
                        .map(|config| config.cache_dir.clone())
                        .unwrap_or_else(|| LedgerManagerConfig::default().cache_dir),
                },
            ) as Arc<dyn fluree_db_indexer::FulltextConfigProvider>;
            let indexer_config = idx_config
                .indexer_config
                .clone()
                .with_fulltext_config_provider(provider);
            let (worker, handle) =
                BackgroundIndexerWorker::new(backend.clone(), nameservice, indexer_config);
            tokio::spawn(worker.run());
            tx::IndexingMode::Background(handle)
        } else {
            tx::IndexingMode::Disabled
        }
    }

    /// Assemble a `Fluree` with the builder's caching config.
    ///
    /// This is the **single source of truth** for:
    /// - LeafletCache creation
    /// - LedgerManager wiring (from `ledger_cache_config`)
    /// - R2RML cache creation
    /// - Final struct assembly
    ///
    /// Shared finalize logic taking a pre-built `StorageBackend` + runtime bundle.
    fn finalize_with_backend(
        ledger_cache_config: Option<LedgerManagerConfig>,
        config: ConnectionConfig,
        parts: RuntimeParts,
        remote_connections: remote_service::RemoteConnectionRegistry,
    ) -> Fluree {
        let RuntimeParts {
            backend,
            nameservice,
            event_bus,
            indexing_mode,
            index_config,
        } = parts;
        let leaflet_cache = make_leaflet_cache(&config);

        let ledger_manager = ledger_cache_config.map(|mut lm_config| {
            if lm_config.leaflet_cache.is_none() {
                lm_config.leaflet_cache = Some(std::sync::Arc::clone(&leaflet_cache));
            }
            Arc::new(LedgerManager::new(
                backend.clone(),
                nameservice.clone(),
                lm_config,
            ))
        });

        Fluree {
            config,
            backend,
            nameservice_mode: nameservice,
            leaflet_cache,
            indexing_mode,
            index_config,
            r2rml_cache: std::sync::Arc::new(graph_source::R2rmlCache::with_defaults()),
            event_bus,
            ledger_manager,
            remote_service: build_remote_service(remote_connections),
        }
    }

    /// Build a type-erased `FlureeClient` from the builder configuration.
    ///
    /// This is the dynamic counterpart to `build()` / `build_memory()` / `build_s3()`.
    /// It handles the full configuration surface including:
    /// - Storage backend selection (memory, file, S3) based on `ConnectionConfig`
    /// - Encryption (wraps storage if encryption key is configured)
    /// - Address identifier routing (multi-storage read routing)
    /// - Tiered commit/index storage (S3: separate buckets for reads vs writes)
    /// - Nameservice selection based on publisher config
    /// - Ledger caching (enabled by default)
    /// - Background indexing (if configured)
    ///
    /// Use this when the storage backend is determined at runtime (e.g., from
    /// JSON-LD config). For compile-time-known backends, prefer the typed build
    /// methods for better type safety.
    pub async fn build_client(self) -> Result<FlureeClient> {
        // --- S3 / AWS path (async-only) ---
        #[cfg(feature = "aws")]
        if matches!(self.config.index_storage.storage_type, StorageType::S3(_)) {
            return self.build_client_s3().await;
        }

        // --- Local (memory/filesystem) ---
        match &self.config.index_storage.storage_type {
            StorageType::Memory => self.build_client_memory(),
            StorageType::File => self.build_client_file(),
            StorageType::S3(_) => Err(ApiError::config(
                "S3 storage requires the 'aws' feature on fluree-db-api",
            )),
            StorageType::Unsupported { type_iri, .. } => Err(ApiError::config(format!(
                "Unsupported storage type: {type_iri}"
            ))),
        }
    }

    /// Build a type-erased memory-backed client.
    fn build_client_memory(self) -> Result<FlureeClient> {
        let base_storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new());

        // Wrap with address identifier routing if configured
        let storage = self.wrap_address_identifiers(base_storage)?;

        let nameservice = MemoryNameService::new();
        let event_bus = Arc::new(fluree_db_nameservice::LedgerEventBus::new(1024));
        let notifying =
            fluree_db_nameservice::NotifyingNameService::new(nameservice, event_bus.clone());
        let ns_mode = NameServiceMode::ReadWrite(Arc::new(notifying.clone()));

        let index_config = self.derive_indexing();
        let backend = StorageBackend::Managed(storage);
        let indexing_mode = self.start_background_indexing(&backend, &notifying);
        Ok(Self::finalize_with_backend(
            self.ledger_cache_config,
            self.config,
            RuntimeParts {
                backend,
                nameservice: ns_mode,
                event_bus,
                indexing_mode,
                index_config,
            },
            self.remote_connections,
        ))
    }

    /// Build a type-erased file-backed client.
    fn build_client_file(self) -> Result<FlureeClient> {
        #[cfg(not(feature = "native"))]
        {
            Err(ApiError::config(
                "Filesystem storage requires the 'native' feature",
            ))
        }
        #[cfg(feature = "native")]
        {
            let path = self
                .config
                .index_storage
                .path
                .as_ref()
                .ok_or_else(|| ApiError::config("File storage requires filePath"))?
                .clone();

            let file_storage = FileStorage::new(path.as_ref());
            let base_storage: Arc<dyn Storage> = if let Some(key) = self.encryption_key {
                let encryption_key = EncryptionKey::new(key, 0);
                let key_provider = StaticKeyProvider::new(encryption_key);
                Arc::new(EncryptedStorage::new(file_storage, key_provider))
            } else {
                Arc::new(file_storage)
            };

            // Wrap with address identifier routing if configured
            let storage = self.wrap_address_identifiers(base_storage)?;

            let nameservice = FileNameService::new(path.as_ref());
            let event_bus = Arc::new(fluree_db_nameservice::LedgerEventBus::new(1024));
            let notifying =
                fluree_db_nameservice::NotifyingNameService::new(nameservice, event_bus.clone());
            let ns_mode = NameServiceMode::ReadWrite(Arc::new(notifying.clone()));

            let index_config = self.derive_indexing();
            let backend = StorageBackend::Managed(storage);
            let indexing_mode = self.start_background_indexing(&backend, &notifying);
            Ok(Self::finalize_with_backend(
                self.ledger_cache_config,
                self.config,
                RuntimeParts {
                    backend,
                    nameservice: ns_mode,
                    event_bus,
                    indexing_mode,
                    index_config,
                },
                self.remote_connections,
            ))
        }
    }

    /// Build a type-erased S3-backed client.
    #[cfg(feature = "aws")]
    async fn build_client_s3(self) -> Result<FlureeClient> {
        // Delegate to fluree_db_connection for AWS SDK init,
        // storage registry sharing, and nameservice creation.
        let handle = fluree_db_connection::connect_from_config(self.config.clone()).await?;
        let fluree_db_connection::ConnectionHandle::Aws(aws_handle) = handle else {
            return Err(ApiError::config(
                "Expected AWS connection handle for S3 config",
            ));
        };

        // Decide whether to use tiered commit/index routing.
        let index = aws_handle.index_storage().clone();
        let commit = aws_handle.commit_storage().clone();
        let base_storage: Arc<dyn Storage> =
            if index.bucket() != commit.bucket() || index.prefix() != commit.prefix() {
                Arc::new(TieredStorage::new(commit, index))
            } else {
                Arc::new(index)
            };

        // Wrap with address identifier routing if configured
        let storage = self
            .wrap_address_identifiers_aws(base_storage, aws_handle.config())
            .await?;

        let ns_arc: Arc<dyn NameServicePublisher> = aws_handle.nameservice_arc().clone();
        let event_bus = Arc::new(fluree_db_nameservice::LedgerEventBus::new(1024));
        let ns_mode = NameServiceMode::ReadWrite(ns_arc.clone());

        let index_config = self.derive_indexing();
        let backend = StorageBackend::Managed(storage);
        let ns_rw: Arc<dyn fluree_db_nameservice::ReadWriteNameService> =
            aws_handle.nameservice_arc().clone();
        let indexing_mode = self.start_background_indexing_dyn(&backend, ns_rw);
        Ok(Self::finalize_with_backend(
            self.ledger_cache_config,
            aws_handle.config().clone(),
            RuntimeParts {
                backend,
                nameservice: ns_mode,
                event_bus,
                indexing_mode,
                index_config,
            },
            self.remote_connections,
        ))
    }

    /// Wrap base storage with address identifier routing for local backends.
    fn wrap_address_identifiers(&self, base_storage: Arc<dyn Storage>) -> Result<Arc<dyn Storage>> {
        if let Some(addr_ids) = &self.config.address_identifiers {
            let mut identifier_map = std::collections::HashMap::new();
            for (identifier, storage_config) in addr_ids {
                let id_storage = build_local_storage_from_config(storage_config)?;
                identifier_map.insert(identifier.to_string(), id_storage);
            }
            Ok(Arc::new(AddressIdentifierResolverStorage::new(
                base_storage,
                identifier_map,
            )))
        } else {
            Ok(base_storage)
        }
    }

    /// Wrap base storage with address identifier routing for AWS backends.
    #[cfg(feature = "aws")]
    async fn wrap_address_identifiers_aws(
        &self,
        base_storage: Arc<dyn Storage>,
        config: &ConnectionConfig,
    ) -> Result<Arc<dyn Storage>> {
        if let Some(addr_ids) = &config.address_identifiers {
            let mut identifier_map = std::collections::HashMap::new();
            for (identifier, storage_config) in addr_ids {
                let id_storage: Arc<dyn Storage> = match &storage_config.storage_type {
                    StorageType::S3(_) => build_s3_storage_from_config(storage_config).await?,
                    _ => build_local_storage_from_config(storage_config)?,
                };
                identifier_map.insert(identifier.to_string(), id_storage);
            }
            Ok(Arc::new(AddressIdentifierResolverStorage::new(
                base_storage,
                identifier_map,
            )))
        } else {
            Ok(base_storage)
        }
    }
}

/// Main Fluree API entry point
///
/// Combines connection management, nameservice, and query execution
/// into a unified interface.
pub struct Fluree {
    /// Connection configuration
    config: ConnectionConfig,
    /// Storage backend (managed or permanent).
    backend: StorageBackend,
    /// Nameservice for ledger discovery and publishing.
    nameservice_mode: NameServiceMode,
    /// Shared global cache for decoded index artifacts (one budget).
    leaflet_cache: std::sync::Arc<fluree_db_binary_index::LeafletCache>,
    /// Indexing mode (disabled or background with handle)
    pub indexing_mode: tx::IndexingMode,
    /// Novelty backpressure thresholds used by commits and soft-trigger logic.
    ///
    /// Set from `FlureeBuilder::with_indexing_thresholds()` for builder paths,
    /// or derived from `ConnectionConfig::defaults.indexing` for JSON-LD paths.
    index_config: IndexConfig,
    /// R2RML cache for compiled mappings and table metadata
    r2rml_cache: std::sync::Arc<graph_source::R2rmlCache>,
    /// In-process event bus for ledger/graph-source change notifications.
    event_bus: Arc<fluree_db_nameservice::LedgerEventBus>,
    /// Ledger manager for connection-level caching (enabled by default).
    ///
    /// Loaded ledgers are cached for reuse across queries and transactions.
    /// Disabled via `FlureeBuilder::without_ledger_caching()` for one-shot use.
    ledger_manager: Option<Arc<LedgerManager>>,
    /// Remote SERVICE executor for `fluree:remote:` federation.
    ///
    /// Populated from `FlureeBuilder::remote_connection()`. When `Some`,
    /// the executor is passed to `ContextConfig` and made available to
    /// `ServiceOperator` during query execution.
    remote_service: Option<Arc<dyn fluree_db_query::remote_service::RemoteServiceExecutor>>,
}

impl Fluree {
    /// Create a new Fluree instance with custom components
    ///
    /// Most users should use `FlureeBuilder` instead.
    pub fn new(
        config: ConnectionConfig,
        storage: impl Storage + 'static,
        nameservice: NameServiceMode,
    ) -> Self {
        Self::from_backend(
            config,
            StorageBackend::Managed(Arc::new(storage)),
            nameservice,
        )
    }

    /// Create a new Fluree instance from a pre-built `StorageBackend`.
    pub fn from_backend(
        config: ConnectionConfig,
        backend: StorageBackend,
        nameservice: NameServiceMode,
    ) -> Self {
        let leaflet_cache = make_leaflet_cache(&config);
        Self {
            config,
            backend,
            nameservice_mode: nameservice,
            leaflet_cache,
            indexing_mode: tx::IndexingMode::Disabled,
            index_config: server_defaults::default_index_config(),
            r2rml_cache: std::sync::Arc::new(graph_source::R2rmlCache::with_defaults()),
            event_bus: Arc::new(fluree_db_nameservice::LedgerEventBus::new(1024)),
            ledger_manager: None,
            remote_service: None,
        }
    }

    /// Create a new Fluree instance with a specific indexing mode
    pub fn with_indexing_mode(
        config: ConnectionConfig,
        storage: impl Storage + 'static,
        nameservice: NameServiceMode,
        indexing_mode: tx::IndexingMode,
    ) -> Self {
        let leaflet_cache = make_leaflet_cache(&config);
        Self {
            config,
            backend: StorageBackend::Managed(Arc::new(storage)),
            nameservice_mode: nameservice,
            leaflet_cache,
            indexing_mode,
            index_config: server_defaults::default_index_config(),
            r2rml_cache: std::sync::Arc::new(graph_source::R2rmlCache::with_defaults()),
            event_bus: Arc::new(fluree_db_nameservice::LedgerEventBus::new(1024)),
            ledger_manager: None,
            remote_service: None,
        }
    }

    /// Set the indexing mode
    pub fn set_indexing_mode(&mut self, mode: tx::IndexingMode) {
        self.indexing_mode = mode;
    }

    /// Get the remote SERVICE executor, if configured.
    pub fn remote_service_executor(
        &self,
    ) -> Option<&dyn fluree_db_query::remote_service::RemoteServiceExecutor> {
        self.remote_service.as_deref()
    }

    /// Set a custom remote SERVICE executor (for testing or advanced use).
    pub fn set_remote_service(
        &mut self,
        executor: Arc<dyn fluree_db_query::remote_service::RemoteServiceExecutor>,
    ) {
        self.remote_service = Some(executor);
    }

    /// Returns the novelty backpressure thresholds for this instance.
    ///
    /// Set from `FlureeBuilder::with_indexing_thresholds()` for builder paths,
    /// or derived from `ConnectionConfig::defaults.indexing` for JSON-LD paths.
    pub(crate) fn default_index_config(&self) -> IndexConfig {
        self.index_config.clone()
    }

    /// Check whether indexing is enabled in connection defaults.
    ///
    /// Defaults to `true` if not explicitly configured.
    pub(crate) fn defaults_indexing_enabled(&self) -> bool {
        self.config
            .defaults
            .as_ref()
            .and_then(|d| d.indexing.as_ref())
            .and_then(|i| i.indexing_enabled)
            .unwrap_or(true)
    }

    /// Get read-only nameservice access (always available).
    pub fn nameservice(&self) -> &dyn NameService {
        self.nameservice_mode.reader()
    }

    /// Get read-write nameservice access, or error if read-only.
    pub fn publisher(&self) -> Result<&dyn NameServicePublisher> {
        self.nameservice_mode
            .publisher()
            .ok_or_else(|| ApiError::internal("write operations require a read-write nameservice"))
    }

    /// Get the raw nameservice mode (for mode checks or `publisher_arc()`).
    pub fn nameservice_mode(&self) -> &NameServiceMode {
        &self.nameservice_mode
    }

    /// Get the in-process event bus for subscribing to ledger/graph-source changes.
    pub fn event_bus(&self) -> &Arc<fluree_db_nameservice::LedgerEventBus> {
        &self.event_bus
    }

    /// Get a reference to the connection config
    pub fn config(&self) -> &ConnectionConfig {
        &self.config
    }

    /// Get a reference to the storage backend.
    pub fn backend(&self) -> &StorageBackend {
        &self.backend
    }

    /// Get a content store scoped to the given namespace/ledger ID.
    pub fn content_store(&self, namespace_id: &str) -> Arc<dyn ContentStore> {
        self.backend.content_store(namespace_id)
    }

    /// Get a content store for `ledger_id` that walks branch ancestry on
    /// read miss.
    ///
    /// For non-branched ledgers this is identical to [`Self::content_store`];
    /// the only added cost is a single nameservice lookup. For branched
    /// ledgers it returns a `BranchedContentStore` that resolves pre-fork
    /// commits from the source branch's namespace, which a flat
    /// branch-scoped store cannot do.
    ///
    /// Use this on any path that walks the commit chain (catch-up,
    /// incremental indexing, full rebuild). Per-query reads against an
    /// already-loaded `LedgerState` do not need it — the branched store
    /// is already wired up by [`fluree_db_ledger::LedgerState::load`].
    pub async fn branched_content_store(&self, ledger_id: &str) -> Result<Arc<dyn ContentStore>> {
        Ok(fluree_db_nameservice::branched_content_store_for_id(
            &self.backend,
            self.nameservice_mode.reader(),
            ledger_id,
        )
        .await?)
    }

    /// Resolve a content store from an `Option<NsRecord>`, falling back
    /// to the flat namespace store keyed by `fallback_id` when no record
    /// is present.
    ///
    /// This collapses the recurring `match record { Some(...) => ..., None
    /// => ... }` pattern at every site that wants a branch-aware store
    /// when an `NsRecord` is in scope but may not be loaded yet.
    pub(crate) async fn content_store_for_record_or_id(
        &self,
        record: Option<&fluree_db_nameservice::NsRecord>,
        fallback_id: &str,
    ) -> Result<Arc<dyn ContentStore>> {
        Ok(fluree_db_nameservice::content_store_for_record_or_id(
            &self.backend,
            self.nameservice_mode.reader(),
            record,
            fallback_id,
        )
        .await?)
    }

    /// Read and parse a ledger's `default_context` blob from CAS via a
    /// branch-aware store. Returns `Ok(None)` when the record has no
    /// `default_context` CID set; returns `Err` on read or parse failure.
    /// Callers that want soft-fail behavior should match on the result.
    pub(crate) async fn load_default_context_blob(
        &self,
        record: &fluree_db_nameservice::NsRecord,
    ) -> Result<Option<serde_json::Value>> {
        Ok(fluree_db_nameservice::load_default_context_blob(
            &self.backend,
            self.nameservice_mode.reader(),
            record,
        )
        .await?)
    }

    /// Build a [`fluree_db_indexer::FulltextConfigProvider`] backed by this
    /// connection's storage + nameservice. Attach it to the indexer's
    /// `IndexerConfig` (via `with_fulltext_config_provider`) so every index
    /// build — including CLI-driven incremental runs — refreshes the
    /// configured full-text property set from the live ledger's
    /// `f:fullTextDefaults`.
    ///
    /// The background indexer constructed at `FlureeBuilder::build()` time
    /// already attaches one of these automatically; external callers
    /// invoking `fluree_db_indexer::build_index_for_ledger` directly should
    /// attach their own by calling this method.
    pub fn fulltext_config_provider(&self) -> Arc<dyn fluree_db_indexer::FulltextConfigProvider> {
        Arc::new(
            crate::indexer_fulltext_provider::ApiFulltextConfigProvider {
                backend: self.backend.clone(),
                nameservice: self.nameservice_mode.as_arc_reader(),
                leaflet_cache: Arc::clone(&self.leaflet_cache),
                cache_dir: self.binary_store_cache_dir(),
            },
        )
    }

    /// Get the raw address-based storage for admin/GC operations.
    ///
    /// Returns `None` for `Permanent` (IPFS) backends, which do not
    /// support address-based listing or deletion.
    pub fn admin_storage(&self) -> Option<&dyn Storage> {
        self.backend.admin_storage()
    }

    /// Get a reference to the R2RML cache
    pub fn r2rml_cache(&self) -> &std::sync::Arc<graph_source::R2rmlCache> {
        &self.r2rml_cache
    }

    /// Get a reference to the global decoded-artifact cache.
    pub fn leaflet_cache(&self) -> &std::sync::Arc<fluree_db_binary_index::LeafletCache> {
        &self.leaflet_cache
    }

    /// Global cache budget in MB.
    pub fn cache_budget_mb(&self) -> usize {
        self.config().cache.max_mb
    }

    /// Check if ledger caching is enabled (true by default).
    pub fn is_caching_enabled(&self) -> bool {
        self.ledger_manager.is_some()
    }

    /// Get the ledger manager (if caching is enabled)
    pub fn ledger_manager(&self) -> Option<&Arc<LedgerManager>> {
        self.ledger_manager.as_ref()
    }
}

impl Fluree {
    /// Resolve the binary-store disk cache directory for this instance.
    ///
    /// When ledger caching is enabled, binary-store reloads must use the
    /// manager's configured cache dir so post-commit namespace repair attaches
    /// into the same on-disk layout as normal ledger loads.
    pub(crate) fn binary_store_cache_dir(&self) -> std::path::PathBuf {
        self.ledger_manager
            .as_ref()
            .map(|mgr| mgr.config().cache_dir.clone())
            .unwrap_or_else(|| LedgerManagerConfig::default().cache_dir)
    }
}

impl Fluree {
    /// Create a builder for a new ledger.
    ///
    /// Returns a [`CreateBuilder`] that supports `.import(path)` for bulk import
    /// or can be extended for other creation patterns.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Bulk import from TTL chunks
    /// let result = fluree.create("mydb")
    ///     .import("/data/chunks/")
    ///     .threads(8)
    ///     .execute()
    ///     .await?;
    ///
    /// // Query normally after import
    /// let view = fluree.db("mydb").await?;
    /// let qr = fluree.query(&view, "SELECT * WHERE { ?s ?p ?o } LIMIT 10").await?;
    /// ```
    pub fn create(&self, ledger_id: &str) -> import::CreateBuilder<'_> {
        import::CreateBuilder::new(self, ledger_id.to_string())
    }

    /// Create a lazy graph handle for a ledger at the latest head.
    ///
    /// No I/O occurs until a terminal method is called (`.load()`,
    /// `.query().execute()`, `.transact().commit()`).
    ///
    /// # Examples
    ///
    /// ```ignore
    /// // Lazy query
    /// let result = fluree
    ///     .graph("mydb:main")
    ///     .query()
    ///     .sparql("SELECT ?s WHERE { ?s ?p ?o }")
    ///     .execute()
    ///     .await?;
    ///
    /// // Lazy transact + commit
    /// let out = fluree
    ///     .graph("mydb:main")
    ///     .transact()
    ///     .insert(&data)
    ///     .commit()
    ///     .await?;
    ///
    /// // Materialize for reuse
    /// let db = fluree.graph("mydb:main").load().await?;
    /// ```
    pub fn graph(&self, ledger_id: &str) -> Graph<'_> {
        Graph::new(self, ledger_id.to_string(), TimeSpec::Latest)
    }

    /// Create a lazy graph handle at a specific time.
    ///
    /// Supports `TimeSpec::AtT`, `TimeSpec::AtTime` (ISO-8601),
    /// `TimeSpec::AtCommit` (SHA prefix), and `TimeSpec::Latest`.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let result = fluree
    ///     .graph_at("mydb:main", TimeSpec::AtT(42))
    ///     .query()
    ///     .jsonld(&q)
    ///     .execute()
    ///     .await?;
    /// ```
    pub fn graph_at(&self, ledger_id: &str, spec: TimeSpec) -> Graph<'_> {
        Graph::new(self, ledger_id.to_string(), spec)
    }
}

impl Fluree {
    /// Create a transaction builder using a cached [`LedgerHandle`].
    ///
    /// This is the recommended way to transact in server/application contexts.
    /// The handle is borrowed and its internal state is updated in-place
    /// on successful commit, ensuring concurrent readers see the update.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let handle = fluree.ledger_cached("mydb:main").await?;
    /// let result = fluree.stage(&handle)
    ///     .insert(&data)
    ///     .execute().await?;
    /// ```
    pub fn stage<'a>(&'a self, handle: &'a LedgerHandle) -> RefTransactBuilder<'a> {
        RefTransactBuilder::new(self, handle)
    }

    /// Create a transaction builder that consumes a `LedgerState`.
    ///
    /// Use this for CLI tools, scripts, or tests where you manage your own
    /// ledger state. The ledger state is consumed and returned in the result
    /// as an updated `LedgerState` after commit.
    ///
    /// For server/application contexts, prefer [`stage()`](Self::stage) which
    /// uses cached handles with proper concurrency support.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let result = fluree.stage_owned(ledger)
    ///     .insert(&data)
    ///     .execute().await?;
    /// let ledger = result.ledger;
    /// ```
    pub fn stage_owned(&self, ledger: LedgerState) -> OwnedTransactBuilder<'_> {
        OwnedTransactBuilder::new(self, ledger)
    }

    /// Create a FROM-driven query builder.
    ///
    /// Use this when the query body itself specifies which ledgers to target
    /// (via `"from"` in JSON-LD or `FROM`/`FROM NAMED` in SPARQL).
    ///
    /// # Example
    ///
    /// ```ignore
    /// let result = fluree.query_from()
    ///     .jsonld(&query_with_from)
    ///     .policy(ctx)
    ///     .execute().await?;
    /// ```
    /// Create a FROM-driven query builder.
    ///
    /// When the `iceberg` feature is compiled, R2RML/Iceberg graph source
    /// support is automatically enabled — graph sources referenced via
    /// `FROM` or `GRAPH` patterns resolve transparently.
    pub fn query_from(&self) -> FromQueryBuilder<'_> {
        let builder = FromQueryBuilder::new(self);
        #[cfg(feature = "iceberg")]
        let builder = builder.with_r2rml();
        builder
    }

    /// Create a ledger info builder for retrieving comprehensive ledger metadata.
    ///
    /// Returns metadata including commit info, nameservice record, namespace
    /// codes, stats with decoded IRIs, and index information.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let info = fluree.ledger_info("mydb:main")
    ///     .with_context(&context)
    ///     .execute()
    ///     .await?;
    /// ```
    pub fn ledger_info(&self, ledger_id: &str) -> ledger_info::LedgerInfoBuilder<'_> {
        ledger_info::LedgerInfoBuilder::new(self, ledger_id.to_string())
    }

    /// Check if a ledger exists by address.
    ///
    /// Returns `true` if the ledger is registered in the nameservice,
    /// `false` otherwise. This is a lightweight check that only queries
    /// the nameservice without loading the ledger data.
    ///
    /// # Arguments
    /// * `ledger_id` - Ledger ID (e.g., "my/ledger") or full address
    ///
    /// # Example
    ///
    /// ```ignore
    /// if fluree.ledger_exists("my/ledger").await? {
    ///     let ledger = fluree.ledger("my/ledger").await?;
    /// } else {
    ///     let ledger = fluree.create_ledger(&config).await?;
    /// }
    /// ```
    pub async fn ledger_exists(&self, ledger_id: &str) -> Result<bool> {
        Ok(self.nameservice().lookup(ledger_id).await?.is_some())
    }

    /// Get a cached ledger handle (loads if not cached).
    ///
    /// Ledger caching is enabled by default. If disabled via
    /// `FlureeBuilder::without_ledger_caching()`, returns an ephemeral
    /// handle that wraps a fresh load.
    pub async fn ledger_cached(&self, ledger_id: &str) -> Result<LedgerHandle> {
        match &self.ledger_manager {
            Some(mgr) => mgr.get_or_load(ledger_id).await,
            None => {
                // Caching disabled: load fresh, wrap in ephemeral handle.
                // Note: This handle is NOT cached; each call loads fresh.
                // Extract the concrete BinaryIndexStore from the state's TypeErasedStore
                // so the handle's binary_store stays coherent with db.range_provider.
                let state = self.ledger(ledger_id).await?;
                let binary_store = state.binary_store.as_ref().and_then(|te| {
                    te.0.clone()
                        .downcast::<fluree_db_binary_index::BinaryIndexStore>()
                        .ok()
                });
                Ok(LedgerHandle::new(
                    ledger_id.to_string(),
                    state,
                    binary_store,
                ))
            }
        }
    }

    /// Disconnect a ledger from the connection cache
    ///
    /// Releases the cached ledger state, forcing a fresh load on the next access.
    /// Release a cached ledger handle.
    ///
    /// Use this when:
    /// - You want to force a fresh load of a ledger (e.g., after external changes)
    /// - You want to free memory for a ledger you no longer need
    /// - You're shutting down and want to release resources cleanly
    ///
    /// If caching is disabled, this is a no-op.
    /// If the ledger is currently being loaded or reloaded, waiters will receive
    /// cancellation errors.
    ///
    /// # Arguments
    ///
    /// * `ledger_id` - The ledger ID to disconnect
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Force fresh load on next access
    /// fluree.disconnect_ledger("my/ledger").await;
    ///
    /// // Next query will load fresh state
    /// let handle = fluree.ledger_cached("my/ledger").await?;
    /// ```
    pub async fn disconnect_ledger(&self, ledger_id: &str) {
        if let Some(mgr) = &self.ledger_manager {
            mgr.disconnect(ledger_id).await;
        }
        // If caching is disabled, this is a no-op
    }

    /// Disconnect the Fluree system, performing best-effort cleanup
    ///
    /// Callers should stop issuing new queries and transactions before calling
    /// this method. New operations issued concurrently with `disconnect` may
    /// receive cancellation errors, and `wait_all_idle` could block if new
    /// indexing work keeps arriving.
    ///
    /// This drains and releases cached state:
    /// 1. Cancels any pending background indexing and waits for in-progress work to idle
    /// 2. Evicts all cached ledgers from the ledger manager (with a shutdown flag
    ///    that prevents in-flight loaders from re-inserting)
    /// 3. Clears the R2RML mapping cache
    ///
    /// For full termination, the caller should also:
    /// - Abort the maintenance task JoinHandle (if spawned via `spawn_maintenance`)
    /// - Drop the Fluree instance (stops the indexer worker)
    pub async fn disconnect(&self) {
        // 1. Cancel background indexing and wait for idle
        if let tx::IndexingMode::Background(handle) = &self.indexing_mode {
            handle.cancel_all().await;
            handle.wait_all_idle().await;
        }

        // 2. Evict all cached ledgers
        if let Some(mgr) = &self.ledger_manager {
            mgr.disconnect_all().await;
        }

        // 3. Clear R2RML cache
        self.r2rml_cache.clear().await;
    }

    /// Refresh a cached ledger by polling the nameservice
    ///
    /// Refresh a cached ledger if stale.
    ///
    /// # Behavior
    ///
    /// - Looks up the latest nameservice record for the ledger
    /// - If the ledger is cached, compares local state vs remote and updates if stale
    /// - Does NOT cold-load: if the ledger isn't cached, returns `NotLoaded`
    ///
    /// # Returns
    ///
    /// - `Ok(None)` - Nameservice lookup returned no record (ledger doesn't exist)
    /// - `Ok(Some(NotifyResult::NotLoaded))` - Record exists but ledger not cached
    /// - `Ok(Some(NotifyResult::Current))` - Ledger is already up to date
    /// - `Ok(Some(NotifyResult::IndexUpdated))` - Index was refreshed incrementally
    /// - `Ok(Some(NotifyResult::CommitsApplied { count }))` - Commits applied incrementally
    /// - `Ok(Some(NotifyResult::Reloaded))` - Full reload was performed
    ///
    /// # Use Cases
    ///
    /// - **Serverless/Lambda**: Poll for updates on warm invocations before querying
    /// - **Long-running processes**: Periodic freshness check without SSE subscriptions
    /// - **External updates**: Check if another process has committed new data
    ///
    /// # `min_t` enforcement
    ///
    /// Pass [`RefreshOpts`] with `min_t` to assert the ledger has reached at
    /// least that transaction time.  If, after pulling and applying the latest
    /// nameservice state, `t` is still below `min_t`, the call returns
    /// [`ApiError::AwaitTNotReached`].  The caller owns retry / back-off /
    /// timeout policy.
    ///
    /// # Important
    ///
    /// `refresh` is only meaningful when ledger caching is enabled (the server always
    /// enables caching). If caching is disabled, returns `Some(NotLoaded)`.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Cold start: load the ledger
    /// let _ledger = fluree.ledger_cached("mydb:main").await?;
    ///
    /// // Later (warm invocation): poll for updates
    /// let result = fluree.refresh("mydb:main", Default::default()).await?;
    /// match result {
    ///     Some(r) => println!("refreshed to t={}, action={:?}", r.t, r.action),
    ///     None => println!("Ledger not found in nameservice"),
    /// }
    ///
    /// // With min_t enforcement (e.g. after a transaction returned t=42):
    /// let opts = RefreshOpts { min_t: Some(42) };
    /// match fluree.refresh("mydb:main", opts).await {
    ///     Ok(Some(r)) => println!("ready at t={}", r.t),
    ///     Err(ApiError::AwaitTNotReached { current, requested }) => {
    ///         println!("not yet: t={current}, need t={requested} — retry later");
    ///     }
    ///     _ => {}
    /// }
    /// ```
    pub async fn refresh(
        &self,
        ledger_id: &str,
        opts: RefreshOpts,
    ) -> Result<Option<RefreshResult>> {
        // Step A: Check if caching is enabled
        let mgr = match &self.ledger_manager {
            Some(mgr) => mgr,
            None => {
                // Caching disabled - refresh is a no-op
                return Ok(Some(RefreshResult {
                    t: 0,
                    action: NotifyResult::NotLoaded,
                }));
            }
        };

        // Fast path: if min_t is set, check current cached t before hitting NS
        if let Some(min_t) = opts.min_t {
            if let Some(current_t) = mgr.current_t(ledger_id).await {
                if current_t >= min_t {
                    return Ok(Some(RefreshResult {
                        t: current_t,
                        action: NotifyResult::Current,
                    }));
                }
            }
        }

        // Step B: Lookup nameservice record
        // The nameservice handles address resolution (mydb -> mydb:main, etc.)
        let ns_record = match self.nameservice().lookup(ledger_id).await? {
            Some(record) => record,
            None => return Ok(None), // Ledger doesn't exist in nameservice
        };
        // Step C: Use NsRecord.ledger_id as the cache key
        // The ledger_id field contains the canonical form (e.g., "testdb:main")
        // Note: NsRecord.name field only contains the name without branch, despite docs
        let canonical_alias = ns_record.ledger_id.clone();

        // Step D: Delegate to notify with the fresh record
        let action = mgr
            .notify(NsNotify {
                ledger_id: canonical_alias.clone(),
                record: Some(ns_record),
            })
            .await?;

        // Step E: Read resulting t from the cached state
        let t = mgr.current_t(&canonical_alias).await.unwrap_or(0);
        // Step F: Enforce min_t if requested
        if let Some(min_t) = opts.min_t {
            if t < min_t {
                return Err(ApiError::AwaitTNotReached {
                    requested: min_t,
                    current: t,
                });
            }
        }

        Ok(Some(RefreshResult { t, action }))
    }

    /// Spawn the ledger manager maintenance task (idle eviction)
    ///
    /// Returns JoinHandle for graceful shutdown. Call `.abort()` on shutdown.
    /// Should be called once after building Fluree.
    /// Returns None if caching is not enabled.
    pub fn spawn_maintenance(&self) -> Option<tokio::task::JoinHandle<()>> {
        self.ledger_manager
            .as_ref()
            .map(ledger_manager::LedgerManager::spawn_maintenance)
    }
}

// ============================================================================
// Default context management
// ============================================================================

/// Result of a set_default_context operation.
#[derive(Debug)]
pub enum SetContextResult {
    /// Context was successfully updated.
    Updated,
    /// CAS conflict — another writer updated the config concurrently.
    /// The caller may retry.
    Conflict,
}

/// Maximum retries for CAS conflict during context update.
const CONTEXT_CAS_MAX_RETRIES: usize = 3;

impl Fluree {
    /// Create an export builder for streaming RDF data from a ledger.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use fluree_db_api::export::ExportFormat;
    ///
    /// let stats = fluree.export("mydb")
    ///     .format(ExportFormat::Turtle)
    ///     .write_to(&mut writer)
    ///     .await?;
    /// ```
    pub fn export(&self, ledger_id: &str) -> export_builder::ExportBuilder<'_> {
        export_builder::ExportBuilder::new(self, ledger_id.to_string())
    }

    /// Stream a self-contained ledger archive (`.flpack`) for `ledger_id`.
    ///
    /// This is the export side of the `fluree create --from <file>.flpack`
    /// pipeline. Frame bytes (header → commits → optional indexes →
    /// nameservice manifest → end) are written to `writer` in order, so the
    /// caller can target a file, stdout, or any `AsyncWrite` sink without
    /// buffering the full archive in memory.
    ///
    /// `include_indexes` controls whether binary index artifacts ride along
    /// (`true` → instantly queryable on import; `false` → smaller archive,
    /// import will need to reindex). When the ledger has no index root, the
    /// flag is silently downgraded to commits-only.
    pub async fn archive_ledger<W: tokio::io::AsyncWrite + Unpin + Send>(
        &self,
        ledger_id: &str,
        include_indexes: bool,
        writer: &mut W,
    ) -> Result<pack::PackStreamResult> {
        use tokio::io::AsyncWriteExt as _;

        let record = self
            .nameservice()
            .lookup(ledger_id)
            .await?
            .ok_or_else(|| ApiError::NotFound(ledger_id.to_string()))?;

        let canonical_id = record.ledger_id.clone();
        let handle = self.ledger_cached(&canonical_id).await?;

        // Source the manifest *and* the pack request from the same view, so
        // the archive bytes and the manifest pointers always agree. Reading
        // the heads from the nameservice record while reading the pack
        // contents from the cached handle could disagree if the cache is
        // behind a freshly committed head.
        let view = handle.snapshot().await;

        let head_commit_id = view.head_commit_id.clone().ok_or_else(|| {
            ApiError::internal(format!("ledger {canonical_id} has no head commit to pack"))
        })?;

        // `full_ledger_pack_request` silently drops the index when the
        // ledger has none. Mirror that decision here so we never advertise
        // an `index_head_id` we did not archive.
        let archived_index = if include_indexes {
            view.head_index_id.clone()
        } else {
            None
        };
        let request = match archived_index.clone() {
            Some(index_root) => pack::PackRequest::with_indexes(
                vec![head_commit_id.clone()],
                vec![],
                index_root,
                None,
            ),
            None => pack::PackRequest::commits(vec![head_commit_id.clone()], vec![]),
        };

        let mut manifest = serde_json::json!({
            "phase": "nameservice",
            "ledger_id": canonical_id,
            "name": record.name,
            "branch": record.branch,
            "commit_head_id": head_commit_id.to_string(),
            "commit_t": view.t,
        });
        if let Some(cid) = archived_index.as_ref() {
            manifest["index_head_id"] = serde_json::Value::String(cid.to_string());
            manifest["index_t"] = serde_json::Value::from(view.index_t());
        }

        let (tx, mut rx) = tokio::sync::mpsc::channel::<pack::PackChunk>(64);

        // Run producer and consumer concurrently in the same task: the
        // producer is borrowing `&self`, so we cannot `tokio::spawn` it
        // without an owning handle. The bounded channel still gives us
        // backpressure as long as the consumer keeps draining.
        let producer = pack::stream_archive(self, &handle, &request, manifest, tx);
        let consumer = async {
            while let Some(chunk) = rx.recv().await {
                let bytes = chunk.map_err(|e| ApiError::internal(format!("pack stream: {e}")))?;
                writer
                    .write_all(&bytes)
                    .await
                    .map_err(|e| ApiError::internal(format!("archive write: {e}")))?;
            }
            writer
                .flush()
                .await
                .map_err(|e| ApiError::internal(format!("archive flush: {e}")))?;
            Ok::<_, ApiError>(())
        };

        let (producer_result, consumer_result) = tokio::join!(producer, consumer);
        // Surface a producer-side failure even if the consumer drained
        // cleanly. Without this, a corrupt or empty archive would land on
        // disk and `archive_ledger` would still report success.
        let stats = producer_result
            .map_err(|e| ApiError::internal(format!("archive generation failed: {e}")))?;
        consumer_result?;
        Ok(stats)
    }

    /// Walk the commit chain for a ledger and return per-commit summaries.
    ///
    /// `limit` caps the number of returned summaries (newest-first by `t`).
    /// The returned `total` reflects the full chain length regardless of cap;
    /// truncation is implied by `summaries.len() < total`.
    ///
    /// Uses a branch-aware content store so the walk crosses fork points —
    /// pre-fork commits live under the source branch's namespace, not the
    /// current branch's.
    pub async fn commit_log(
        &self,
        ledger_id: &str,
        limit: Option<usize>,
    ) -> Result<(Vec<CommitSummary>, usize)> {
        let record = self
            .nameservice()
            .lookup(ledger_id)
            .await?
            .ok_or_else(|| ApiError::NotFound(ledger_id.to_string()))?;

        let head = match record.commit_head_id.as_ref() {
            Some(id) => id.clone(),
            None => return Ok((Vec::new(), 0)),
        };

        let store = fluree_db_nameservice::branched_content_store_for_record(
            self.backend(),
            self.nameservice(),
            &record,
        )
        .await?;

        let (summaries, total) =
            fluree_db_core::walk_commit_summaries(&store, &head, 0, limit).await?;
        Ok((summaries, total))
    }

    /// Get the default JSON-LD context for a ledger.
    ///
    /// Reads the context CID from nameservice config and fetches the blob
    /// from CAS. Returns `None` if no default context has been set.
    pub async fn get_default_context(&self, ledger_id: &str) -> Result<Option<serde_json::Value>> {
        // Resolve to canonical ledger ID (e.g., "mydb" -> "mydb:main")
        let record = self
            .nameservice()
            .lookup(ledger_id)
            .await?
            .ok_or_else(|| ApiError::NotFound(ledger_id.to_string()))?;
        let canonical_id = &record.ledger_id;

        // Read config to get context CID
        use fluree_db_nameservice::ConfigLookup as _;
        let config = self.nameservice_mode.get_config(canonical_id).await?;
        let ctx_cid = config
            .as_ref()
            .and_then(|c| c.payload.as_ref())
            .and_then(|p| p.default_context.as_ref());

        let cid = match ctx_cid {
            Some(cid) => cid,
            None => return Ok(None),
        };

        // Branch-aware store: branches inherit the parent's default
        // context CID until they publish their own, and that blob lives
        // under the source branch's namespace.
        let cs = fluree_db_nameservice::branched_content_store_for_record(
            self.backend(),
            self.nameservice(),
            &record,
        )
        .await?;
        let bytes = cs.get(cid).await.map_err(|e| {
            ApiError::internal(format!("failed to read default context from CAS: {e}"))
        })?;

        let ctx: serde_json::Value = serde_json::from_slice(&bytes).map_err(|e| {
            ApiError::internal(format!("failed to parse default context JSON: {e}"))
        })?;

        Ok(Some(ctx))
    }

    /// Set (replace) the default JSON-LD context for a ledger.
    ///
    /// Writes the new context blob to CAS, then updates the nameservice config
    /// using compare-and-set semantics. Retries internally on CAS conflict
    /// (up to 3 attempts). After success, invalidates the ledger cache so
    /// subsequent queries pick up the new context.
    pub async fn set_default_context(
        &self,
        ledger_id: &str,
        context: &serde_json::Value,
    ) -> Result<SetContextResult> {
        // Validate: context must be a JSON object (prefix → IRI map)
        if !context.is_object() {
            return Err(ApiError::Config(
                "context must be a JSON object mapping prefixes to IRIs".to_string(),
            ));
        }

        // Resolve to canonical ledger ID (e.g., "mydb" -> "mydb:main")
        let record = self
            .nameservice()
            .lookup(ledger_id)
            .await?
            .ok_or_else(|| ApiError::NotFound(ledger_id.to_string()))?;
        let canonical_id = &record.ledger_id;

        // Serialize and write context blob to CAS
        let context_bytes = serde_json::to_vec(context)
            .map_err(|e| ApiError::internal(format!("failed to serialize context: {e}")))?;

        let cs = self.content_store(canonical_id);
        let new_cid = cs
            .put(ContentKind::LedgerConfig, &context_bytes)
            .await
            .map_err(|e| ApiError::internal(format!("failed to write context to CAS: {e}")))?;

        // CAS loop: read current config, push new config
        for attempt in 0..CONTEXT_CAS_MAX_RETRIES {
            use fluree_db_nameservice::ConfigLookup as _;
            let current_config = self.nameservice_mode.get_config(canonical_id).await?;

            let old_cid = current_config
                .as_ref()
                .and_then(|c| c.payload.as_ref())
                .and_then(|p| p.default_context.clone());

            // Preserve existing config_id and extra fields
            let mut new_payload = current_config
                .as_ref()
                .and_then(|c| c.payload.clone())
                .unwrap_or_default();
            new_payload.default_context = Some(new_cid.clone());

            let new_v = current_config.as_ref().map_or(1, |c| c.v + 1);
            let new_config = ConfigValue::new(new_v, Some(new_payload));

            match self
                .nameservice_mode
                .push_config(canonical_id, current_config.as_ref(), &new_config)
                .await?
            {
                ConfigCasResult::Updated => {
                    tracing::info!(
                        cid = %new_cid,
                        ledger = canonical_id,
                        "default context updated"
                    );

                    // GC old blob if CID changed.
                    if let Some(old) = old_cid {
                        if old != new_cid {
                            let cs = self.content_store(canonical_id);
                            if let Err(e) = cs.release(&old).await {
                                tracing::debug!(
                                    %e,
                                    old_cid = %old,
                                    "could not release old default context blob"
                                );
                            }
                        }
                    }

                    // Invalidate cached ledger so next query reloads with new context
                    self.disconnect_ledger(canonical_id).await;

                    return Ok(SetContextResult::Updated);
                }
                ConfigCasResult::Conflict { .. } => {
                    tracing::debug!(
                        attempt,
                        ledger = canonical_id,
                        "CAS conflict updating default context, retrying"
                    );
                    continue;
                }
            }
        }

        // All retries exhausted — best-effort GC the orphan blob we wrote.
        let cs = self.content_store(canonical_id);
        if let Err(e) = cs.release(&new_cid).await {
            tracing::debug!(
                %e,
                orphan_cid = %new_cid,
                "could not release orphan context blob after conflict"
            );
        }

        Ok(SetContextResult::Conflict)
    }
}

/// Convenience functions for common configurations
///
/// Create a file-backed Fluree instance
///
/// This is the most common configuration for production use.
#[cfg(feature = "native")]
pub fn fluree_file(path: impl Into<String>) -> Result<Fluree> {
    FlureeBuilder::file(path).build()
}

/// Create a memory-backed Fluree instance
///
/// Useful for testing or when persistence is not needed.
pub fn fluree_memory() -> Fluree {
    FlureeBuilder::memory().build_memory()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fluree_builder_memory() {
        let fluree = FlureeBuilder::memory().cache_max_mb(500).build_memory();

        assert_eq!(fluree.config.cache.max_mb, 500);
    }

    #[test]
    #[cfg(feature = "native")]
    fn test_fluree_builder_file() {
        // `without_indexing()` keeps this a plain `#[test]` — the default
        // background indexer would require a tokio runtime.
        let result = FlureeBuilder::file("/tmp/test")
            .without_indexing()
            .parallelism(8)
            .cache_max_mb(1000)
            .build();

        assert!(result.is_ok());
        let fluree = result.unwrap();
        assert_eq!(fluree.config.parallelism, 8);
    }

    #[test]
    #[cfg(feature = "native")]
    fn test_fluree_builder_no_path_error() {
        let result = FlureeBuilder::new().build();
        assert!(result.is_err());
    }

    #[test]
    fn test_fluree_memory_convenience() {
        let _fluree = fluree_memory();
    }

    // ========================================================================
    // IndexConfig propagation tests (commit e6d0044)
    // ========================================================================

    #[test]
    fn test_default_index_config_returns_defaults_without_thresholds() {
        let fluree = FlureeBuilder::memory().build_memory();
        let cfg = fluree.default_index_config();
        let expected = server_defaults::default_index_config();
        assert_eq!(cfg.reindex_min_bytes, expected.reindex_min_bytes);
        assert_eq!(cfg.reindex_max_bytes, expected.reindex_max_bytes);
    }

    #[test]
    fn test_with_indexing_thresholds_propagates_to_default_index_config() {
        // This is the exact scenario that was broken before e6d0044:
        // custom thresholds set via the builder were silently dropped.
        let fluree = FlureeBuilder::memory()
            .with_indexing_thresholds(500_000, 5_000_000)
            .build_memory();

        let cfg = fluree.default_index_config();
        assert_eq!(cfg.reindex_min_bytes, 500_000);
        assert_eq!(cfg.reindex_max_bytes, 5_000_000);
    }

    #[test]
    fn test_derive_index_config_extracts_from_connection_config() {
        use fluree_db_connection::config::{DefaultsConfig, IndexingDefaults};

        let config = ConnectionConfig {
            defaults: Some(DefaultsConfig {
                identity: None,
                indexing: Some(IndexingDefaults {
                    reindex_min_bytes: Some(250_000),
                    reindex_max_bytes: Some(2_500_000),
                    max_old_indexes: None,
                    indexing_enabled: None,
                    track_class_stats: None,
                    gc_min_time_mins: None,
                }),
            }),
            ..Default::default()
        };

        let idx = derive_index_config(&config);
        assert_eq!(idx.reindex_min_bytes, 250_000);
        assert_eq!(idx.reindex_max_bytes, 2_500_000);
    }

    #[test]
    fn test_derive_index_config_falls_back_to_defaults() {
        let config = ConnectionConfig::default();
        let idx = derive_index_config(&config);
        let expected = server_defaults::default_index_config();
        assert_eq!(idx.reindex_min_bytes, expected.reindex_min_bytes);
        assert_eq!(idx.reindex_max_bytes, expected.reindex_max_bytes);
    }

    // ========================================================================
    // Refresh API tests
    // ========================================================================

    #[tokio::test]
    async fn test_refresh_returns_none_when_no_ns_record() {
        // Build with caching enabled
        let fluree = FlureeBuilder::memory().build_memory();

        // Refresh unknown ledger should return None (not in nameservice)
        let result = fluree
            .refresh("nonexistent:main", RefreshOpts::default())
            .await
            .unwrap();
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn test_refresh_noop_when_not_cached() {
        use fluree_db_core::{ContentId, ContentKind};

        let fluree = FlureeBuilder::memory().build_memory();
        let cid = ContentId::new(ContentKind::Commit, b"commit-1");

        // Publish a record to nameservice directly (without caching the ledger)
        fluree
            .publisher()
            .unwrap()
            .publish_commit("mydb:main", 5, &cid)
            .await
            .unwrap();

        // Refresh should return NotLoaded (record exists but not cached)
        let result = fluree
            .refresh("mydb:main", RefreshOpts::default())
            .await
            .unwrap();
        assert_eq!(result.map(|r| r.action), Some(NotifyResult::NotLoaded));
    }

    #[tokio::test]
    async fn test_refresh_noop_when_caching_disabled() {
        // Build WITHOUT caching
        let fluree = FlureeBuilder::memory()
            .without_ledger_caching()
            .build_memory();

        // Refresh should return NotLoaded (caching disabled = no-op)
        let result = fluree
            .refresh("mydb:main", RefreshOpts::default())
            .await
            .unwrap();
        assert_eq!(result.map(|r| r.action), Some(NotifyResult::NotLoaded));
    }

    #[tokio::test]
    async fn test_refresh_with_alias_resolution() {
        use fluree_db_core::{ContentId, ContentKind};

        let fluree = FlureeBuilder::memory().build_memory();
        let cid = ContentId::new(ContentKind::Commit, b"commit-1");

        // Publish with canonical alias
        fluree
            .publisher()
            .unwrap()
            .publish_commit("mydb:main", 5, &cid)
            .await
            .unwrap();

        // Refresh with short alias should resolve to canonical
        let result = fluree
            .refresh("mydb", RefreshOpts::default())
            .await
            .unwrap();
        // Should return NotLoaded since we haven't cached it
        assert_eq!(result.map(|r| r.action), Some(NotifyResult::NotLoaded));

        // Refresh with full alias should also work
        let result = fluree
            .refresh("mydb:main", RefreshOpts::default())
            .await
            .unwrap();
        assert_eq!(result.map(|r| r.action), Some(NotifyResult::NotLoaded));
    }

    #[tokio::test]
    async fn test_refresh_current_when_up_to_date() {
        let fluree = FlureeBuilder::memory().build_memory();

        // Create a ledger (this publishes to NS and returns state)
        let ledger = fluree.create_ledger("testdb").await.unwrap();
        let initial_t = ledger.t();

        // Cache the ledger by loading it
        let handle = fluree.ledger_cached("testdb:main").await.unwrap();
        let snapshot = handle.snapshot().await;
        assert_eq!(snapshot.t, initial_t);

        // Refresh should return Current (cache matches NS)
        let result = fluree
            .refresh("testdb:main", RefreshOpts::default())
            .await
            .unwrap();
        assert_eq!(result.map(|r| r.action), Some(NotifyResult::Current));

        // Verify cached state is unchanged
        let snapshot_after = handle.snapshot().await;
        assert_eq!(snapshot_after.t, initial_t);
    }

    #[tokio::test]
    async fn test_refresh_after_stage_update() {
        use serde_json::json;

        let fluree = FlureeBuilder::memory().build_memory();

        // Create a ledger (genesis at t=0)
        let _ledger = fluree.create_ledger("txdb").await.unwrap();

        // Cache the ledger (loads from NS at t=0)
        let handle = fluree.ledger_cached("txdb:main").await.unwrap();
        let snapshot_before = handle.snapshot().await;
        assert_eq!(snapshot_before.t, 0);

        // Do a transaction using stage builder (updates cache in place)
        let txn = json!({
            "@context": {"ex": "http://example.org/"},
            "insert": [{"@id": "ex:test", "ex:name": "test"}]
        });
        let _result = fluree.stage(&handle).update(&txn).execute().await.unwrap();

        // The handle should now reflect the new t
        let snapshot_after = handle.snapshot().await;
        assert_eq!(
            snapshot_after.t, 1,
            "Ledger should be at t=1 after transaction"
        );

        // Refresh should return Current (cache is up to date with NS)
        let result = fluree
            .refresh("txdb:main", RefreshOpts::default())
            .await
            .unwrap();
        assert_eq!(result.map(|r| r.action), Some(NotifyResult::Current));
    }

    #[tokio::test]
    async fn test_refresh_with_short_alias_caching() {
        // CRITICAL: Test cache key consistency when using short aliases
        // This verifies that caching via "mydb" and refreshing via "mydb" works
        // (the cache key must be canonical, not the raw input alias)

        let fluree = FlureeBuilder::memory().build_memory();

        // Create ledger
        let _ledger = fluree.create_ledger("shortdb").await.unwrap();

        // Cache via SHORT alias (without :main suffix)
        let handle = fluree.ledger_cached("shortdb").await.unwrap();
        let snapshot = handle.snapshot().await;
        assert_eq!(snapshot.t, 0);

        // Refresh via SHORT alias - should find the cached entry
        // If this returns NotLoaded, there's a cache key mismatch bug
        let result = fluree
            .refresh("shortdb", RefreshOpts::default())
            .await
            .unwrap();
        assert_eq!(
            result.map(|r| r.action),
            Some(NotifyResult::Current),
            "Short alias refresh should find cached entry (not NotLoaded)"
        );

        // Also verify full alias refresh works
        let result_full = fluree
            .refresh("shortdb:main", RefreshOpts::default())
            .await
            .unwrap();
        assert_eq!(
            result_full.map(|r| r.action),
            Some(NotifyResult::Current),
            "Full alias refresh should also find cached entry"
        );
    }

    // ========================================================================
    // AddressIdentifierResolverStorage tests
    // ========================================================================

    #[tokio::test]
    async fn test_resolver_routes_to_identifier_storage() {
        // Create two separate memory storages
        // Note: MemoryStorage uses the address directly as the key
        let default_storage = MemoryStorage::new();
        default_storage
            .write_bytes("fluree:memory://default-key.json", b"default-data")
            .await
            .unwrap();

        let commit_storage = MemoryStorage::new();
        // When routing via identifier, the full address is passed to the storage
        commit_storage
            .write_bytes(
                "fluree:commit-store:memory://mydb/main/commit/abc.fcv2",
                b"commit-data",
            )
            .await
            .unwrap();

        // Build resolver with identifier mapping
        let mut identifier_map = std::collections::HashMap::new();
        identifier_map.insert(
            "commit-store".to_string(),
            Arc::new(commit_storage) as Arc<dyn fluree_db_core::Storage>,
        );

        let resolver = AddressIdentifierResolverStorage::new(
            Arc::new(default_storage) as Arc<dyn fluree_db_core::Storage>,
            identifier_map,
        );

        // Address with identifier should route to mapped storage
        let bytes = resolver
            .read_bytes("fluree:commit-store:memory://mydb/main/commit/abc.fcv2")
            .await
            .unwrap();
        assert_eq!(bytes, b"commit-data");

        // Address without identifier should route to default
        let bytes = resolver
            .read_bytes("fluree:memory://default-key.json")
            .await
            .unwrap();
        assert_eq!(bytes, b"default-data");
    }

    #[tokio::test]
    async fn test_resolver_unknown_identifier_routes_to_default() {
        let default_storage = MemoryStorage::new();
        // Write with the full address that will be used for lookup
        default_storage
            .write_bytes("fluree:unknown-id:memory://fallback.json", b"fallback-data")
            .await
            .unwrap();

        // Empty identifier map - all reads go to default
        let resolver = AddressIdentifierResolverStorage::new(
            Arc::new(default_storage) as Arc<dyn fluree_db_core::Storage>,
            std::collections::HashMap::new(),
        );

        // Unknown identifier falls through to default
        let bytes = resolver
            .read_bytes("fluree:unknown-id:memory://fallback.json")
            .await
            .unwrap();
        assert_eq!(bytes, b"fallback-data");
    }

    #[tokio::test]
    async fn test_resolver_writes_go_to_default() {
        let default_storage = MemoryStorage::new();
        let other_storage = MemoryStorage::new();

        let mut identifier_map = std::collections::HashMap::new();
        identifier_map.insert(
            "other".to_string(),
            Arc::new(other_storage.clone()) as Arc<dyn fluree_db_core::Storage>,
        );

        let resolver = AddressIdentifierResolverStorage::new(
            Arc::new(default_storage.clone()) as Arc<dyn fluree_db_core::Storage>,
            identifier_map,
        );

        // Write with identifier - should go to default (not to "other" storage)
        resolver
            .write_bytes("fluree:other:memory://test.json", b"written-data")
            .await
            .unwrap();

        // Verify it went to default
        let default_exists = default_storage
            .exists("fluree:other:memory://test.json")
            .await
            .unwrap();
        assert!(default_exists, "Write should go to default storage");

        // Verify it did NOT go to other
        let other_exists = other_storage
            .exists("fluree:other:memory://test.json")
            .await
            .unwrap();
        assert!(!other_exists, "Write should NOT go to mapped storage");
    }

    #[tokio::test]
    async fn test_resolver_read_bytes_hint() {
        let default_storage = MemoryStorage::new();
        default_storage
            .write_bytes("fluree:memory://data.json", b"hint-test")
            .await
            .unwrap();

        let resolver = AddressIdentifierResolverStorage::new(
            Arc::new(default_storage) as Arc<dyn fluree_db_core::Storage>,
            std::collections::HashMap::new(),
        );

        // read_bytes_hint should work same as read_bytes
        let bytes = resolver
            .read_bytes_hint(
                "fluree:memory://data.json",
                fluree_db_core::ReadHint::AnyBytes,
            )
            .await
            .unwrap();
        assert_eq!(bytes, b"hint-test");
    }

    #[tokio::test]
    async fn test_resolver_exists() {
        let default_storage = MemoryStorage::new();
        default_storage
            .write_bytes("fluree:memory://exists.json", b"data")
            .await
            .unwrap();

        let mapped_storage = MemoryStorage::new();
        mapped_storage
            .write_bytes("fluree:mapped:memory://mapped.json", b"mapped-data")
            .await
            .unwrap();

        let mut identifier_map = std::collections::HashMap::new();
        identifier_map.insert(
            "mapped".to_string(),
            Arc::new(mapped_storage) as Arc<dyn fluree_db_core::Storage>,
        );

        let resolver = AddressIdentifierResolverStorage::new(
            Arc::new(default_storage) as Arc<dyn fluree_db_core::Storage>,
            identifier_map,
        );

        // Check exists on default storage
        assert!(resolver
            .exists("fluree:memory://exists.json")
            .await
            .unwrap());

        // Check exists on mapped storage
        assert!(resolver
            .exists("fluree:mapped:memory://mapped.json")
            .await
            .unwrap());

        // Check non-existent file
        assert!(!resolver
            .exists("fluree:memory://nonexistent.json")
            .await
            .unwrap());
    }

    #[test]
    #[cfg(feature = "ipfs")]
    fn test_build_ipfs_constructs_fluree() {
        // No real Kubo node needed — this only verifies the type plumbing.
        let fluree = FlureeBuilder::memory().build_ipfs("http://127.0.0.1:5001");
        // Backend should be Permanent (IPFS), not Managed.
        assert!(matches!(
            fluree.backend(),
            fluree_db_core::StorageBackend::Permanent(_)
        ));
        // Admin storage should be None for IPFS (no raw Storage interface).
        assert!(fluree.admin_storage().is_none());
    }
}

#[cfg(all(test, feature = "shacl"))]
mod shacl_tests;
