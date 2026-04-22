//! # Fluree DB Indexer
//!
//! Index building for Fluree DB. This crate provides:
//!
//! - Binary columnar index building (`run_index` module)
//! - Background indexing orchestration
//! - Namespace delta replay
//! - Garbage collection support
//!
//! ## Design
//!
//! The indexer supports two deployment modes:
//!
//! 1. **Embedded**: Background indexing within the main process
//! 2. **External**: Standalone Lambda-style indexer
//!
//! The binary index pipeline produces FIR6/FBR3/FLI3 columnar index artifacts.
//!
//! ## Build Pipeline Modules
//!
//! The actual build pipelines live in [`build`] sub-modules:
//! - [`build::rebuild`]: Full index rebuild from genesis (Phase A..F)
//! - [`build::incremental`]: Incremental index update (Phase 1..5)
//! - [`build::upload`]: CAS upload primitives + index artifact upload
//! - [`build::upload_dicts`]: Dictionary flat-file upload
//! - [`build::spatial`]: Spatial index building
//! - [`build::root_assembly`]: Common root finalization
//! - [`build::commit_chain`]: Commit chain walking

mod build;
pub mod config;
pub mod drop;
pub mod error;
pub mod fulltext_hook;
pub mod gc;
#[path = "stats/hll256.rs"]
pub mod hll;
pub mod orchestrator;
pub mod run_index;
pub mod spatial_hook;
pub mod stats;

// Re-export main types
pub use config::{
    ConfiguredFulltextProperty, ConfiguredFulltextScope, FulltextConfigProvider, IndexerConfig,
};
pub use drop::collect_ledger_cids;
pub use error::{IndexerError, Result};
pub use gc::{
    clean_garbage, write_garbage_record, CleanGarbageConfig, CleanGarbageResult, GarbageRecord,
    DEFAULT_MAX_OLD_INDEXES, DEFAULT_MIN_TIME_GARBAGE_MINS,
};
pub use orchestrator::{
    current_index_request_correlation, with_index_request_correlation, BackgroundIndexerWorker,
    IndexCompletion, IndexOutcome, IndexPhase, IndexRequestCorrelation, IndexStatusSnapshot,
    IndexerHandle, IndexerOrchestrator,
};
#[cfg(feature = "embedded-orchestrator")]
pub use orchestrator::{
    maybe_refresh_after_commit, require_refresh_before_commit, PostCommitIndexResult,
};
pub use stats::{IndexStatsHook, NoOpStatsHook, StatsArtifacts, StatsSummary};

// Re-export build pipeline types
pub use build::types::{UploadedDicts, UploadedIndexes};

// Re-export build pipeline types
pub use run_index::build::build_from_commits::{
    build_indexes_from_commits, build_indexes_from_remapped_commits, BuildConfig, BuildResult,
    CommitInput, BUILD_STAGE_LINK_RUNS, BUILD_STAGE_MERGE, BUILD_STAGE_REMAP,
};

use fluree_db_core::ContentStore;
use fluree_db_nameservice::{NameService, Publisher};
use tracing::Instrument;

/// Result of building an index
#[derive(Debug, Clone)]
pub struct IndexResult {
    /// Content identifier of the index root (derived from SHA-256 of root bytes).
    pub root_id: fluree_db_core::ContentId,
    /// Transaction time the index is current through
    pub index_t: i64,
    /// Ledger ID (name:branch format)
    pub ledger_id: String,
    /// Index build statistics
    pub stats: IndexStats,
}

/// Statistics from index building
#[derive(Debug, Clone, Default)]
pub struct IndexStats {
    /// Total number of flakes in the index (after dedup)
    pub flake_count: usize,
    /// Number of leaf nodes created
    pub leaf_count: usize,
    /// Number of branch nodes created
    pub branch_count: usize,
    /// Total bytes written
    pub total_bytes: usize,
}

/// Current index version for compatibility checking
pub const CURRENT_INDEX_VERSION: i32 = 2;

/// Build a binary index from an existing nameservice record.
///
/// This is the main index-build implementation once the caller already has an
/// up-to-date [`fluree_db_nameservice::NsRecord`]. It preserves the same
/// refresh-first behavior as [`build_index_for_ledger`], but skips the extra
/// nameservice lookup.
pub async fn build_index_for_record(
    content_store: std::sync::Arc<dyn ContentStore>,
    record: &fluree_db_nameservice::NsRecord,
    mut config: IndexerConfig,
) -> Result<IndexResult> {
    let ledger_id = record.ledger_id.as_str();

    // If a config provider is attached, let it refresh the per-run
    // `fulltext_configured_properties` from the live ledger state. This is
    // the hook that keeps background / incremental indexing in sync with
    // `f:fullTextDefaults` changes committed after process start.
    if let Some(provider) = config.fulltext_config_provider.clone() {
        config.fulltext_configured_properties =
            provider.fulltext_configured_properties(ledger_id).await;
    }
    let correlation = crate::orchestrator::current_index_request_correlation();
    let span = tracing::debug_span!(
        "index_build",
        ledger_id = ledger_id,
        request_id = correlation
            .as_ref()
            .and_then(|ctx| ctx.request_id.as_deref()),
        trace_id = correlation.as_ref().and_then(|ctx| ctx.trace_id.as_deref()),
        trigger_operation = correlation
            .as_ref()
            .and_then(|ctx| ctx.operation.as_deref()),
    );
    async move {
        let commit_gap = record.commit_t - record.index_t;

        tracing::info!(
            ledger_id = ledger_id,
            index_t = record.index_t,
            commit_t = record.commit_t,
            commit_gap,
            has_index = record.index_head_id.is_some(),
            incremental_enabled = config.incremental_enabled,
            incremental_max_commits = config.incremental_max_commits,
            "loaded ledger state for index build"
        );

        // If index is already current, return it
        if let Some(ref root_id) = record.index_head_id {
            if record.index_t >= record.commit_t {
                tracing::info!(
                    ledger_id = ledger_id,
                    index_t = record.index_t,
                    commit_t = record.commit_t,
                    "index already current; returning existing root"
                );
                return Ok(IndexResult {
                    root_id: root_id.clone(),
                    index_t: record.index_t,
                    ledger_id: ledger_id.to_string(),
                    stats: IndexStats::default(),
                });
            }
        }

        // Try incremental indexing if conditions are met.
        let can_incremental = config.incremental_enabled
            && record.index_head_id.is_some()
            && record.index_t > 0
            && commit_gap <= config.incremental_max_commits as i64;

        if can_incremental {
            tracing::info!(
                from_t = record.index_t,
                to_t = record.commit_t,
                commit_gap = commit_gap,
                "attempting incremental index"
            );

            match incremental_index(content_store.clone(), ledger_id, record, config.clone()).await
            {
                Ok(result) => {
                    return Ok(result);
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        "incremental indexing failed, falling back to full rebuild"
                    );
                }
            }
        } else if config.incremental_enabled && record.index_head_id.is_some() && record.index_t > 0
        {
            tracing::info!(
                commit_gap = commit_gap,
                max = config.incremental_max_commits,
                "commit gap exceeds incremental limit, using full rebuild"
            );
        }

        tracing::info!(
            ledger_id = ledger_id,
            index_t = record.index_t,
            commit_t = record.commit_t,
            commit_gap,
            "starting full rebuild path"
        );
        rebuild_index_from_commits(content_store, ledger_id, record, config).await
    }
    .instrument(span)
    .await
}

/// External indexer entry point
///
/// Builds a binary columnar index from the commit chain. The pipeline:
/// 1. Walks the commit chain and generates sorted run files
/// 2. Builds per-graph leaf/branch indexes for all sort orders
/// 3. Creates an FIR6 root descriptor and writes it to storage
///
/// Returns early if the index is already current (no work needed).
/// Use `rebuild_index_from_commits` directly to force a rebuild regardless.
pub async fn build_index_for_ledger(
    content_store: std::sync::Arc<dyn ContentStore>,
    nameservice: &dyn NameService,
    ledger_id: &str,
    config: IndexerConfig,
) -> Result<IndexResult> {
    let record = nameservice
        .lookup(ledger_id)
        .await
        .map_err(|e| IndexerError::NameService(e.to_string()))?
        .ok_or_else(|| IndexerError::LedgerNotFound(ledger_id.to_string()))?;

    build_index_for_record(content_store, &record, config).await
}

/// Build a binary index from an existing nameservice record.
///
/// Unlike `build_index_for_ledger`, this skips the nameservice lookup and
/// the "already current" early-return check. Use this when you already have
/// the `NsRecord` and want to force a rebuild (e.g., `reindex`).
///
/// See [`build::rebuild::rebuild_index_from_commits`] for the full pipeline.
pub async fn rebuild_index_from_commits(
    content_store: std::sync::Arc<dyn ContentStore>,
    ledger_id: &str,
    record: &fluree_db_nameservice::NsRecord,
    config: IndexerConfig,
) -> Result<IndexResult> {
    build::rebuild::rebuild_index_from_commits(content_store, ledger_id, record, config).await
}

/// Like [`rebuild_index_from_commits`], but accepts a caller-provided
/// [`ContentStore`] for reading commit blobs. Use this when commit history
/// spans multiple storage namespaces (e.g. rebasing a branch whose commit
/// chain falls through to parent namespaces via `BranchedContentStore`).
pub async fn rebuild_index_from_commits_with_store<C>(
    commit_store: C,
    ledger_id: &str,
    record: &fluree_db_nameservice::NsRecord,
    config: IndexerConfig,
) -> Result<IndexResult>
where
    C: ContentStore + Clone + Send + Sync + 'static,
{
    build::rebuild::rebuild_index_from_commits_with_store(commit_store, ledger_id, record, config)
        .await
}

/// Incremental index from an existing FIR6 root.
///
/// Loads the existing `IndexRoot`, resolves only new commits, merges
/// novelty into affected FLI3 leaves, and publishes a new FIR6 root.
async fn incremental_index(
    content_store: std::sync::Arc<dyn fluree_db_core::ContentStore>,
    ledger_id: &str,
    record: &fluree_db_nameservice::NsRecord,
    config: IndexerConfig,
) -> Result<IndexResult> {
    build::incremental::incremental_index(content_store, ledger_id, record, config).await
}

/// Upload index artifacts (FLI3 leaves, FHS1 sidecars, FBR3 branches) to CAS.
pub async fn upload_indexes_to_cas(
    content_store: &dyn fluree_db_core::ContentStore,
    build_result: &BuildResult,
) -> Result<UploadedIndexes> {
    build::upload::upload_indexes_to_cas(content_store, build_result).await
}

/// Upload dictionary artifacts from persisted flat files to CAS.
pub async fn upload_dicts_from_disk(
    content_store: &dyn fluree_db_core::ContentStore,
    run_dir: &std::path::Path,
    namespace_codes: &std::collections::HashMap<u16, String>,
    trust_sorted_order_invariants: bool,
) -> Result<UploadedDicts> {
    build::upload_dicts::upload_dicts_from_disk(
        content_store,
        run_dir,
        namespace_codes,
        trust_sorted_order_invariants,
    )
    .await
}

/// Publish index result to nameservice
pub async fn publish_index_result(publisher: &dyn Publisher, result: &IndexResult) -> Result<()> {
    publisher
        .publish_index(&result.ledger_id, result.index_t, &result.root_id)
        .await
        .map_err(|e| IndexerError::NameService(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_stats_default() {
        let stats = IndexStats::default();
        assert_eq!(stats.flake_count, 0);
        assert_eq!(stats.leaf_count, 0);
        assert_eq!(stats.branch_count, 0);
        assert_eq!(stats.total_bytes, 0);
    }
}
