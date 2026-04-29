//! Administrative operations for Fluree DB
//!
//! This module provides admin-level operations like `drop_ledger` and
//! `drop_graph_source` that are separate from normal CRUD operations.
//!
//! # Note
//!
//! These operations require `S: Storage`, which provides full read/write/delete
//! capabilities. They work with memory/file/S3 admin backends but are not
//! available on read-only storage.

use crate::{error::ApiError, tx::IndexingMode, Result};
use fluree_db_core::{address_path::ledger_id_to_path_prefix, format_ledger_id, DEFAULT_BRANCH};
use fluree_db_indexer::{clean_garbage, rebuild_index_from_commits, CleanGarbageConfig};
use fluree_db_nameservice::NsRecord;
use std::time::Duration;
use tracing::{debug, info, warn};

// =============================================================================
// Drop Mode and Status Types
// =============================================================================

/// Mode for drop operation
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum DropMode {
    /// Retract from nameservice only (data files remain)
    ///
    /// This is the default and safest option. The ledger is marked as retracted
    /// in the nameservice, but all data files remain on disk for potential
    /// recovery.
    #[default]
    Soft,

    /// Retract + delete all storage artifacts (irreversible)
    ///
    /// **WARNING**: This is irreversible. All commit and index files will be
    /// permanently deleted after the nameservice retraction.
    Hard,
}

/// Result status of drop operation
///
/// NOTE: This reflects the **nameservice state at lookup time**, not deletion success.
/// Deletion success is reported via `artifacts_deleted` and `warnings` fields
/// in `DropReport`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DropStatus {
    /// Record existed and was not retracted at lookup time
    Dropped,
    /// Record was already marked as retracted
    AlreadyRetracted,
    /// No record found for this ledger_id or graph_source_id
    #[default]
    NotFound,
}

// =============================================================================
// Drop Report Types
// =============================================================================

/// Report of what was deleted/retracted for a ledger
#[derive(Debug, Clone, Default)]
pub struct DropReport {
    /// The normalized ledger ID that was dropped
    pub ledger_id: String,
    /// Status based on nameservice state at lookup time
    pub status: DropStatus,
    /// Number of storage artifacts deleted (Hard mode only).
    ///
    /// Includes commits, transactions, index roots, leaves, branches, dicts,
    /// garbage records, config, and context blobs.
    pub artifacts_deleted: usize,
    /// Any non-fatal errors or warnings encountered during the operation
    pub warnings: Vec<String>,
}

/// Report of what was deleted/retracted for a graph source
#[derive(Debug, Clone, Default)]
pub struct GraphSourceDropReport {
    /// Name of the graph source
    pub name: String,
    /// Branch of the graph source
    pub branch: String,
    /// Status based on nameservice state at lookup time
    pub status: DropStatus,
    /// Number of files deleted (Hard mode only)
    pub files_deleted: usize,
    /// Any non-fatal errors or warnings encountered during the operation
    pub warnings: Vec<String>,
}

/// Report of a branch drop operation
#[derive(Debug, Clone, Default)]
pub struct BranchDropReport {
    /// The normalized ledger ID of the dropped branch
    pub ledger_id: String,
    /// Status based on nameservice state at lookup time
    pub status: DropStatus,
    /// Whether the branch was deferred (retracted but storage preserved for children)
    pub deferred: bool,
    /// Number of storage artifacts deleted
    pub artifacts_deleted: usize,
    /// Ledger IDs of ancestor branches that were cascade-dropped
    pub cascaded: Vec<String>,
    /// Any non-fatal errors or warnings encountered
    pub warnings: Vec<String>,
}

// =============================================================================
// Index Maintenance Types
// =============================================================================

/// Options for trigger_index operation
#[derive(Debug, Clone, Default)]
pub struct TriggerIndexOptions {
    /// Optional wait timeout in milliseconds.
    ///
    /// If `None`, `trigger_index()` waits until indexing completes.
    pub timeout_ms: Option<u64>,
}

impl TriggerIndexOptions {
    /// Set the timeout in milliseconds
    pub fn with_timeout(mut self, timeout_ms: u64) -> Self {
        self.timeout_ms = Some(timeout_ms);
        self
    }
}

/// Options for reindex operation
#[derive(Debug, Clone, Default)]
pub struct ReindexOptions {
    /// Indexer configuration (leaf/branch sizes, GC settings)
    /// If not specified, uses IndexerConfig::default()
    pub indexer_config: Option<fluree_db_indexer::IndexerConfig>,
}

impl ReindexOptions {
    /// Set the indexer configuration for controlling output index structure
    ///
    /// Controls leaf/branch node sizes in the resulting index.
    pub fn with_indexer_config(mut self, config: fluree_db_indexer::IndexerConfig) -> Self {
        self.indexer_config = Some(config);
        self
    }
}

/// Result of trigger_index operation
#[derive(Debug, Clone)]
pub struct TriggerIndexResult {
    /// Ledger ID
    pub ledger_id: String,
    /// Transaction time the index was built to
    pub index_t: i64,
    /// Content identifier of the index root (when available)
    pub root_id: Option<fluree_db_core::ContentId>,
}

/// Result of reindex operation
#[derive(Debug, Clone)]
pub struct ReindexResult {
    /// Ledger ID
    pub ledger_id: String,
    /// Transaction time the index was built to
    pub index_t: i64,
    /// Content identifier of the index root
    pub root_id: fluree_db_core::ContentId,
    /// Build statistics
    pub stats: fluree_db_indexer::IndexStats,
}

/// Result of index_status query
#[derive(Debug, Clone)]
pub struct IndexStatusResult {
    /// Ledger ID
    pub ledger_id: String,
    /// Current index transaction time (from nameservice)
    pub index_t: i64,
    /// Current commit transaction time (from nameservice)
    pub commit_t: i64,
    /// Whether background indexing is enabled
    pub indexing_enabled: bool,
    /// Current indexing phase (Idle/Pending/InProgress)
    pub phase: fluree_db_indexer::IndexPhase,
    /// Pending minimum t (if work is queued)
    pub pending_min_t: Option<i64>,
    /// Last error message (if any)
    pub last_error: Option<String>,
}

// =============================================================================
// Helper Functions
// =============================================================================

/// Normalize ledger ID to canonical form with branch
///
/// If the address already contains a colon (indicating a branch), it's returned as-is.
/// Otherwise, `:main` is appended as the default branch.
fn normalize_ledger_id(ledger_id: &str) -> String {
    fluree_db_core::normalize_ledger_id(ledger_id).unwrap_or_else(|_| ledger_id.to_string())
}

// =============================================================================
// Fluree Drop Implementation
// =============================================================================

impl crate::Fluree {
    /// Drop a ledger
    ///
    /// This operation:
    /// 1. Normalizes the ledger ID (ensures branch suffix like `:main`)
    /// 2. Cancels any pending background indexing
    /// 3. Waits for in-progress indexing to complete
    /// 4. In Hard mode: deletes all storage artifacts (commits + indexes)
    /// 5. Retracts from nameservice
    /// 6. Disconnects from ledger cache (if caching enabled)
    ///
    /// # Arguments
    ///
    /// * `ledger_id` - Ledger ID (e.g., "mydb" or "mydb:main")
    /// * `mode` - `Soft` (retract only) or `Hard` (retract + delete files)
    ///
    /// # Safety
    ///
    /// - `Soft` mode is reversible (data remains, only nameservice retracted)
    /// - `Hard` mode is **IRREVERSIBLE** - all data will be permanently deleted
    ///
    /// # Idempotency
    ///
    /// Safe to call multiple times:
    /// - Returns `AlreadyRetracted` if ledger was previously dropped
    /// - Hard mode still attempts deletion even for `NotFound`/`AlreadyRetracted`
    ///   to enable admin cleanup scenarios
    ///
    /// # External Indexers
    ///
    /// This only stops the in-process background worker. External indexers
    /// (Lambda, etc.) **MUST** check `NsRecord.retracted` before indexing
    /// and before publishing to prevent recreating files after drop.
    pub async fn drop_ledger(&self, ledger_id: &str, mode: DropMode) -> Result<DropReport> {
        // 1. Normalize ledger_id (ensure branch suffix)
        let ledger_id = normalize_ledger_id(ledger_id);
        info!(ledger_id = %ledger_id, mode = ?mode, "Dropping ledger");

        let mut report = DropReport {
            ledger_id: ledger_id.clone(),
            ..Default::default()
        };

        // 2. Lookup current state (for status reporting)
        let record = self.nameservice().lookup(&ledger_id).await?;
        let status = match &record {
            None => DropStatus::NotFound,
            Some(r) if r.retracted => DropStatus::AlreadyRetracted,
            Some(_) => DropStatus::Dropped,
        };
        report.status = status;

        // 3. Stop background indexing (THE FLAKE FIX)
        // NOTE: This only stops the in-process worker. External indexers must
        // check NsRecord.retracted and refuse to index/publish if true.
        if let IndexingMode::Background(handle) = &self.indexing_mode {
            info!(ledger_id = %ledger_id, "Cancelling pending indexing");
            handle.cancel(&ledger_id).await;
            handle.wait_for_idle(&ledger_id).await;
            info!(ledger_id = %ledger_id, "Indexing cancelled and idle");
        }

        // 4. Delete artifacts (Hard mode)
        // Run deletion even for NotFound/AlreadyRetracted - enables admin cleanup
        if matches!(mode, DropMode::Hard) {
            let (count, warnings) = self.drop_artifacts(&ledger_id, record.as_ref()).await;
            report.artifacts_deleted = count;
            report.warnings.extend(warnings);
        }

        // 5. Retract or purge from nameservice
        // Soft drop: retract (mark as retracted, alias cannot be reused)
        // Hard drop: purge (remove record entirely, alias can be reused)
        let publisher = self.publisher()?;
        let ns_result = if matches!(mode, DropMode::Hard) {
            publisher.purge(&ledger_id).await
        } else {
            publisher.retract(&ledger_id).await
        };
        if let Err(e) = ns_result {
            // Log but don't fail - retract/purge may fail if truly not found
            warn!(ledger_id = %ledger_id, error = %e, "Nameservice retract warning");
            report.warnings.push(format!("Nameservice retract: {e}"));
        }

        // 6. Disconnect from ledger cache (if caching enabled)
        // This evicts the ledger from the LedgerManager so stale state isn't served.
        // Equivalent to releasing the ledger at the end of drop-ledger.
        if let Some(mgr) = &self.ledger_manager {
            info!(ledger_id = %ledger_id, "Disconnecting ledger from cache");
            mgr.disconnect(&ledger_id).await;
        }

        info!(ledger_id = %ledger_id, status = ?report.status, "Ledger dropped");
        Ok(report)
    }

    /// Drop a branch
    ///
    /// This operation:
    /// 1. Refuses to drop the "main" branch
    /// 2. If the branch has children (`branches > 0`): retracts (soft-delete),
    ///    preserving storage for children, reports as deferred
    /// 3. If the branch is a leaf (`branches == 0`): cancels indexing, deletes
    ///    all storage artifacts, purges from nameservice, and cascades upward
    ///    to any retracted ancestors that now have zero children
    ///
    /// # Errors
    /// - `ApiError::NotFound` if the branch does not exist
    /// - `ApiError::InvalidInput` if attempting to drop "main"
    pub async fn drop_branch(&self, ledger_name: &str, branch: &str) -> Result<BranchDropReport> {
        if branch == "main" {
            return Err(ApiError::Http {
                status: 400,
                message: "Cannot drop the main branch".to_string(),
            });
        }

        let ledger_id = format_ledger_id(ledger_name, branch);
        info!(ledger_id = %ledger_id, "Dropping branch");

        let mut report = BranchDropReport {
            ledger_id: ledger_id.clone(),
            ..Default::default()
        };

        // Look up the record
        let record = self
            .nameservice()
            .lookup(&ledger_id)
            .await?
            .ok_or_else(|| ApiError::NotFound(format!("Branch not found: {ledger_id}")))?;

        if record.retracted {
            report.status = DropStatus::AlreadyRetracted;
            return Ok(report);
        }

        report.status = DropStatus::Dropped;

        if record.branches > 0 {
            // Has children — retract but preserve storage
            self.publisher()?.retract(&ledger_id).await?;
            report.deferred = true;

            // Disconnect from cache
            if let Some(mgr) = &self.ledger_manager {
                mgr.disconnect(&ledger_id).await;
            }

            info!(
                ledger_id = %ledger_id,
                children = record.branches,
                "Branch retracted (deferred — has children)"
            );
            return Ok(report);
        }

        // Leaf branch — full drop
        let parent_new_count = self
            .purge_branch(&ledger_id, Some(&record), &mut report)
            .await?;

        // Cascade upward if parent is retracted with zero children
        if let (Some(0), Some(source)) = (parent_new_count, &record.source_branch) {
            let parent_id = format_ledger_id(ledger_name, source);
            self.try_cascade_drop(ledger_name, &parent_id, &mut report)
                .await;
        }

        info!(
            ledger_id = %ledger_id,
            artifacts_deleted = report.artifacts_deleted,
            cascaded = ?report.cascaded,
            "Branch dropped"
        );
        Ok(report)
    }

    /// Cancel indexing, delete storage artifacts, purge nameservice record,
    /// and disconnect from cache. Returns the parent's new child count.
    async fn purge_branch(
        &self,
        ledger_id: &str,
        record: Option<&NsRecord>,
        report: &mut BranchDropReport,
    ) -> Result<Option<u32>> {
        if let IndexingMode::Background(handle) = &self.indexing_mode {
            handle.cancel(ledger_id).await;
            handle.wait_for_idle(ledger_id).await;
        }

        let (count, warnings) = self.drop_artifacts(ledger_id, record).await;
        report.artifacts_deleted += count;
        report.warnings.extend(warnings);

        let parent_new_count = self.publisher()?.drop_branch(ledger_id).await?;

        if let Some(mgr) = &self.ledger_manager {
            mgr.disconnect(ledger_id).await;
        }

        Ok(parent_new_count)
    }

    /// Recursively drop retracted ancestor branches that have zero children.
    async fn try_cascade_drop(
        &self,
        ledger_name: &str,
        ancestor_id: &str,
        report: &mut BranchDropReport,
    ) {
        let Ok(Some(ancestor)) = self.nameservice().lookup(ancestor_id).await else {
            return;
        };

        if !ancestor.retracted || ancestor.branches > 0 {
            return;
        }

        info!(ledger_id = %ancestor_id, "Cascading drop to retracted ancestor");

        let parent_new_count = match self
            .purge_branch(ancestor_id, Some(&ancestor), report)
            .await
        {
            Ok(c) => c,
            Err(e) => {
                report
                    .warnings
                    .push(format!("Cascade purge of {ancestor_id}: {e}"));
                return;
            }
        };

        report.cascaded.push(ancestor_id.to_string());

        if let (Some(0), Some(source)) = (parent_new_count, &ancestor.source_branch) {
            let next_ancestor = format_ledger_id(ledger_name, source);
            Box::pin(self.try_cascade_drop(ledger_name, &next_ancestor, report)).await;
        }
    }

    /// Delete all storage artifacts for a ledger.
    ///
    /// Uses a two-path strategy:
    /// - **Fast path**: `list_prefix` on the entire ledger root — catches all
    ///   subdirectories (commit/, txn/, index/, config/, etc.). Works for
    ///   file, S3, and memory backends.
    /// - **Slow path**: If `list_prefix` fails (e.g., IPFS), walks the commit
    ///   chain + index tree to collect all CIDs, derives storage addresses, and
    ///   deletes each individually.
    ///
    /// Returns `(count_deleted, warnings)`.
    async fn drop_artifacts(
        &self,
        ledger_id: &str,
        record: Option<&fluree_db_nameservice::NsRecord>,
    ) -> (usize, Vec<String>) {
        let mut warnings = Vec::new();
        let storage = match self.admin_storage() {
            Some(s) => s,
            None => {
                // Permanent backend (IPFS): no list_prefix or delete — use
                // CID-walk + release to unpin artifacts.
                return self
                    .drop_artifacts_by_cid_walk(ledger_id, record, &mut warnings)
                    .await;
            }
        };
        let storage_method = storage.storage_method();

        // Build the ledger root prefix: fluree:{method}://{ledger_path}/
        let prefix = match ledger_id_to_path_prefix(ledger_id) {
            Ok(p) => p,
            Err(e) => {
                warnings.push(format!("Invalid ledger ID '{ledger_id}': {e}"));
                return (0, warnings);
            }
        };
        let ledger_root = format!("fluree:{storage_method}://{prefix}/");

        // Fast path: list everything under the ledger root and batch delete
        match storage.list_prefix(&ledger_root).await {
            Ok(mut files) => {
                files.sort();
                let mut count = 0;
                for file in &files {
                    if let Err(e) = storage.delete(file).await {
                        warn!(file = %file, error = %e, "Failed to delete artifact");
                        warnings.push(format!("Failed to delete {file}: {e}"));
                    } else {
                        count += 1;
                    }
                }
                info!(count = count, "Fast-path artifact deletion complete");
                (count, warnings)
            }
            Err(e) => {
                // Slow path: walk CID chains (any backend without list_prefix)
                info!(
                    error = %e,
                    "list_prefix unavailable, falling back to CID-walking drop"
                );
                self.drop_artifacts_by_cid_walk(ledger_id, record, &mut warnings)
                    .await
            }
        }
    }

    /// Slow-path artifact deletion: walk commit + index chains to collect CIDs,
    /// then release each via `ContentStore::release`.
    ///
    /// For managed backends, `release` deletes by derived address. For permanent
    /// backends (IPFS), it unpins the CID so Kubo's GC can reclaim the block.
    async fn drop_artifacts_by_cid_walk(
        &self,
        ledger_id: &str,
        record: Option<&fluree_db_nameservice::NsRecord>,
        warnings: &mut Vec<String>,
    ) -> (usize, Vec<String>) {
        let content_store = self.content_store(ledger_id);

        let record = match record {
            Some(r) => r,
            None => {
                warnings.push("No NsRecord available for CID-walking drop".to_string());
                return (0, std::mem::take(warnings));
            }
        };

        let cids = match fluree_db_indexer::collect_ledger_cids(
            content_store.as_ref(),
            record.commit_head_id.as_ref(),
            record.index_head_id.as_ref(),
            record.config_id.as_ref(),
            record.default_context.as_ref(),
        )
        .await
        {
            Ok(c) => c,
            Err(e) => {
                warn!(error = %e, "Failed to collect ledger CIDs for drop");
                warnings.push(format!("CID collection failed: {e}"));
                return (0, std::mem::take(warnings));
            }
        };

        info!(cid_count = cids.len(), "Collected CIDs for slow-path drop");

        let mut count = 0;
        for cid in &cids {
            if let Err(e) = content_store.release(cid).await {
                let msg = e.to_string();
                if msg.contains("not found")
                    || msg.contains("No such file")
                    || msg.contains("not pinned")
                {
                    tracing::debug!(cid = %cid, error = %e, "artifact already removed");
                } else {
                    warn!(cid = %cid, error = %e, "unexpected error releasing artifact");
                    warnings.push(format!("Release failed for {cid}: {e}"));
                }
            } else {
                count += 1;
            }
        }

        info!(count = count, "Slow-path artifact deletion complete");
        (count, std::mem::take(warnings))
    }
}

// =============================================================================
// Graph Source Drop Implementation
// =============================================================================

impl crate::Fluree {
    /// Drop a graph source
    ///
    /// This operation:
    /// 1. Looks up the graph source record in the nameservice
    /// 2. In Hard mode: deletes graph source index files (if prefix is defined)
    /// 3. Retracts from nameservice
    ///
    /// # Arguments
    ///
    /// * `name` - Graph source name (e.g., "my-search")
    /// * `branch` - Branch name (defaults to "main" if None)
    /// * `mode` - `Soft` (retract only) or `Hard` (retract + delete files)
    ///
    /// # Note
    ///
    /// Graph source artifact deletion requires a canonical storage prefix defined in the
    /// indexer crate. Until that exists, Hard mode may skip artifact deletion
    /// and report a warning.
    pub async fn drop_graph_source(
        &self,
        name: &str,
        branch: Option<&str>,
        mode: DropMode,
    ) -> Result<GraphSourceDropReport> {
        let branch = branch.unwrap_or(DEFAULT_BRANCH);
        let graph_source_id = format_ledger_id(name, branch);
        info!(name = %name, branch = %branch, mode = ?mode, "Dropping graph source");

        let mut report = GraphSourceDropReport {
            name: name.to_string(),
            branch: branch.to_string(),
            ..Default::default()
        };

        // 1. Lookup graph source record (for status)
        let record = self
            .nameservice()
            .lookup_graph_source(&graph_source_id)
            .await?;
        let status = match &record {
            None => DropStatus::NotFound,
            Some(r) if r.retracted => DropStatus::AlreadyRetracted,
            Some(_) => DropStatus::Dropped,
        };
        report.status = status;

        // 2. Delete graph source artifacts (Hard mode)
        #[cfg(feature = "iceberg")]
        if matches!(mode, DropMode::Hard) {
            if let Some(ref record) = record {
                // Try to delete the CAS-stored mapping blob
                if let Ok(iceberg_config) =
                    fluree_db_iceberg::IcebergGsConfig::from_json(&record.config)
                {
                    if let Some(mapping) = &iceberg_config.mapping {
                        if let Ok(cid) = mapping.source.parse::<fluree_db_core::ContentId>() {
                            // Resolve CID to storage path and delete
                            let path = fluree_db_core::content_path(
                                fluree_db_core::ContentKind::GraphSourceMapping,
                                &graph_source_id,
                                &cid.digest_hex(),
                            );
                            if let Some(storage) = self.admin_storage() {
                                if let Err(e) = storage.delete(&path).await {
                                    report.warnings.push(format!(
                                        "Failed to delete mapping blob {}: {}",
                                        mapping.source, e
                                    ));
                                } else {
                                    report.files_deleted += 1;
                                }
                            }
                        }
                    }
                }
            }
        }

        // 3. Retract from nameservice (always attempt, idempotent)
        if let Err(e) = self.publisher()?.retract_graph_source(name, branch).await {
            warn!(name = %name, branch = %branch, error = %e, "Nameservice graph source retract warning");
            report.warnings.push(format!("Nameservice retract: {e}"));
        }

        info!(name = %name, branch = %branch, status = ?report.status, "Graph source dropped");
        Ok(report)
    }
}

// =============================================================================
// Index Status and Trigger (minimal bounds - not native-only)
// =============================================================================

impl crate::Fluree {
    /// Get current indexing status for a ledger
    ///
    /// Returns status from both nameservice (index_t, commit_t) and
    /// the background indexer (phase, pending work).
    pub async fn index_status(&self, ledger_id: &str) -> Result<IndexStatusResult> {
        use fluree_db_indexer::IndexPhase;

        let ledger_id = normalize_ledger_id(ledger_id);

        // Get nameservice record
        let record = self
            .nameservice()
            .lookup(&ledger_id)
            .await?
            .ok_or_else(|| ApiError::NotFound(format!("Ledger not found: {ledger_id}")))?;

        // Get indexer status if available
        let (indexing_enabled, phase, pending_min_t, last_error) = match &self.indexing_mode {
            IndexingMode::Background(handle) => {
                if let Some(status) = handle.status(&ledger_id).await {
                    (true, status.phase, status.pending_min_t, status.last_error)
                } else {
                    (true, IndexPhase::Idle, None, None)
                }
            }
            IndexingMode::Disabled => (false, IndexPhase::Idle, None, None),
        };

        Ok(IndexStatusResult {
            ledger_id,
            index_t: record.index_t,
            commit_t: record.commit_t,
            indexing_enabled,
            phase,
            pending_min_t,
            last_error,
        })
    }

    /// Trigger background indexing and wait for completion
    ///
    /// Enqueues an index request for the ledger and waits for the index to
    /// reach the current commit_t.
    ///
    /// If `opts.timeout_ms` is set, waiting stops with `IndexTimeout` once the
    /// deadline expires. If `opts.timeout_ms` is `None`, this waits until the
    /// indexing work completes or fails.
    ///
    /// # No-commit ledgers
    /// If the ledger has no commits yet, returns successfully with index_t=0.
    ///
    /// # Concurrent commits
    /// This targets `commit_t` at call time. Commits after the call aren't waited for.
    ///
    /// # Errors
    /// - `IndexingDisabled` if no background indexer configured
    /// - `IndexTimeout` if timeout expires before completion
    /// - `NotFound` if ledger doesn't exist
    pub async fn trigger_index(
        &self,
        ledger_id: &str,
        opts: TriggerIndexOptions,
    ) -> Result<TriggerIndexResult> {
        use fluree_db_indexer::IndexOutcome;

        let ledger_id = normalize_ledger_id(ledger_id);
        info!(ledger_id = %ledger_id, "Triggering index");

        // Check indexing mode
        let handle = match &self.indexing_mode {
            IndexingMode::Background(h) => h,
            IndexingMode::Disabled => return Err(ApiError::IndexingDisabled),
        };

        // Look up current state
        let record = self
            .nameservice()
            .lookup(&ledger_id)
            .await?
            .ok_or_else(|| ApiError::NotFound(format!("Ledger not found: {ledger_id}")))?;

        if record.retracted {
            return Err(ApiError::NotFound(format!(
                "Ledger is retracted: {ledger_id}"
            )));
        }

        // Handle no-commit ledgers (nothing to index)
        if record.commit_head_id.is_none() {
            info!(ledger_id = %ledger_id, "No commits to index");
            return Ok(TriggerIndexResult {
                ledger_id,
                index_t: 0,
                root_id: None,
            });
        }

        // Trigger with min_t = commit_t
        let min_t = record.commit_t;
        let timeout_ms = opts.timeout_ms;
        info!(
            ledger_id = %ledger_id,
            index_t = record.index_t,
            commit_t = record.commit_t,
            timeout_ms = ?timeout_ms,
            "Queueing index request"
        );
        let completion = handle.trigger(ledger_id.clone(), min_t).await;

        if let Some(status) = handle.status(&ledger_id).await {
            info!(
                ledger_id = %ledger_id,
                target_t = min_t,
                phase = ?status.phase,
                pending_min_t = ?status.pending_min_t,
                last_index_t = status.last_index_t,
                waiter_count = status.waiter_count,
                "Index request queued"
            );
        } else {
            info!(
                ledger_id = %ledger_id,
                target_t = min_t,
                "Index request queued"
            );
        }

        // Wait for completion, emitting periodic status so long-running or
        // stuck indexing work shows up clearly in INFO/DEBUG logs. Apply a
        // deadline only when the caller requested one.
        info!(
            ledger_id = %ledger_id,
            target_t = min_t,
            timeout_ms = ?timeout_ms,
            "Waiting for index completion"
        );
        let wait_started = std::time::Instant::now();
        let mut wait_fut = Box::pin(completion.wait());
        let mut info_interval = tokio::time::interval(Duration::from_secs(60));
        info_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let _ = info_interval.tick().await;
        let mut debug_interval = tokio::time::interval(Duration::from_secs(15));
        debug_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let _ = debug_interval.tick().await;

        macro_rules! finish_wait {
            ($outcome:expr) => {
                match $outcome {
                    IndexOutcome::Completed { index_t, root_id } => {
                        info!(
                            ledger_id = %ledger_id,
                            index_t = index_t,
                            elapsed_ms = wait_started.elapsed().as_millis() as u64,
                            "Indexing completed"
                        );
                        return Ok(TriggerIndexResult {
                            ledger_id: ledger_id.clone(),
                            index_t,
                            root_id,
                        });
                    }
                    IndexOutcome::Failed(msg) => {
                        warn!(
                            ledger_id = %ledger_id,
                            elapsed_ms = wait_started.elapsed().as_millis() as u64,
                            error = %msg,
                            "Indexing failed while waiting"
                        );
                        return Err(ApiError::internal(format!("Indexing failed: {}", msg)));
                    }
                    IndexOutcome::Cancelled => {
                        warn!(
                            ledger_id = %ledger_id,
                            elapsed_ms = wait_started.elapsed().as_millis() as u64,
                            "Indexing was cancelled while waiting"
                        );
                        return Err(ApiError::internal("Indexing was cancelled"));
                    }
                }
            };
        }

        macro_rules! log_wait_status {
            ($level:ident, $message:literal) => {{
                let elapsed_ms = wait_started.elapsed().as_millis() as u64;
                if let Some(status) = handle.status(&ledger_id).await {
                    $level!(
                        ledger_id = %ledger_id,
                        elapsed_ms,
                        target_t = min_t,
                        phase = ?status.phase,
                        pending_min_t = ?status.pending_min_t,
                        last_index_t = status.last_index_t,
                        last_error = ?status.last_error,
                        waiter_count = status.waiter_count,
                        $message
                    );
                } else {
                    $level!(
                        ledger_id = %ledger_id,
                        elapsed_ms,
                        target_t = min_t,
                        $message
                    );
                }
            }};
        }

        if let Some(timeout_ms) = timeout_ms {
            let mut timeout_fut = Box::pin(tokio::time::sleep(Duration::from_millis(timeout_ms)));
            loop {
                tokio::select! {
                    outcome = &mut wait_fut => finish_wait!(outcome),
                    () = &mut timeout_fut => {
                        let elapsed_ms = wait_started.elapsed().as_millis() as u64;
                        if let Some(status) = handle.status(&ledger_id).await {
                            warn!(
                                ledger_id = %ledger_id,
                                timeout_ms,
                                elapsed_ms,
                                target_t = min_t,
                                phase = ?status.phase,
                                pending_min_t = ?status.pending_min_t,
                                last_index_t = status.last_index_t,
                                last_error = ?status.last_error,
                                waiter_count = status.waiter_count,
                                "Index trigger timed out"
                            );
                        } else {
                            warn!(
                                ledger_id = %ledger_id,
                                timeout_ms,
                                elapsed_ms,
                                target_t = min_t,
                                "Index trigger timed out"
                            );
                        }
                        return Err(ApiError::IndexTimeout(timeout_ms));
                    }
                    _ = info_interval.tick() => {
                        log_wait_status!(info, "Still waiting for index completion");
                    }
                    _ = debug_interval.tick() => {
                        log_wait_status!(debug, "Waiting for index completion");
                    }
                }
            }
        } else {
            loop {
                tokio::select! {
                    outcome = &mut wait_fut => finish_wait!(outcome),
                    _ = info_interval.tick() => {
                        log_wait_status!(info, "Still waiting for index completion");
                    }
                    _ = debug_interval.tick() => {
                        log_wait_status!(debug, "Waiting for index completion");
                    }
                }
            }
        }
    }
}

// =============================================================================
// Reindex (requires AdminPublisher for allow-equal publish)
// =============================================================================

impl crate::Fluree {
    /// Full offline reindex from commit history
    ///
    /// Rebuilds the binary index by replaying all commits. This operation:
    /// 1. Cancels any background indexing
    /// 2. Builds a fresh binary columnar index from the commit chain
    /// 3. Validates ledger hasn't advanced (conflict detection)
    /// 4. Publishes new index (allows same t via AdminPublisher)
    ///
    /// # Errors
    /// - `NotFound` if ledger doesn't exist or has no commits
    /// - `ReindexConflict` (409) if ledger advanced during rebuild
    pub async fn reindex(&self, ledger_id: &str, opts: ReindexOptions) -> Result<ReindexResult> {
        let ledger_id = normalize_ledger_id(ledger_id);
        info!(ledger_id = %ledger_id, "Starting reindex");

        // 1. Look up current state and capture commit_t for conflict detection
        let record = self
            .nameservice()
            .lookup(&ledger_id)
            .await?
            .ok_or_else(|| ApiError::NotFound(format!("Ledger not found: {ledger_id}")))?;

        if record.retracted {
            return Err(ApiError::NotFound(format!(
                "Ledger is retracted: {ledger_id}"
            )));
        }

        let initial_commit_t = record.commit_t;
        if record.commit_head_id.is_none() {
            return Err(ApiError::NotFound("No commits to reindex".to_string()));
        }

        // 2. Cancel background indexing if active
        if let IndexingMode::Background(handle) = &self.indexing_mode {
            info!(ledger_id = %ledger_id, "Cancelling background indexing for reindex");
            handle.cancel(&ledger_id).await;
            handle.wait_for_idle(&ledger_id).await;
        }

        // 3. Build binary index from commit chain
        let mut indexer_config = opts.indexer_config.clone().unwrap_or_default();
        let gc_max_old_indexes = indexer_config.gc_max_old_indexes;
        let gc_min_time_mins = indexer_config.gc_min_time_mins;

        // Read the current ledger's `f:fullTextDefaults` so the reindex routes
        // configured plain-string values into BM25 arena building. Best-effort:
        // if the existing index can't be loaded (e.g. first-ever reindex of a
        // commits-only ledger) we fall back to empty — only the `@fulltext`
        // datatype path will contribute entries, which matches pre-config
        // behavior.
        match self.ledger(&ledger_id).await {
            Ok(state) => {
                // Use `state.t()` (= max(novelty.t, snapshot.t)) so that on a
                // first-ever reindex (no prior index, all config in novelty)
                // the config query isn't filtered out by
                // `Novelty::for_each_overlay_flake`'s `flake.t <= to_t` guard.
                let to_t = state.t();
                let snapshot = &state.snapshot;
                let overlay: &dyn fluree_db_core::OverlayProvider = &*state.novelty;
                match crate::config_resolver::resolve_ledger_config(snapshot, overlay, to_t).await {
                    Ok(Some(cfg)) => {
                        indexer_config.fulltext_configured_properties =
                            crate::config_resolver::configured_fulltext_properties_for_indexer(
                                &cfg,
                            );
                    }
                    Ok(None) => {
                        // No LedgerConfig — nothing to seed.
                    }
                    Err(e) => {
                        tracing::warn!(
                            error = %e,
                            "reindex: failed to read LedgerConfig; configured fulltext properties will be skipped"
                        );
                    }
                }
            }
            Err(e) => {
                tracing::debug!(
                    error = %e,
                    "reindex: no loadable ledger state for config read; proceeding without fulltext config"
                );
            }
        }

        let index_result = rebuild_index_from_commits(
            self.content_store(&ledger_id),
            &ledger_id,
            &record,
            indexer_config,
        )
        .await?;

        info!(
            ledger_id = %ledger_id,
            index_t = index_result.index_t,
            "Binary index build complete"
        );

        // 4. Conflict detection: check if ledger advanced during rebuild
        let final_record = self
            .nameservice()
            .lookup(&ledger_id)
            .await?
            .ok_or_else(|| {
                ApiError::NotFound(format!("Ledger disappeared during reindex: {ledger_id}"))
            })?;

        if final_record.commit_t != initial_commit_t {
            return Err(ApiError::ReindexConflict {
                expected: initial_commit_t,
                found: final_record.commit_t,
            });
        }

        // 5. Publish new index (allows same t for reindex via AdminPublisher)
        self.publisher()?
            .publish_index_allow_equal(&ledger_id, index_result.index_t, &index_result.root_id)
            .await?;

        info!(
            ledger_id = %ledger_id,
            index_t = index_result.index_t,
            root_id = %index_result.root_id,
            "Reindex completed"
        );

        // 6. Spawn async garbage collection (non-blocking)
        let gc_store = self.content_store(&ledger_id);
        let gc_root_id = index_result.root_id.clone();
        let gc_config = CleanGarbageConfig {
            max_old_indexes: Some(gc_max_old_indexes),
            min_time_garbage_mins: Some(gc_min_time_mins),
        };
        tokio::spawn(async move {
            if let Err(e) = clean_garbage(gc_store.as_ref(), &gc_root_id, gc_config).await {
                tracing::warn!(
                    error = %e,
                    root_id = %gc_root_id,
                    "Background garbage collection failed (non-fatal)"
                );
            } else {
                tracing::debug!(root_id = %gc_root_id, "Background garbage collection completed");
            }
        });

        Ok(ReindexResult {
            ledger_id,
            index_t: index_result.index_t,
            root_id: index_result.root_id,
            stats: index_result.stats,
        })
    }
}

// =============================================================================
// Ledger Config
// =============================================================================

impl crate::Fluree {
    /// Store a `LedgerConfig` blob in CAS and update the config_id on the
    /// NsRecord via ConfigPublisher.
    ///
    /// Returns the `ContentId` of the stored config blob.
    pub async fn set_ledger_config(
        &self,
        ledger_id: &str,
        config: &fluree_db_nameservice::LedgerConfig,
    ) -> Result<fluree_db_core::ContentId> {
        use fluree_db_core::ContentKind;
        use fluree_db_core::ContentStore;
        use fluree_db_nameservice::{ConfigCasResult, ConfigPayload, ConfigValue};

        let ledger_id = normalize_ledger_id(ledger_id);
        let canonical_bytes = config.to_bytes();

        // Store blob in CAS.
        let content_store = self.content_store(&ledger_id);
        let cid = content_store
            .put(ContentKind::LedgerConfig, &canonical_bytes)
            .await?;

        // Update config_id via ConfigPublisher (preserving existing payload fields).
        let publisher = self.publisher()?;
        let current = publisher.get_config(&ledger_id).await?;
        let existing_payload = current
            .as_ref()
            .and_then(|c| c.payload.clone())
            .unwrap_or_default();
        let new_config = ConfigValue::new(
            current.as_ref().map_or(1, |c| c.v + 1),
            Some(ConfigPayload {
                config_id: Some(cid.clone()),
                default_context: existing_payload.default_context,
                extra: existing_payload.extra,
            }),
        );
        match publisher
            .push_config(&ledger_id, current.as_ref(), &new_config)
            .await?
        {
            ConfigCasResult::Updated => {}
            ConfigCasResult::Conflict { .. } => {
                return Err(ApiError::Http {
                    status: 409,
                    message: format!("config for '{ledger_id}' was modified concurrently; retry"),
                });
            }
        }

        info!(ledger_id = %ledger_id, %cid, "LedgerConfig set");
        Ok(cid)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_ledger_id_with_branch() {
        assert_eq!(normalize_ledger_id("test:main"), "test:main");
        assert_eq!(normalize_ledger_id("mydb:feature"), "mydb:feature");
    }

    #[test]
    fn test_normalize_ledger_id_without_branch() {
        assert_eq!(normalize_ledger_id("test"), "test:main");
        assert_eq!(normalize_ledger_id("mydb"), "mydb:main");
    }

    #[test]
    fn test_drop_mode_default() {
        assert_eq!(DropMode::default(), DropMode::Soft);
    }

    #[test]
    fn test_drop_status_default() {
        assert_eq!(DropStatus::default(), DropStatus::NotFound);
    }
}
