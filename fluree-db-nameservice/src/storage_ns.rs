//! Storage-backed nameservice implementation
//!
//! This implementation uses any storage backend that implements the extended storage traits
//! (`Storage`, `StorageWrite`, `StorageList`, `StorageCas`) to provide a nameservice.
//!
//! This is useful for cloud deployments where you want to use S3 for both data storage
//! and nameservice, without requiring a separate DynamoDB table.
//!
//! # File Layout
//!
//! Uses the ns@v2 format compatible with legacy implementations:
//! - `{prefix}/ns@v2/{ledger-name}/{branch}.json` - Main record (commit info)
//! - `{prefix}/ns@v2/{ledger-name}/{branch}.index.json` - Index record (separate for indexer)
//!
//! # Concurrency
//!
//! Uses ETag-based compare-and-swap (CAS) operations for atomic updates.
//! Under contention, operations will retry with exponential backoff.

use crate::ns_format::{
    ns_context, BranchPointRef, IndexRef, LedgerRef, NsFileV2, NsIndexFileV2, NS_VERSION,
};
use crate::{
    deserialize_json, parse_default_context_value, serialize_json, AdminPublisher, CasResult,
    ConfigCasResult, ConfigLookup, ConfigPublisher, ConfigValue, GraphSourceLookup,
    GraphSourcePublisher, GraphSourceRecord, GraphSourceType, NameService, NameServiceError,
    NsLookupResult, NsRecord, Publisher, RefKind, RefLookup, RefPublisher, RefValue, Result,
    StatusCasResult, StatusLookup, StatusPublisher, StatusValue,
};
use async_trait::async_trait;
use fluree_db_core::ledger_id::{format_ledger_id, normalize_ledger_id, split_ledger_id};
use fluree_db_core::{
    CasAction, CasOutcome, ContentId, Error as CoreError, StorageCas, StorageList, StorageRead,
    StorageWrite,
};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

/// Storage-backed nameservice
///
/// Uses any storage backend that implements the required traits for
/// read, write, list, and CAS operations.
pub struct StorageNameService<S> {
    storage: S,
    prefix: String,
}

impl<S: Clone> Clone for StorageNameService<S> {
    fn clone(&self) -> Self {
        Self {
            storage: self.storage.clone(),
            prefix: self.prefix.clone(),
        }
    }
}

impl<S: Debug> Debug for StorageNameService<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StorageNameService")
            .field("storage", &self.storage)
            .field("prefix", &self.prefix)
            .finish()
    }
}

// =============================================================================
// Graph Source File Structures (ns@v2 format)
// =============================================================================

/// JSON structure for graph source main config file
#[derive(Debug, Clone, Serialize, Deserialize)]
struct GraphSourceNsFileV2 {
    #[serde(rename = "@context")]
    context: serde_json::Value,

    #[serde(rename = "@id")]
    id: String,

    #[serde(rename = "@type")]
    record_type: Vec<String>,

    #[serde(rename = "f:name")]
    name: String,

    #[serde(rename = "f:branch")]
    branch: String,

    #[serde(rename = "f:graphSourceConfig")]
    config: GraphSourceConfigRef,

    #[serde(rename = "f:graphSourceDependencies")]
    dependencies: Vec<String>,

    #[serde(rename = "f:status")]
    status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GraphSourceConfigRef {
    #[serde(rename = "@value")]
    value: String,
}

/// JSON structure for graph source index file
#[derive(Debug, Clone, Serialize, Deserialize)]
struct GraphSourceIndexFileV2 {
    #[serde(rename = "@context")]
    context: serde_json::Value,

    #[serde(rename = "@id")]
    id: String,

    #[serde(rename = "f:graphSourceIndex")]
    index: GraphSourceIndexRef,

    #[serde(rename = "f:graphSourceIndexT")]
    index_t: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GraphSourceIndexRef {
    #[serde(rename = "@type")]
    ref_type: String,

    #[serde(rename = "f:graphSourceIndexCid")]
    cid: String,
}

// Methods that do not depend on storage trait bounds.
impl<S> StorageNameService<S> {
    /// Create a new `NsFileV2` for initial creation.
    ///
    /// This is pure data construction and is intentionally available without
    /// requiring `S` to implement any storage traits (useful for unit tests).
    fn new_main_file(
        ledger_name: &str,
        branch: &str,
        commit_cid: Option<&str>,
        commit_t: i64,
    ) -> NsFileV2 {
        NsFileV2 {
            context: ns_context(),
            id: format_ledger_id(ledger_name, branch),
            record_type: vec!["f:LedgerSource".to_string()],
            ledger: LedgerRef {
                id: ledger_name.to_string(),
            },
            branch: branch.to_string(),
            commit_cid: commit_cid.map(std::string::ToString::to_string),
            config_cid: None,
            t: commit_t,
            index: None,
            status: "ready".to_string(),
            default_context_cid: None,
            // v2 extension fields
            status_v: Some(1),
            status_meta: None,
            config_v: Some(0),
            config_meta: None,
            source_branch: None,
            branch_point: None,
            branches: 0,
        }
    }
}

impl<S> StorageNameService<S>
where
    S: StorageRead + StorageWrite + StorageList + StorageCas + Debug,
{
    /// Create a new storage-backed nameservice
    ///
    /// # Arguments
    ///
    /// * `storage` - Storage backend implementing required traits
    /// * `prefix` - Optional prefix for all keys (e.g., "ledgers")
    pub fn new(storage: S, prefix: impl Into<String>) -> Self {
        Self {
            storage,
            prefix: prefix.into(),
        }
    }

    /// Get the storage key for the main ns record
    fn ns_key(&self, ledger_name: &str, branch: &str) -> String {
        if self.prefix.is_empty() {
            format!("{NS_VERSION}/{ledger_name}/{branch}.json")
        } else {
            format!(
                "{}/{}/{}/{}.json",
                self.prefix, NS_VERSION, ledger_name, branch
            )
        }
    }

    /// Get the storage key for the index-only ns record
    fn index_key(&self, ledger_name: &str, branch: &str) -> String {
        if self.prefix.is_empty() {
            format!("{NS_VERSION}/{ledger_name}/{branch}.index.json")
        } else {
            format!(
                "{}/{}/{}/{}.index.json",
                self.prefix, NS_VERSION, ledger_name, branch
            )
        }
    }

    /// Check if a record is a graph source by reading and checking @type.
    async fn is_graph_source_record(&self, name: &str, branch: &str) -> Result<bool> {
        let key = self.ns_key(name, branch);

        match self.storage.read_bytes(&key).await {
            Ok(bytes) => Ok(Self::is_graph_source_from_bytes(&bytes)),
            Err(CoreError::NotFound(_)) => Ok(false),
            Err(e) => Err(NameServiceError::storage(format!(
                "Failed to read {key}: {e}"
            ))),
        }
    }

    /// Check if raw JSON bytes represent a graph source record (exact match).
    fn is_graph_source_from_bytes(bytes: &[u8]) -> bool {
        let Ok(parsed) = serde_json::from_slice::<serde_json::Value>(bytes) else {
            return false;
        };
        Self::is_graph_source_from_json(&parsed)
    }

    /// Check if parsed JSON represents a graph source record.
    fn is_graph_source_from_json(parsed: &serde_json::Value) -> bool {
        if let Some(types) = parsed.get("@type").and_then(|t| t.as_array()) {
            for t in types {
                if let Some(s) = t.as_str() {
                    if s == "f:IndexSource"
                        || s == "f:MappedSource"
                        || s == fluree_vocab::ns_types::INDEX_SOURCE
                        || s == fluree_vocab::ns_types::MAPPED_SOURCE
                    {
                        return true;
                    }
                }
            }
        }
        false
    }

    /// Load a graph source record and merge with index file
    async fn load_graph_source_record(
        &self,
        name: &str,
        branch: &str,
    ) -> Result<Option<GraphSourceRecord>> {
        let main_key = self.ns_key(name, branch);

        // Read main record
        let main_file: Option<GraphSourceNsFileV2> = self.read_json(&main_key).await?;

        let Some(main) = main_file else {
            return Ok(None);
        };

        self.graph_source_file_to_record(main, name, branch).await
    }

    /// Convert already-parsed GraphSourceNsFileV2 to GraphSourceRecord, merging with index file.
    /// This avoids re-reading the main file when we've already parsed it.
    async fn graph_source_file_to_record(
        &self,
        main: GraphSourceNsFileV2,
        name: &str,
        branch: &str,
    ) -> Result<Option<GraphSourceRecord>> {
        let index_key = self.index_key(name, branch);

        // Determine graph source type from @type array (exclude the kind types).
        let source_type = main
            .record_type
            .iter()
            .find(|t| {
                !matches!(
                    t.as_str(),
                    "f:IndexSource"
                        | "f:MappedSource"
                        | fluree_vocab::ns_types::INDEX_SOURCE
                        | fluree_vocab::ns_types::MAPPED_SOURCE
                )
            })
            .map(|t| GraphSourceType::from_type_string(t))
            .unwrap_or(GraphSourceType::Unknown("unknown".to_string()));

        // Convert to GraphSourceRecord
        let mut record = GraphSourceRecord {
            graph_source_id: format_ledger_id(name, branch),
            name: main.name,
            branch: main.branch,
            source_type,
            config: main.config.value,
            dependencies: main.dependencies,
            index_id: None,
            index_t: 0,
            retracted: main.status == "retracted",
        };

        // Read index file (if exists) and merge
        let index_file: Option<GraphSourceIndexFileV2> = self.read_json(&index_key).await?;
        if let Some(idx) = index_file {
            record.index_id = idx.index.cid.parse::<ContentId>().ok();
            record.index_t = idx.index_t;
        }

        Ok(Some(record))
    }

    /// Read and parse a JSON file from storage
    async fn read_json<T: for<'de> Deserialize<'de>>(&self, key: &str) -> Result<Option<T>> {
        match self.storage.read_bytes(key).await {
            Ok(bytes) => {
                let parsed = serde_json::from_slice(&bytes)?;
                Ok(Some(parsed))
            }
            Err(CoreError::NotFound(_)) => Ok(None),
            Err(e) => Err(NameServiceError::storage(format!(
                "Failed to read {key}: {e}"
            ))),
        }
    }

    /// Load and merge main record with index file
    async fn load_record(&self, ledger_name: &str, branch: &str) -> Result<Option<NsRecord>> {
        let main_key = self.ns_key(ledger_name, branch);
        let index_key = self.index_key(ledger_name, branch);

        // Read main record
        let main_file: Option<NsFileV2> = self.read_json(&main_key).await?;

        let Some(main) = main_file else {
            return Ok(None);
        };

        // Read index file (if exists)
        let index_file: Option<NsIndexFileV2> = self.read_json(&index_key).await?;

        // Convert to NsRecord, parsing persisted CID strings
        let mut record = NsRecord {
            ledger_id: format_ledger_id(ledger_name, branch),
            name: main.ledger.id.clone(),
            branch: main.branch,
            commit_head_id: main
                .commit_cid
                .as_deref()
                .and_then(|s| s.parse::<ContentId>().ok()),
            config_id: main
                .config_cid
                .as_deref()
                .and_then(|s| s.parse::<ContentId>().ok()),
            commit_t: main.t,
            index_head_id: main
                .index
                .as_ref()
                .and_then(|i| i.cid.as_deref())
                .and_then(|s| s.parse::<ContentId>().ok()),
            index_t: main.index.as_ref().map(|i| i.t).unwrap_or(0),
            default_context: main
                .default_context_cid
                .as_deref()
                .and_then(parse_default_context_value),
            retracted: main.status == "retracted",
            source_branch: main
                .source_branch
                .or_else(|| main.branch_point.map(|bp| bp.source)),
            branches: main.branches,
        };

        // Merge index file if it has equal or higher t (READ-TIME merge rule)
        if let Some(index_data) = index_file {
            if index_data.index.t >= record.index_t {
                record.index_head_id = index_data
                    .index
                    .cid
                    .as_deref()
                    .and_then(|s| s.parse::<ContentId>().ok());
                record.index_t = index_data.index.t;
            }
        }

        Ok(Some(record))
    }

    /// Perform an atomic read-modify-write on a JSON value.
    ///
    /// Reads the current value at `key`, deserializes it, applies `update_fn`,
    /// and writes the result back atomically. If the closure returns `None`,
    /// no write is performed.
    async fn cas_update<T, F>(&self, key: &str, update_fn: F) -> Result<()>
    where
        T: Serialize + for<'de> Deserialize<'de>,
        F: Fn(Option<T>) -> Option<T> + Send + Sync,
    {
        let outcome = self
            .storage
            .compare_and_swap(key, |current_bytes| {
                let current: Option<T> = current_bytes.map(deserialize_json).transpose()?;

                match update_fn(current) {
                    Some(value) => {
                        let bytes = serialize_json(&value)?;
                        Ok(CasAction::Write(bytes))
                    }
                    None => Ok(CasAction::Abort(())),
                }
            })
            .await
            .map_err(|e| NameServiceError::storage(format!("CAS update failed for {key}: {e}")))?;

        match outcome {
            CasOutcome::Written | CasOutcome::Aborted(()) => Ok(()),
        }
    }

    /// Atomic read-modify-write that returns an outcome decided by the closure.
    ///
    /// Unlike `cas_update`, this lets the closure signal "I decided not to update" as
    /// a non-error condition, carrying an application-level result out.
    async fn cas_update_with_outcome<T, F>(
        &self,
        key: &str,
        update_fn: F,
    ) -> Result<CasUpdateOutcome>
    where
        T: Serialize + for<'de> Deserialize<'de>,
        F: Fn(Option<T>) -> CasUpdateDecision<T> + Send + Sync,
    {
        let outcome = self
            .storage
            .compare_and_swap(key, |current_bytes| {
                let current: Option<T> = current_bytes.map(deserialize_json).transpose()?;

                match update_fn(current) {
                    CasUpdateDecision::Apply(value) => {
                        let bytes = serialize_json(&value)?;
                        Ok(CasAction::Write(bytes))
                    }
                    CasUpdateDecision::Skip(result) => Ok(CasAction::Abort(result)),
                }
            })
            .await
            .map_err(|e| NameServiceError::storage(format!("CAS update failed for {key}: {e}")))?;

        match outcome {
            CasOutcome::Written => Ok(CasUpdateOutcome::Updated),
            CasOutcome::Aborted(result) => Ok(CasUpdateOutcome::Skipped(result)),
        }
    }
}

/// Decision returned by a `cas_update_with_outcome` closure.
enum CasUpdateDecision<T> {
    /// Apply the update (write this value).
    Apply(T),
    /// Skip the update (closure decided not to proceed). Carries a `CasResult`
    /// so the caller can report the reason (e.g. address mismatch, monotonic guard).
    Skip(CasResult),
}

/// Outcome of `cas_update_with_outcome`.
enum CasUpdateOutcome {
    /// The value was written successfully.
    Updated,
    /// The closure decided to skip (returned `CasUpdateDecision::Skip`).
    Skipped(CasResult),
}

#[async_trait]
impl<S> NameService for StorageNameService<S>
where
    S: StorageRead + StorageWrite + StorageList + StorageCas + Debug + Send + Sync,
{
    async fn lookup(&self, ledger_id: &str) -> Result<Option<NsRecord>> {
        let (ledger_name, branch) = split_ledger_id(ledger_id)?;
        self.load_record(&ledger_name, &branch).await
    }

    async fn list_branches(&self, ledger_name: &str) -> Result<Vec<NsRecord>> {
        let prefix = if self.prefix.is_empty() {
            format!("{NS_VERSION}/{ledger_name}/")
        } else {
            format!("{}/{}/{}/", self.prefix, NS_VERSION, ledger_name)
        };

        let keys = StorageList::list_prefix(&self.storage, &prefix)
            .await
            .map_err(|e| NameServiceError::storage(format!("Failed to list branches: {e}")))?;

        let mut records = Vec::new();

        for key in keys {
            if key.ends_with(".index.json") || !key.ends_with(".json") {
                continue;
            }

            // Extract branch name from the key suffix
            let file_part = key.rsplit('/').next().unwrap_or("");
            let branch = file_part.trim_end_matches(".json");

            if let Ok(Some(record)) = self.load_record(ledger_name, branch).await {
                if !record.retracted {
                    records.push(record);
                }
            }
        }

        Ok(records)
    }

    async fn all_records(&self) -> Result<Vec<NsRecord>> {
        let prefix = if self.prefix.is_empty() {
            NS_VERSION.to_string()
        } else {
            format!("{}/{}", self.prefix, NS_VERSION)
        };

        // List all files under ns@v2
        let keys = StorageList::list_prefix(&self.storage, &prefix)
            .await
            .map_err(|e| NameServiceError::storage(format!("Failed to list records: {e}")))?;

        let mut records = Vec::new();

        for key in keys {
            // Skip index files
            if key.ends_with(".index.json") {
                continue;
            }

            if !key.ends_with(".json") {
                continue;
            }

            // Parse ledger name and branch from key
            // Key format: {prefix}/ns@v2/{ledger-name}/{branch}.json
            let path_part = if self.prefix.is_empty() {
                key.strip_prefix(&format!("{NS_VERSION}/"))
            } else {
                key.strip_prefix(&format!("{}/{}/", self.prefix, NS_VERSION))
            };

            if let Some(path) = path_part {
                // path is now "{ledger-name}/{branch}.json"
                if let Some(slash_pos) = path.rfind('/') {
                    let ledger_name = &path[..slash_pos];
                    let branch = path[slash_pos + 1..].trim_end_matches(".json");

                    if let Ok(Some(record)) = self.load_record(ledger_name, branch).await {
                        records.push(record);
                    }
                }
            }
        }

        Ok(records)
    }

    async fn create_branch(
        &self,
        ledger_name: &str,
        new_branch: &str,
        source_branch: &str,
        at_commit: Option<(ContentId, i64)>,
    ) -> Result<()> {
        let key = self.ns_key(ledger_name, new_branch);
        let normalized_id = format_ledger_id(ledger_name, new_branch);

        // Read the source branch to validate it exists (and to get commit info
        // when `at_commit` is None).
        let source_record = self
            .load_record(ledger_name, source_branch)
            .await?
            .ok_or_else(|| {
                NameServiceError::not_found(format!("source branch {ledger_name}:{source_branch}"))
            })?;

        let (commit_head_id, commit_t) = match at_commit {
            Some((id, t)) => (Some(id), t),
            None => (source_record.commit_head_id.clone(), source_record.commit_t),
        };

        let file = NsFileV2 {
            context: ns_context(),
            id: normalized_id.clone(),
            record_type: vec!["f:LedgerSource".to_string()],
            ledger: LedgerRef {
                id: ledger_name.to_string(),
            },
            branch: new_branch.to_string(),
            commit_cid: commit_head_id
                .as_ref()
                .map(std::string::ToString::to_string),
            config_cid: None,
            t: commit_t,
            index: None,
            status: "ready".to_string(),
            default_context_cid: None,
            status_v: Some(1),
            status_meta: None,
            config_v: Some(0),
            config_meta: None,
            source_branch: Some(source_branch.to_string()),
            branch_point: Some(BranchPointRef {
                source: source_branch.to_string(),
                commit_cid: None,
                t: 0,
            }),
            branches: 0,
        };
        let bytes = serde_json::to_vec_pretty(&file)
            .map_err(|e| NameServiceError::storage(e.to_string()))?;

        let created = self.storage.insert(&key, &bytes).await?;
        if !created {
            return Err(NameServiceError::ledger_already_exists(&normalized_id));
        }

        // Increment source branch's child count
        let source_key = self.ns_key(ledger_name, source_branch);
        self.cas_update::<NsFileV2, _>(&source_key, |existing| {
            let mut file = existing?;
            file.branches += 1;
            Some(file)
        })
        .await?;

        Ok(())
    }

    async fn drop_branch(&self, ledger_id: &str) -> Result<Option<u32>> {
        let (ledger_name, branch) = split_ledger_id(ledger_id)?;

        // Read the record to find the parent before purging
        let record = self
            .load_record(&ledger_name, &branch)
            .await?
            .ok_or_else(|| NameServiceError::not_found(ledger_id))?;

        let parent_source = record.source_branch.clone();

        // Remove the NS files
        let main_key = self.ns_key(&ledger_name, &branch);
        let index_key = self.index_key(&ledger_name, &branch);
        let _ = self.storage.delete(&main_key).await;
        let _ = self.storage.delete(&index_key).await;

        // Decrement parent's child count if this branch had a parent
        match parent_source {
            Some(source) => {
                let parent_key = self.ns_key(&ledger_name, &source);
                self.cas_update::<NsFileV2, _>(&parent_key, move |existing| {
                    let mut file = existing?;
                    file.branches = file.branches.saturating_sub(1);
                    Some(file)
                })
                .await?;
                // Re-read the parent to get the updated count
                let parent_record = self.load_record(&ledger_name, &source).await?;
                Ok(parent_record.map(|r| r.branches))
            }
            None => Ok(None),
        }
    }

    async fn reset_head(&self, ledger_id: &str, snapshot: crate::NsRecordSnapshot) -> Result<()> {
        let (ledger_name, branch) = split_ledger_id(ledger_id)?;
        let key = self.ns_key(&ledger_name, &branch);

        let outcome = self
            .storage
            .compare_and_swap(&key, |bytes| {
                let Some(data) = bytes else {
                    return Ok(CasAction::Abort(()));
                };
                let mut file: NsFileV2 = deserialize_json(data)?;
                file.apply_snapshot(&snapshot);
                let new_bytes = serialize_json(&file)?;
                Ok(CasAction::Write(new_bytes))
            })
            .await?;

        if matches!(outcome, CasOutcome::Aborted(())) {
            return Err(NameServiceError::not_found(ledger_id));
        }

        Ok(())
    }
}

#[async_trait]
impl<S> Publisher for StorageNameService<S>
where
    S: StorageRead + StorageWrite + StorageList + StorageCas + Debug + Send + Sync,
{
    async fn publish_ledger_init(&self, ledger_id: &str) -> Result<()> {
        let (ledger_name, branch) = split_ledger_id(ledger_id)?;
        let key = self.ns_key(&ledger_name, &branch);
        let normalized_address = format_ledger_id(&ledger_name, &branch);

        // Create minimal record with no commits
        let file = NsFileV2 {
            context: ns_context(),
            id: normalized_address.clone(),
            record_type: vec!["f:LedgerSource".to_string()],
            ledger: LedgerRef {
                id: ledger_name.clone(),
            },
            branch: branch.clone(),
            commit_cid: None,
            config_cid: None,
            t: 0,
            index: None,
            status: "ready".to_string(),
            default_context_cid: None,
            // v2 extension fields
            status_v: Some(1),
            status_meta: None,
            config_v: Some(0),
            config_meta: None,
            source_branch: None,
            branch_point: None,
            branches: 0,
        };

        let bytes = serde_json::to_vec_pretty(&file)?;

        // Use insert for atomic create-if-not-exists
        match self.storage.insert(&key, &bytes).await {
            Ok(true) => Ok(()), // Successfully created
            Ok(false) => {
                // Record exists — check if it's retracted (dropped) and allow re-creation
                match self.storage.read_bytes(&key).await {
                    Ok(existing_bytes) => {
                        let existing: NsFileV2 = serde_json::from_slice(&existing_bytes)?;
                        if existing.status == "retracted" {
                            // Overwrite the retracted record with a fresh one
                            self.storage.write_bytes(&key, &bytes).await.map_err(|e| {
                                NameServiceError::storage(format!(
                                    "Failed to re-create ledger {normalized_address}: {e}"
                                ))
                            })?;
                            // Clean up stale index sidecar
                            let idx_key = self.index_key(&ledger_name, &branch);
                            let _ = self.storage.delete(&idx_key).await;
                            return Ok(());
                        }
                        Err(NameServiceError::ledger_already_exists(normalized_address))
                    }
                    Err(_) => Err(NameServiceError::ledger_already_exists(normalized_address)),
                }
            }
            Err(e) => Err(NameServiceError::storage(format!(
                "Failed to create ledger {normalized_address}: {e}"
            ))),
        }
    }

    async fn publish_commit(
        &self,
        ledger_id: &str,
        commit_t: i64,
        commit_id: &ContentId,
    ) -> Result<()> {
        let (ledger_name, branch) = split_ledger_id(ledger_id)?;
        let key = self.ns_key(&ledger_name, &branch);

        let ledger_name_clone = ledger_name.clone();
        let branch_clone = branch.clone();
        let cid_str = commit_id.to_string();

        self.cas_update::<NsFileV2, _>(&key, move |existing| {
            match existing {
                Some(mut file) => {
                    // Only update if strictly newer
                    if commit_t > file.t {
                        file.commit_cid = Some(cid_str.clone());
                        file.t = commit_t;
                        Some(file)
                    } else {
                        None // No update needed
                    }
                }
                None => {
                    // Create new record
                    Some(Self::new_main_file(
                        &ledger_name_clone,
                        &branch_clone,
                        Some(&cid_str),
                        commit_t,
                    ))
                }
            }
        })
        .await
    }

    async fn publish_index(
        &self,
        ledger_id: &str,
        index_t: i64,
        index_id: &ContentId,
    ) -> Result<()> {
        let (ledger_name, branch) = split_ledger_id(ledger_id)?;
        let key = self.index_key(&ledger_name, &branch);

        let cid_str = index_id.to_string();

        self.cas_update::<NsIndexFileV2, _>(&key, move |existing| {
            // Only update if strictly newer
            if let Some(ref file) = existing {
                if index_t <= file.index.t {
                    return None;
                }
            }

            Some(NsIndexFileV2 {
                context: ns_context(),
                index: IndexRef {
                    cid: Some(cid_str.clone()),
                    t: index_t,
                },
            })
        })
        .await
    }

    async fn retract(&self, ledger_id: &str) -> Result<()> {
        let (ledger_name, branch) = split_ledger_id(ledger_id)?;
        let key = self.ns_key(&ledger_name, &branch);

        self.cas_update::<NsFileV2, _>(&key, |existing| {
            let mut file = existing?;
            if file.status == "retracted" {
                return None; // Already retracted
            }
            file.status = "retracted".to_string();
            // Advance status_v when retracting
            let current_v = file.status_v.unwrap_or(1);
            file.status_v = Some(current_v + 1);
            Some(file)
        })
        .await
    }

    fn publishing_ledger_id(&self, ledger_id: &str) -> Option<String> {
        // Return normalized ledger ID for publishing
        Some(normalize_ledger_id(ledger_id).unwrap_or_else(|_| ledger_id.to_string()))
    }
}

#[async_trait]
impl<S> AdminPublisher for StorageNameService<S>
where
    S: StorageRead + StorageWrite + StorageList + StorageCas + Debug + Send + Sync,
{
    async fn publish_index_allow_equal(
        &self,
        ledger_id: &str,
        index_t: i64,
        index_id: &ContentId,
    ) -> Result<()> {
        let (ledger_name, branch) = split_ledger_id(ledger_id)?;
        let index_key = self.index_key(&ledger_name, &branch);
        let cid_str = index_id.to_string();

        self.cas_update::<NsIndexFileV2, _>(&index_key, |existing| {
            let should_update = match &existing {
                Some(file) => index_t >= file.index.t, // Allow equal
                None => true,
            };

            if should_update {
                Some(NsIndexFileV2 {
                    context: ns_context(),
                    index: IndexRef {
                        cid: Some(cid_str.clone()),
                        t: index_t,
                    },
                })
            } else {
                None
            }
        })
        .await

        // Note: StorageNameService has no event_tx (no Publication support),
        // so we don't emit NameServiceEvent here. This mirrors existing
        // publish_index() behavior for StorageNameService.
    }
}

#[async_trait]
impl<S> RefLookup for StorageNameService<S>
where
    S: StorageRead + StorageWrite + StorageList + StorageCas + Debug + Send + Sync,
{
    async fn get_ref(&self, ledger_id: &str, kind: RefKind) -> Result<Option<RefValue>> {
        let (ledger_name, branch) = split_ledger_id(ledger_id)?;

        match kind {
            RefKind::CommitHead => {
                let key = self.ns_key(&ledger_name, &branch);
                let file: Option<NsFileV2> = self.read_json(&key).await?;
                Ok(file.map(|f| RefValue {
                    id: f
                        .commit_cid
                        .as_deref()
                        .and_then(|s| s.parse::<ContentId>().ok()),
                    t: f.t,
                }))
            }
            RefKind::IndexHead => {
                // Read both main and index files, take the one with higher t
                // (same merge rule as load_record)
                let main_key = self.ns_key(&ledger_name, &branch);
                let index_key = self.index_key(&ledger_name, &branch);

                let main_file: Option<NsFileV2> = self.read_json(&main_key).await?;
                let index_file: Option<NsIndexFileV2> = self.read_json(&index_key).await?;

                let main_index = main_file.as_ref().and_then(|f| {
                    f.index.as_ref().map(|i| RefValue {
                        id: i.cid.as_deref().and_then(|s| s.parse::<ContentId>().ok()),
                        t: i.t,
                    })
                });

                let separate_index = index_file.map(|f| RefValue {
                    id: f
                        .index
                        .cid
                        .as_deref()
                        .and_then(|s| s.parse::<ContentId>().ok()),
                    t: f.index.t,
                });

                // If main file doesn't exist at all, the ref is unknown
                if main_file.is_none() {
                    return Ok(None);
                }

                // Merge: take whichever has higher t, preferring separate index file
                match (main_index, separate_index) {
                    (None, None) => Ok(Some(RefValue { id: None, t: 0 })),
                    (Some(m), None) => Ok(Some(m)),
                    (None, Some(s)) => Ok(Some(s)),
                    (Some(m), Some(s)) => {
                        if s.t >= m.t {
                            Ok(Some(s))
                        } else {
                            Ok(Some(m))
                        }
                    }
                }
            }
        }
    }
}

#[async_trait]
impl<S> RefPublisher for StorageNameService<S>
where
    S: StorageRead + StorageWrite + StorageList + StorageCas + Debug + Send + Sync,
{
    async fn compare_and_set_ref(
        &self,
        ledger_id: &str,
        kind: RefKind,
        expected: Option<&RefValue>,
        new: &RefValue,
    ) -> Result<CasResult> {
        let (ledger_name, branch) = split_ledger_id(ledger_id)?;

        match kind {
            RefKind::CommitHead => {
                let key = self.ns_key(&ledger_name, &branch);
                let new_cid = new.id.clone();
                let new_cid_str = new.id.as_ref().map(std::string::ToString::to_string);
                let new_t = new.t;
                let expected_id = expected.and_then(|e| e.id.clone());
                let expect_exists = expected.is_some();

                let outcome = self
                    .cas_update_with_outcome::<NsFileV2, _>(&key, move |existing| {
                        let current_ref = existing.as_ref().map(|f| RefValue {
                            id: f
                                .commit_cid
                                .as_deref()
                                .and_then(|s| s.parse::<ContentId>().ok()),
                            t: f.t,
                        });

                        // Compare expected with current
                        match (expect_exists, &current_ref) {
                            (false, None) => {
                                // Create new record
                                return CasUpdateDecision::Apply(
                                    StorageNameService::<S>::new_main_file(
                                        &ledger_name,
                                        &branch,
                                        new_cid_str.as_deref(),
                                        new_t,
                                    ),
                                );
                            }
                            (false, Some(actual)) => {
                                return CasUpdateDecision::Skip(CasResult::Conflict {
                                    actual: Some(actual.clone()),
                                });
                            }
                            (true, None) => {
                                return CasUpdateDecision::Skip(CasResult::Conflict {
                                    actual: None,
                                });
                            }
                            (true, Some(actual)) => {
                                // Compare by content id
                                let identity_matches = match (&expected_id, &actual.id) {
                                    (Some(a), Some(b)) => a == b,
                                    (None, None) => true,
                                    _ => false,
                                };
                                if !identity_matches {
                                    return CasUpdateDecision::Skip(CasResult::Conflict {
                                        actual: Some(actual.clone()),
                                    });
                                }
                                // Identity matches — check monotonic guard (strict for CommitHead)
                                if new_t <= actual.t {
                                    return CasUpdateDecision::Skip(CasResult::Conflict {
                                        actual: Some(actual.clone()),
                                    });
                                }
                            }
                        }

                        // Apply the update
                        let mut file = existing.unwrap();
                        file.commit_cid = new_cid.as_ref().map(std::string::ToString::to_string);
                        file.t = new_t;
                        CasUpdateDecision::Apply(file)
                    })
                    .await?;

                match outcome {
                    CasUpdateOutcome::Updated => Ok(CasResult::Updated),
                    CasUpdateOutcome::Skipped(cas) => Ok(cas),
                }
            }
            RefKind::IndexHead => {
                let key = self.index_key(&ledger_name, &branch);
                let new_cid = new.id.clone();
                let new_t = new.t;
                let expected_id = expected.and_then(|e| e.id.clone());
                let expect_exists = expected.is_some();

                let outcome = self
                    .cas_update_with_outcome::<NsIndexFileV2, _>(&key, move |existing| {
                        let current_ref = existing.as_ref().map(|f| RefValue {
                            id: f
                                .index
                                .cid
                                .as_deref()
                                .and_then(|s| s.parse::<ContentId>().ok()),
                            t: f.index.t,
                        });

                        match (expect_exists, &current_ref) {
                            (false, None) => {
                                // Create new index record
                                return CasUpdateDecision::Apply(NsIndexFileV2 {
                                    context: ns_context(),
                                    index: IndexRef {
                                        cid: new_cid.as_ref().map(std::string::ToString::to_string),
                                        t: new_t,
                                    },
                                });
                            }
                            (false, Some(actual)) => {
                                return CasUpdateDecision::Skip(CasResult::Conflict {
                                    actual: Some(actual.clone()),
                                });
                            }
                            (true, None) => {
                                // The separate index file doesn't exist yet.
                                // get_ref returns Some(RefValue { id: None, t: 0 })
                                // for a freshly created ledger (from the main file
                                // fallback). Allow if the caller expected that empty
                                // state; otherwise conflict.
                                let expected_is_empty = expected_id.is_none();
                                if !expected_is_empty {
                                    return CasUpdateDecision::Skip(CasResult::Conflict {
                                        actual: None,
                                    });
                                }
                                // Treat as create — fall through to apply
                                return CasUpdateDecision::Apply(NsIndexFileV2 {
                                    context: ns_context(),
                                    index: IndexRef {
                                        cid: new_cid.as_ref().map(std::string::ToString::to_string),
                                        t: new_t,
                                    },
                                });
                            }
                            (true, Some(actual)) => {
                                // Compare by content id
                                let identity_matches = match (&expected_id, &actual.id) {
                                    (Some(a), Some(b)) => a == b,
                                    (None, None) => true,
                                    _ => false,
                                };
                                if !identity_matches {
                                    return CasUpdateDecision::Skip(CasResult::Conflict {
                                        actual: Some(actual.clone()),
                                    });
                                }
                                // Non-strict for IndexHead: new.t >= current.t
                                if new_t < actual.t {
                                    return CasUpdateDecision::Skip(CasResult::Conflict {
                                        actual: Some(actual.clone()),
                                    });
                                }
                            }
                        }

                        let mut file = existing.unwrap();
                        file.index = IndexRef {
                            cid: new_cid.as_ref().map(std::string::ToString::to_string),
                            t: new_t,
                        };
                        CasUpdateDecision::Apply(file)
                    })
                    .await?;

                match outcome {
                    CasUpdateOutcome::Updated => Ok(CasResult::Updated),
                    CasUpdateOutcome::Skipped(cas) => Ok(cas),
                }
            }
        }
    }

    // Note: StorageNameService has no event_tx (no Publication support),
    // so no events are emitted on CAS success. Uses the default
    // fast_forward_commit implementation from the trait.
}

#[async_trait]
impl<S> GraphSourcePublisher for StorageNameService<S>
where
    S: StorageRead + StorageWrite + StorageList + StorageCas + Debug + Send + Sync,
{
    async fn publish_graph_source(
        &self,
        name: &str,
        branch: &str,
        source_type: GraphSourceType,
        config: &str,
        dependencies: &[String],
    ) -> Result<()> {
        let key = self.ns_key(name, branch);

        let name = name.to_string();
        let branch = branch.to_string();
        let config = config.to_string();
        let dependencies = dependencies.to_vec();
        let kind_type_str = match source_type.kind() {
            crate::GraphSourceKind::Index => "f:IndexSource".to_string(),
            crate::GraphSourceKind::Mapped => "f:MappedSource".to_string(),
            crate::GraphSourceKind::Ledger => "f:LedgerSource".to_string(),
        };
        let source_type_str = source_type.to_type_string();

        self.cas_update::<GraphSourceNsFileV2, _>(&key, move |existing| {
            // Clone captured values so closure is Fn (can be called multiple times for retry)
            let name = name.clone();
            let branch = branch.clone();
            let config = config.clone();
            let dependencies = dependencies.clone();
            let kind_type_str = kind_type_str.clone();
            let source_type_str = source_type_str.clone();

            // For graph source config, we always update (config changes are allowed)
            // Only preserve retracted status if already set
            let status = existing
                .as_ref()
                .map(|f| f.status.clone())
                .filter(|s| s == "retracted")
                .unwrap_or_else(|| "ready".to_string());

            Some(GraphSourceNsFileV2 {
                context: ns_context(),
                id: format_ledger_id(&name, &branch),
                record_type: vec![kind_type_str, source_type_str],
                name,
                branch,
                config: GraphSourceConfigRef { value: config },
                dependencies,
                status,
            })
        })
        .await
    }

    async fn publish_graph_source_index(
        &self,
        name: &str,
        branch: &str,
        index_id: &ContentId,
        index_t: i64,
    ) -> Result<()> {
        let key = self.index_key(name, branch);

        let name = name.to_string();
        let branch = branch.to_string();
        let cid_str = index_id.to_string();

        self.cas_update::<GraphSourceIndexFileV2, _>(&key, move |existing| {
            // Clone captured values so closure is Fn (can be called multiple times for retry)
            let name = name.clone();
            let branch = branch.clone();
            let cid_str = cid_str.clone();

            // Strictly monotonic: only update if new_t > existing_t
            if let Some(ref file) = existing {
                if index_t <= file.index_t {
                    return None;
                }
            }

            Some(GraphSourceIndexFileV2 {
                context: ns_context(),
                id: format_ledger_id(&name, &branch),
                index: GraphSourceIndexRef {
                    ref_type: "f:ContentId".to_string(),
                    cid: cid_str,
                },
                index_t,
            })
        })
        .await
    }

    async fn retract_graph_source(&self, name: &str, branch: &str) -> Result<()> {
        let key = self.ns_key(name, branch);

        self.cas_update::<GraphSourceNsFileV2, _>(&key, |existing| {
            let mut file = existing?;
            if file.status == "retracted" {
                return None;
            }
            file.status = "retracted".to_string();
            Some(file)
        })
        .await
    }
}

#[async_trait]
impl<S> GraphSourceLookup for StorageNameService<S>
where
    S: StorageRead + StorageWrite + StorageList + StorageCas + Debug + Send + Sync,
{
    async fn lookup_graph_source(
        &self,
        graph_source_id: &str,
    ) -> Result<Option<GraphSourceRecord>> {
        let (name, branch) = split_ledger_id(graph_source_id)?;

        if !self.is_graph_source_record(&name, &branch).await? {
            return Ok(None);
        }

        self.load_graph_source_record(&name, &branch).await
    }

    async fn lookup_any(&self, resource_id: &str) -> Result<NsLookupResult> {
        let (name, branch) = split_ledger_id(resource_id)?;
        let key = self.ns_key(&name, &branch);

        // Check if file exists
        match self.storage.read_bytes(&key).await {
            Ok(_) => {}
            Err(CoreError::NotFound(_)) => return Ok(NsLookupResult::NotFound),
            Err(e) => {
                return Err(NameServiceError::storage(format!(
                    "Failed to read {key}: {e}"
                )))
            }
        }

        // Check if it's a graph source record
        if self.is_graph_source_record(&name, &branch).await? {
            match self.load_graph_source_record(&name, &branch).await? {
                Some(record) => Ok(NsLookupResult::GraphSource(record)),
                None => Ok(NsLookupResult::NotFound),
            }
        } else {
            // It's a ledger record
            match self.load_record(&name, &branch).await? {
                Some(record) => Ok(NsLookupResult::Ledger(record)),
                None => Ok(NsLookupResult::NotFound),
            }
        }
    }

    async fn all_graph_source_records(&self) -> Result<Vec<GraphSourceRecord>> {
        let prefix = if self.prefix.is_empty() {
            NS_VERSION.to_string()
        } else {
            format!("{}/{}", self.prefix, NS_VERSION)
        };

        // List all files under ns@v2
        let keys = StorageList::list_prefix(&self.storage, &prefix)
            .await
            .map_err(|e| NameServiceError::storage(format!("Failed to list records: {e}")))?;

        let mut records = Vec::new();

        for key in keys {
            // Skip index files and snapshot files
            if key.ends_with(".index.json") || key.ends_with(".snapshots.json") {
                continue;
            }

            if !key.ends_with(".json") {
                continue;
            }

            // Parse name and branch from key
            // Key format: {prefix}/ns@v2/{name}/{branch}.json
            let path_part = if self.prefix.is_empty() {
                key.strip_prefix(&format!("{NS_VERSION}/"))
            } else {
                key.strip_prefix(&format!("{}/{}/", self.prefix, NS_VERSION))
            };

            let Some(path) = path_part else { continue };

            // path is now "{name}/{branch}.json"
            let Some(slash_pos) = path.rfind('/') else {
                continue;
            };

            let name = &path[..slash_pos];
            let branch = path[slash_pos + 1..].trim_end_matches(".json");

            // Single read: fetch bytes, check type, and convert if graph source
            // This avoids 2-3 reads per record on S3.
            let bytes = match self.storage.read_bytes(&key).await {
                Ok(b) => b,
                Err(CoreError::NotFound(_)) => continue,
                Err(e) => {
                    tracing::warn!(key = %key, error = %e, "Failed to read NS record, skipping");
                    continue;
                }
            };

            // Check if graph source from raw bytes (avoids full parse if not graph source)
            if !Self::is_graph_source_from_bytes(&bytes) {
                continue;
            }

            // Parse as GraphSourceNsFileV2
            let main: GraphSourceNsFileV2 = match serde_json::from_slice(&bytes) {
                Ok(f) => f,
                Err(e) => {
                    tracing::warn!(key = %key, error = %e, "Failed to parse graph source record, skipping");
                    continue;
                }
            };

            // Convert to GraphSourceRecord (reads index file if exists)
            match self.graph_source_file_to_record(main, name, branch).await {
                Ok(Some(record)) => records.push(record),
                Ok(None) => {}
                Err(e) => {
                    tracing::warn!(
                        name = %name, branch = %branch, error = %e,
                        "Failed to load graph source record, skipping"
                    );
                }
            }
        }

        Ok(records)
    }
}

// ---------------------------------------------------------------------------
// V2 Extension: StatusPublisher and ConfigPublisher
// ---------------------------------------------------------------------------

#[async_trait]
impl<S> StatusLookup for StorageNameService<S>
where
    S: StorageRead + StorageWrite + StorageList + StorageCas + Debug + Send + Sync,
{
    async fn get_status(&self, ledger_id: &str) -> Result<Option<StatusValue>> {
        let (ledger_name, branch) = split_ledger_id(ledger_id)?;
        let key = self.ns_key(&ledger_name, &branch);

        let data = match self.storage.read_bytes(&key).await {
            Ok(data) => data,
            Err(CoreError::NotFound(_)) => return Ok(None),
            Err(e) => {
                return Err(NameServiceError::storage(format!(
                    "Failed to read {key}: {e}"
                )))
            }
        };

        let file: NsFileV2 = serde_json::from_slice(&data)?;

        Ok(Some(file.to_status_value()))
    }
}

#[async_trait]
impl<S> StatusPublisher for StorageNameService<S>
where
    S: StorageRead + StorageWrite + StorageList + StorageCas + Debug + Send + Sync,
{
    async fn push_status(
        &self,
        ledger_id: &str,
        expected: Option<&StatusValue>,
        new: &StatusValue,
    ) -> Result<StatusCasResult> {
        let (ledger_name, branch) = split_ledger_id(ledger_id)?;
        let key = self.ns_key(&ledger_name, &branch);

        let expected = expected.cloned();
        let new = new.clone();

        let outcome = self
            .storage
            .compare_and_swap(&key, |current_bytes| {
                let Some(bytes) = current_bytes else {
                    return Ok(CasAction::Abort(StatusCasResult::Conflict { actual: None }));
                };

                let mut file: NsFileV2 = deserialize_json(bytes)?;

                let current = file.to_status_value();

                // Compare expected with current
                match &expected {
                    None => {
                        return Ok(CasAction::Abort(StatusCasResult::Conflict {
                            actual: Some(current),
                        }));
                    }
                    Some(exp) => {
                        if exp.v != current.v || exp.payload != current.payload {
                            return Ok(CasAction::Abort(StatusCasResult::Conflict {
                                actual: Some(current),
                            }));
                        }
                    }
                }

                // Monotonic guard: new.v > current.v
                if new.v <= current.v {
                    return Ok(CasAction::Abort(StatusCasResult::Conflict {
                        actual: Some(current),
                    }));
                }

                // Apply update
                file.status = new.payload.state.clone();
                file.status_v = Some(new.v);
                file.status_meta = if new.payload.extra.is_empty() {
                    None
                } else {
                    Some(new.payload.extra.clone())
                };

                let new_bytes = serialize_json(&file)?;
                Ok(CasAction::Write(new_bytes))
            })
            .await
            .map_err(|e| NameServiceError::storage(format!("Failed to update status: {e}")))?;

        match outcome {
            CasOutcome::Written => Ok(StatusCasResult::Updated),
            CasOutcome::Aborted(result) => Ok(result),
        }
    }
}

#[async_trait]
impl<S> ConfigLookup for StorageNameService<S>
where
    S: StorageRead + StorageWrite + StorageList + StorageCas + Debug + Send + Sync,
{
    async fn get_config(&self, ledger_id: &str) -> Result<Option<ConfigValue>> {
        let (ledger_name, branch) = split_ledger_id(ledger_id)?;
        let key = self.ns_key(&ledger_name, &branch);

        let data = match self.storage.read_bytes(&key).await {
            Ok(data) => data,
            Err(CoreError::NotFound(_)) => return Ok(None),
            Err(e) => {
                return Err(NameServiceError::storage(format!(
                    "Failed to read {key}: {e}"
                )))
            }
        };

        let file: NsFileV2 = serde_json::from_slice(&data)?;

        Ok(Some(file.to_config_value()))
    }
}

#[async_trait]
impl<S> ConfigPublisher for StorageNameService<S>
where
    S: StorageRead + StorageWrite + StorageList + StorageCas + Debug + Send + Sync,
{
    async fn push_config(
        &self,
        ledger_id: &str,
        expected: Option<&ConfigValue>,
        new: &ConfigValue,
    ) -> Result<ConfigCasResult> {
        let (ledger_name, branch) = split_ledger_id(ledger_id)?;
        let key = self.ns_key(&ledger_name, &branch);

        let expected = expected.cloned();
        let new = new.clone();

        let outcome = self
            .storage
            .compare_and_swap(&key, |current_bytes| {
                let Some(bytes) = current_bytes else {
                    return Ok(CasAction::Abort(ConfigCasResult::Conflict { actual: None }));
                };

                let mut file: NsFileV2 = deserialize_json(bytes)?;

                let current = file.to_config_value();

                // Compare expected with current
                match &expected {
                    None => {
                        return Ok(CasAction::Abort(ConfigCasResult::Conflict {
                            actual: Some(current),
                        }));
                    }
                    Some(exp) => {
                        if exp.v != current.v || exp.payload != current.payload {
                            return Ok(CasAction::Abort(ConfigCasResult::Conflict {
                                actual: Some(current),
                            }));
                        }
                    }
                }

                // Monotonic guard: new.v > current.v
                if new.v <= current.v {
                    return Ok(CasAction::Abort(ConfigCasResult::Conflict {
                        actual: Some(current),
                    }));
                }

                // Apply update
                file.config_v = Some(new.v);

                if let Some(ref payload) = new.payload {
                    file.default_context_cid = payload
                        .default_context
                        .as_ref()
                        .map(std::string::ToString::to_string);
                    file.config_cid = payload
                        .config_id
                        .as_ref()
                        .map(std::string::ToString::to_string);
                    file.config_meta = if payload.extra.is_empty() {
                        None
                    } else {
                        Some(payload.extra.clone())
                    };
                } else {
                    file.default_context_cid = None;
                    file.config_cid = None;
                    file.config_meta = None;
                }

                let new_bytes = serialize_json(&file)?;
                Ok(CasAction::Write(new_bytes))
            })
            .await
            .map_err(|e| NameServiceError::storage(format!("Failed to update config: {e}")))?;

        match outcome {
            CasOutcome::Written => Ok(ConfigCasResult::Updated),
            CasOutcome::Aborted(result) => Ok(result),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CasResult, ConfigPayload, RefPublisher, RefValue, StatusPayload};
    use fluree_db_core::StorageExtError;

    async fn publish_commit(ns: &impl RefPublisher, ledger_id: &str, t: i64, cid: &ContentId) {
        let new = RefValue {
            id: Some(cid.clone()),
            t,
        };
        match ns.fast_forward_commit(ledger_id, &new, 3).await.unwrap() {
            CasResult::Updated => {}
            CasResult::Conflict { actual } => {
                assert!(
                    actual.as_ref().map(|r| r.t).unwrap_or(0) >= t,
                    "unexpected commit publish conflict: {actual:?}"
                );
            }
        }
    }

    #[test]
    fn test_ns_key_with_prefix() {
        // Create a mock storage for testing key generation
        // We can't easily test the full StorageNameService without a real storage impl
        let prefix = "ledgers";
        let expected = format!("{prefix}/ns@v2/mydb/main.json");
        assert_eq!(
            expected,
            format!("{}/{}/{}/{}.json", prefix, NS_VERSION, "mydb", "main")
        );
    }

    #[test]
    fn test_ns_key_without_prefix() {
        let expected = format!("{}/{}/{}.json", NS_VERSION, "mydb", "main");
        assert_eq!(expected, "ns@v2/mydb/main.json");
    }

    #[test]
    fn test_index_key() {
        let expected = format!("{}/{}/{}.index.json", NS_VERSION, "mydb", "main");
        assert_eq!(expected, "ns@v2/mydb/main.index.json");
    }

    #[test]
    fn test_new_main_file() {
        let file = StorageNameService::<()>::new_main_file("mydb", "main", Some("cid-1"), 10);
        assert_eq!(file.id, "mydb:main");
        assert_eq!(file.t, 10);
        assert_eq!(file.status, "ready");
        assert_eq!(file.commit_cid, Some("cid-1".to_string()));
    }

    // =========================================================================
    // In-memory CAS storage for testing StorageNameService
    // =========================================================================

    use fluree_db_core::{ListResult, StorageExtResult};
    use std::collections::HashMap;
    use std::sync::RwLock;

    /// In-memory storage with atomic CAS for testing.
    #[derive(Debug)]
    struct MemoryCasStorage {
        data: RwLock<HashMap<String, Vec<u8>>>,
    }

    impl MemoryCasStorage {
        fn new() -> Self {
            Self {
                data: RwLock::new(HashMap::new()),
            }
        }
    }

    #[async_trait]
    impl fluree_db_core::StorageRead for MemoryCasStorage {
        async fn read_bytes(&self, address: &str) -> fluree_db_core::Result<Vec<u8>> {
            self.data
                .read()
                .unwrap()
                .get(address)
                .cloned()
                .ok_or_else(|| fluree_db_core::Error::not_found(address))
        }

        async fn exists(&self, address: &str) -> fluree_db_core::Result<bool> {
            Ok(self.data.read().unwrap().contains_key(address))
        }

        async fn list_prefix(&self, prefix: &str) -> fluree_db_core::Result<Vec<String>> {
            let data = self.data.read().unwrap();
            Ok(data
                .keys()
                .filter(|k| k.starts_with(prefix))
                .cloned()
                .collect())
        }
    }

    #[async_trait]
    impl fluree_db_core::StorageWrite for MemoryCasStorage {
        async fn write_bytes(&self, address: &str, bytes: &[u8]) -> fluree_db_core::Result<()> {
            self.data
                .write()
                .unwrap()
                .insert(address.to_string(), bytes.to_vec());
            Ok(())
        }

        async fn delete(&self, address: &str) -> fluree_db_core::Result<()> {
            self.data.write().unwrap().remove(address);
            Ok(())
        }
    }

    #[async_trait]
    impl fluree_db_core::ContentAddressedWrite for MemoryCasStorage {
        async fn content_write_bytes_with_hash(
            &self,
            _kind: fluree_db_core::ContentKind,
            _ledger_id: &str,
            content_hash_hex: &str,
            bytes: &[u8],
        ) -> fluree_db_core::Result<fluree_db_core::ContentWriteResult> {
            fluree_db_core::StorageWrite::write_bytes(self, content_hash_hex, bytes).await?;
            Ok(fluree_db_core::ContentWriteResult {
                address: content_hash_hex.to_string(),
                content_hash: content_hash_hex.to_string(),
                size_bytes: bytes.len(),
            })
        }
    }

    #[async_trait]
    impl StorageList for MemoryCasStorage {
        async fn list_prefix(&self, prefix: &str) -> StorageExtResult<Vec<String>> {
            let data = self.data.read().unwrap();
            Ok(data
                .keys()
                .filter(|k| k.starts_with(prefix))
                .cloned()
                .collect())
        }

        async fn list_prefix_paginated(
            &self,
            prefix: &str,
            _continuation_token: Option<String>,
            max_keys: usize,
        ) -> StorageExtResult<ListResult> {
            let data = self.data.read().unwrap();
            let keys: Vec<String> = data
                .keys()
                .filter(|k| k.starts_with(prefix))
                .take(max_keys)
                .cloned()
                .collect();
            Ok(ListResult {
                keys,
                continuation_token: None,
                is_truncated: false,
            })
        }
    }

    #[async_trait]
    impl StorageCas for MemoryCasStorage {
        async fn insert(&self, address: &str, bytes: &[u8]) -> StorageExtResult<bool> {
            let mut data = self.data.write().unwrap();
            if data.contains_key(address) {
                Ok(false)
            } else {
                data.insert(address.to_string(), bytes.to_vec());
                Ok(true)
            }
        }

        async fn compare_and_swap<T, F>(
            &self,
            address: &str,
            f: F,
        ) -> StorageExtResult<CasOutcome<T>>
        where
            F: Fn(Option<&[u8]>) -> std::result::Result<CasAction<T>, StorageExtError>
                + Send
                + Sync,
            T: Send,
        {
            let mut data = self.data.write().unwrap();
            let current = data.get(address).map(std::vec::Vec::as_slice);
            match f(current)? {
                CasAction::Write(new_bytes) => {
                    data.insert(address.to_string(), new_bytes);
                    Ok(CasOutcome::Written)
                }
                CasAction::Abort(t) => Ok(CasOutcome::Aborted(t)),
            }
        }
    }

    fn make_storage_ns() -> StorageNameService<MemoryCasStorage> {
        StorageNameService::new(MemoryCasStorage::new(), "test")
    }

    /// Wrapper that simulates a concurrent modification on the first
    /// `compare_and_swap` call. The closure runs but writes are silently
    /// discarded on the first attempt, forcing a retry. This tests that
    /// callers handle retries correctly.
    #[derive(Debug)]
    struct FlakyCasStorage {
        inner: MemoryCasStorage,
        fail_first_swap: std::sync::atomic::AtomicBool,
    }

    impl FlakyCasStorage {
        fn new() -> Self {
            Self {
                inner: MemoryCasStorage::new(),
                fail_first_swap: std::sync::atomic::AtomicBool::new(true),
            }
        }
    }

    #[async_trait]
    impl fluree_db_core::StorageRead for FlakyCasStorage {
        async fn read_bytes(&self, address: &str) -> fluree_db_core::Result<Vec<u8>> {
            fluree_db_core::StorageRead::read_bytes(&self.inner, address).await
        }

        async fn exists(&self, address: &str) -> fluree_db_core::Result<bool> {
            fluree_db_core::StorageRead::exists(&self.inner, address).await
        }

        async fn list_prefix(&self, prefix: &str) -> fluree_db_core::Result<Vec<String>> {
            fluree_db_core::StorageRead::list_prefix(&self.inner, prefix).await
        }
    }

    #[async_trait]
    impl fluree_db_core::StorageWrite for FlakyCasStorage {
        async fn write_bytes(&self, address: &str, bytes: &[u8]) -> fluree_db_core::Result<()> {
            fluree_db_core::StorageWrite::write_bytes(&self.inner, address, bytes).await
        }

        async fn delete(&self, address: &str) -> fluree_db_core::Result<()> {
            fluree_db_core::StorageWrite::delete(&self.inner, address).await
        }
    }

    #[async_trait]
    impl fluree_db_core::ContentAddressedWrite for FlakyCasStorage {
        async fn content_write_bytes_with_hash(
            &self,
            kind: fluree_db_core::ContentKind,
            ledger_id: &str,
            content_hash_hex: &str,
            bytes: &[u8],
        ) -> fluree_db_core::Result<fluree_db_core::ContentWriteResult> {
            fluree_db_core::ContentAddressedWrite::content_write_bytes_with_hash(
                &self.inner,
                kind,
                ledger_id,
                content_hash_hex,
                bytes,
            )
            .await
        }
    }

    #[async_trait]
    impl StorageList for FlakyCasStorage {
        async fn list_prefix(&self, prefix: &str) -> StorageExtResult<Vec<String>> {
            StorageList::list_prefix(&self.inner, prefix).await
        }

        async fn list_prefix_paginated(
            &self,
            prefix: &str,
            continuation_token: Option<String>,
            max_keys: usize,
        ) -> StorageExtResult<ListResult> {
            self.inner
                .list_prefix_paginated(prefix, continuation_token, max_keys)
                .await
        }
    }

    #[async_trait]
    impl StorageCas for FlakyCasStorage {
        async fn insert(&self, address: &str, bytes: &[u8]) -> StorageExtResult<bool> {
            self.inner.insert(address, bytes).await
        }

        async fn compare_and_swap<T, F>(
            &self,
            address: &str,
            f: F,
        ) -> StorageExtResult<CasOutcome<T>>
        where
            F: Fn(Option<&[u8]>) -> std::result::Result<CasAction<T>, StorageExtError>
                + Send
                + Sync,
            T: Send,
        {
            // On the first call, run the closure but then call it again to
            // simulate a concurrent modification that invalidated the first read.
            if self
                .fail_first_swap
                .swap(false, std::sync::atomic::Ordering::SeqCst)
            {
                let data = self.inner.data.read().unwrap();
                let current = data.get(address).map(std::vec::Vec::as_slice);
                // Run closure but discard result (simulating a race)
                let _ = f(current);
                drop(data);
            }
            // Second attempt succeeds normally
            self.inner.compare_and_swap(address, f).await
        }
    }

    fn make_flaky_storage_ns() -> StorageNameService<FlakyCasStorage> {
        StorageNameService::new(FlakyCasStorage::new(), "test")
    }

    /// Create a dummy ContentId for tests (hashes the label as Commit kind).
    fn dummy_cid(label: &str) -> ContentId {
        ContentId::new(fluree_db_core::ContentKind::Commit, label.as_bytes())
    }

    // =========================================================================
    // RefPublisher tests for StorageNameService
    // =========================================================================

    #[tokio::test]
    async fn test_storage_ref_get_ref_unknown_alias() {
        let ns = make_storage_ns();
        let result = ns
            .get_ref("nonexistent:main", RefKind::CommitHead)
            .await
            .unwrap();
        assert_eq!(result, None);
    }

    // =========================================================================
    // Status/Config retry behavior (ETag mismatch)
    // =========================================================================

    #[tokio::test]
    async fn test_storage_status_push_retries_on_etag_mismatch() {
        let ns = make_flaky_storage_ns();
        publish_commit(&ns, "mydb:main", 1, &dummy_cid("commit-1")).await;

        let expected = ns.get_status("mydb:main").await.unwrap().unwrap();
        let new_status = StatusValue::new(2, StatusPayload::new("indexing"));

        let result = ns
            .push_status("mydb:main", Some(&expected), &new_status)
            .await
            .unwrap();
        assert_eq!(result, StatusCasResult::Updated);

        let current = ns.get_status("mydb:main").await.unwrap().unwrap();
        assert_eq!(current.v, 2);
        assert_eq!(current.payload.state, "indexing");
    }

    #[tokio::test]
    async fn test_storage_config_push_retries_on_etag_mismatch() {
        let ns = make_flaky_storage_ns();
        publish_commit(&ns, "mydb:main", 1, &dummy_cid("commit-1")).await;

        let expected = ns.get_config("mydb:main").await.unwrap().unwrap();
        assert_eq!(expected.v, 0);
        assert!(expected.payload.is_none());

        let ctx_cid = ContentId::new(fluree_db_core::ContentKind::LedgerConfig, b"ctx-1");
        let new_cfg = ConfigValue::new(
            1,
            Some(ConfigPayload::with_default_context(ctx_cid.clone())),
        );

        let result = ns
            .push_config("mydb:main", Some(&expected), &new_cfg)
            .await
            .unwrap();
        assert_eq!(result, ConfigCasResult::Updated);

        let current = ns.get_config("mydb:main").await.unwrap().unwrap();
        assert_eq!(current.v, 1);
        assert_eq!(current.payload.unwrap().default_context, Some(ctx_cid));
    }

    #[tokio::test]
    async fn test_storage_ref_get_ref_after_publish() {
        let ns = make_storage_ns();
        publish_commit(&ns, "mydb:main", 5, &dummy_cid("commit-1")).await;

        let commit = ns
            .get_ref("mydb:main", RefKind::CommitHead)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(commit.id, Some(dummy_cid("commit-1")));
        assert_eq!(commit.t, 5);
    }

    #[tokio::test]
    async fn test_storage_ref_cas_create_new() {
        let ns = make_storage_ns();
        let new_ref = RefValue {
            id: Some(dummy_cid("commit-1")),
            t: 1,
        };

        let result = ns
            .compare_and_set_ref("mydb:main", RefKind::CommitHead, None, &new_ref)
            .await
            .unwrap();
        assert_eq!(result, CasResult::Updated);

        let current = ns
            .get_ref("mydb:main", RefKind::CommitHead)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(current.id, Some(dummy_cid("commit-1")));
        assert_eq!(current.t, 1);
    }

    #[tokio::test]
    async fn test_storage_ref_cas_conflict_already_exists() {
        let ns = make_storage_ns();
        publish_commit(&ns, "mydb:main", 1, &dummy_cid("commit-1")).await;

        let new_ref = RefValue {
            id: Some(dummy_cid("commit-2")),
            t: 2,
        };
        let result = ns
            .compare_and_set_ref("mydb:main", RefKind::CommitHead, None, &new_ref)
            .await
            .unwrap();
        match result {
            CasResult::Conflict { actual } => {
                let a = actual.unwrap();
                assert_eq!(a.id, Some(dummy_cid("commit-1")));
            }
            _ => panic!("expected conflict"),
        }
    }

    #[tokio::test]
    async fn test_storage_ref_cas_id_mismatch() {
        let ns = make_storage_ns();
        publish_commit(&ns, "mydb:main", 1, &dummy_cid("commit-1")).await;

        let expected = RefValue {
            id: Some(dummy_cid("wrong")),
            t: 1,
        };
        let new_ref = RefValue {
            id: Some(dummy_cid("commit-2")),
            t: 2,
        };
        let result = ns
            .compare_and_set_ref("mydb:main", RefKind::CommitHead, Some(&expected), &new_ref)
            .await
            .unwrap();
        match result {
            CasResult::Conflict { .. } => {}
            _ => panic!("expected conflict"),
        }
    }

    #[tokio::test]
    async fn test_storage_ref_cas_success() {
        let ns = make_storage_ns();
        publish_commit(&ns, "mydb:main", 1, &dummy_cid("commit-1")).await;

        let expected = RefValue {
            id: Some(dummy_cid("commit-1")),
            t: 1,
        };
        let new_ref = RefValue {
            id: Some(dummy_cid("commit-2")),
            t: 2,
        };
        let result = ns
            .compare_and_set_ref("mydb:main", RefKind::CommitHead, Some(&expected), &new_ref)
            .await
            .unwrap();
        assert_eq!(result, CasResult::Updated);

        let current = ns
            .get_ref("mydb:main", RefKind::CommitHead)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(current.id, Some(dummy_cid("commit-2")));
        assert_eq!(current.t, 2);
    }

    #[tokio::test]
    async fn test_storage_ref_cas_commit_strict_monotonic() {
        let ns = make_storage_ns();
        publish_commit(&ns, "mydb:main", 5, &dummy_cid("commit-1")).await;

        let expected = RefValue {
            id: Some(dummy_cid("commit-1")),
            t: 5,
        };
        // Same t -> conflict (strict)
        let new_ref = RefValue {
            id: Some(dummy_cid("commit-2")),
            t: 5,
        };
        let result = ns
            .compare_and_set_ref("mydb:main", RefKind::CommitHead, Some(&expected), &new_ref)
            .await
            .unwrap();
        match result {
            CasResult::Conflict { .. } => {}
            _ => panic!("expected conflict for same t on CommitHead"),
        }
    }

    #[tokio::test]
    async fn test_storage_ref_cas_index_allows_equal_t() {
        let ns = make_storage_ns();
        ns.publish_index("mydb:main", 5, &dummy_cid("index-1"))
            .await
            .unwrap();

        let expected = RefValue {
            id: Some(dummy_cid("index-1")),
            t: 5,
        };
        let new_ref = RefValue {
            id: Some(dummy_cid("index-2")),
            t: 5,
        };
        let result = ns
            .compare_and_set_ref("mydb:main", RefKind::IndexHead, Some(&expected), &new_ref)
            .await
            .unwrap();
        assert_eq!(result, CasResult::Updated);
    }

    #[tokio::test]
    async fn test_storage_ref_fast_forward_commit() {
        let ns = make_storage_ns();
        publish_commit(&ns, "mydb:main", 1, &dummy_cid("commit-1")).await;

        let new_ref = RefValue {
            id: Some(dummy_cid("commit-5")),
            t: 5,
        };
        let result = ns
            .fast_forward_commit("mydb:main", &new_ref, 3)
            .await
            .unwrap();
        assert_eq!(result, CasResult::Updated);

        let current = ns
            .get_ref("mydb:main", RefKind::CommitHead)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(current.t, 5);
    }

    #[tokio::test]
    async fn test_storage_ref_fast_forward_rejected_stale() {
        let ns = make_storage_ns();
        publish_commit(&ns, "mydb:main", 10, &dummy_cid("commit-1")).await;

        let new_ref = RefValue {
            id: Some(dummy_cid("old")),
            t: 5,
        };
        let result = ns
            .fast_forward_commit("mydb:main", &new_ref, 3)
            .await
            .unwrap();
        match result {
            CasResult::Conflict { actual } => {
                assert_eq!(actual.unwrap().t, 10);
            }
            _ => panic!("expected conflict"),
        }
    }

    #[tokio::test]
    async fn test_storage_ref_get_index_after_publish() {
        let ns = make_storage_ns();
        publish_commit(&ns, "mydb:main", 5, &dummy_cid("commit-1")).await;
        ns.publish_index("mydb:main", 3, &dummy_cid("index-1"))
            .await
            .unwrap();

        let index = ns
            .get_ref("mydb:main", RefKind::IndexHead)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(index.id, Some(dummy_cid("index-1")));
        assert_eq!(index.t, 3);
    }

    #[tokio::test]
    async fn test_storage_ref_expected_some_but_missing() {
        let ns = make_storage_ns();
        let expected = RefValue {
            id: Some(dummy_cid("commit-1")),
            t: 1,
        };
        let new_ref = RefValue {
            id: Some(dummy_cid("commit-2")),
            t: 2,
        };
        let result = ns
            .compare_and_set_ref("mydb:main", RefKind::CommitHead, Some(&expected), &new_ref)
            .await
            .unwrap();
        match result {
            CasResult::Conflict { actual } => {
                assert_eq!(actual, None);
            }
            _ => panic!("expected conflict when ref doesn't exist"),
        }
    }

    // =========================================================================
    // StatusPublisher tests
    // =========================================================================

    #[tokio::test]
    async fn test_storage_retract_bumps_status_v() {
        use crate::StatusLookup;

        let ns = make_storage_ns();
        ns.publish_ledger_init("mydb:main").await.unwrap();

        // Get initial status (v=1, state="ready")
        let initial = ns.get_status("mydb:main").await.unwrap().unwrap();
        assert_eq!(initial.v, 1);
        assert_eq!(initial.payload.state, "ready");

        // Retract the ledger
        ns.retract("mydb:main").await.unwrap();

        // Verify status_v was incremented and state changed to "retracted"
        let after_retract = ns.get_status("mydb:main").await.unwrap().unwrap();
        assert_eq!(
            after_retract.v, 2,
            "status_v should be incremented on retract"
        );
        assert_eq!(after_retract.payload.state, "retracted");
    }
}
