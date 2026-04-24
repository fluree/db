//! File-based nameservice implementation using ns@v2 format
//!
//! This implementation stores records as JSON files following the ns@v2 format:
//! - `data/ns@v2/{ledger-name}/{branch}.json` - Main record (commit info)
//! - `data/ns@v2/{ledger-name}/{branch}.index.json` - Index record (separate for indexer)
//!
//! The separation of commit and index files allows transactors and indexers
//! to update independently without contention.
//!
//! # Concurrency
//!
//! This implementation uses `FileStorage::compare_and_swap` for atomic
//! read-modify-write operations. `FileStorage` uses `fs2` file locking
//! internally, providing mutual exclusion across processes on the same host.
//!
//! For single-writer scenarios (one transactor per ledger, one indexer per ledger),
//! this implementation is safe. The separation of commit and index files enables
//! a transactor and indexer to operate independently on the same ledger.
//!
//! For multi-writer scenarios across machines (or filesystems where OS locks are not
//! reliable, e.g. some networked FS), use a nameservice backend with CAS semantics:
//! - S3 with ETag conditional writes
//! - DynamoDB with conditional expressions
//! - A database with transactions

use crate::ns_format::{
    ns_context, BranchPointRef, IndexRef, LedgerRef, NsFileV2, NsIndexFileV2, NS_VERSION,
};
use crate::{
    check_cas_expectation, deserialize_json, parse_default_context_value, ref_values_match,
    serialize_json, AdminPublisher, CasResult, ConfigCasResult, ConfigLookup, ConfigPublisher,
    ConfigValue, GraphSourceLookup, GraphSourcePublisher, GraphSourceRecord, GraphSourceType,
    NameService, NameServiceError, NsLookupResult, NsRecord, Publisher, RefKind, RefLookup,
    RefPublisher, RefValue, Result, StatusCasResult, StatusLookup, StatusPublisher, StatusValue,
};
use async_trait::async_trait;
use fluree_db_core::ledger_id::{format_ledger_id, normalize_ledger_id, split_ledger_id};
use fluree_db_core::{CasAction, CasOutcome, ContentId, FileStorage, StorageCas};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::path::{Path, PathBuf};

/// File-based nameservice using ns@v2 format
#[derive(Clone)]
pub struct FileNameService {
    /// File storage for atomic read-modify-write operations
    storage: FileStorage,
}

impl Debug for FileNameService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileNameService")
            .field("storage", &self.storage)
            .finish()
    }
}

/// JSON structure for graph source ns@v2 config record file
///
/// Graph source records use the same ns@v2 path pattern but have different fields:
/// - `@type` includes "f:IndexSource" (or "f:MappedSource") and a source-specific type
/// - `f:graphSourceConfig` contains the graph source configuration as a JSON string
/// - `f:graphSourceDependencies` lists dependent ledger IDs
#[derive(Debug, Serialize, Deserialize)]
struct GraphSourceNsFileV2 {
    /// Context uses f: namespace
    #[serde(rename = "@context")]
    context: serde_json::Value,

    #[serde(rename = "@id")]
    id: String,

    /// Type array includes kind type and source-specific type
    #[serde(rename = "@type")]
    record_type: Vec<String>,

    /// Base name of the graph source
    #[serde(rename = "f:name")]
    name: String,

    /// Branch name
    #[serde(rename = "f:branch")]
    branch: String,

    /// Graph source configuration as JSON string
    #[serde(rename = "f:graphSourceConfig")]
    config: ConfigRef,

    /// Dependent ledger IDs
    #[serde(rename = "f:graphSourceDependencies")]
    dependencies: Vec<String>,

    /// Status (ready/retracted)
    #[serde(rename = "f:status")]
    status: String,
}

/// Config stored as JSON string with @value wrapper
#[derive(Debug, Serialize, Deserialize)]
struct ConfigRef {
    #[serde(rename = "@value")]
    value: String,
}

/// Reference to a graph source index CID
#[derive(Debug, Serialize, Deserialize)]
struct GraphSourceIndexRef {
    /// Content identifier string for the graph source index snapshot
    #[serde(rename = "f:graphSourceIndexCid")]
    cid: String,
}

/// JSON structure for graph source index record (separate from config)
///
/// Stored at `ns@v2/{graph-source-name}/{branch}.index.json` to avoid contention
/// between config updates and index updates. Uses monotonic update rule:
/// only write if new index_t > existing index_t.
#[derive(Debug, Serialize, Deserialize)]
struct GraphSourceIndexFileV2WithT {
    #[serde(rename = "@context")]
    context: serde_json::Value,

    #[serde(rename = "@id")]
    id: String,

    #[serde(rename = "f:graphSourceIndex")]
    index: GraphSourceIndexRef,

    #[serde(rename = "f:graphSourceIndexT")]
    index_t: i64,
}

impl FileNameService {
    /// Create a new file-based nameservice
    pub fn new(base_path: impl Into<PathBuf>) -> Self {
        Self {
            storage: FileStorage::new(base_path),
        }
    }

    /// Build a `fluree:file://` address for the main ns record.
    fn ns_address(ledger_name: &str, branch: &str) -> String {
        format!("fluree:file://{NS_VERSION}/{ledger_name}/{branch}.json")
    }

    /// Build a `fluree:file://` address for the index-only ns record.
    fn index_address(ledger_name: &str, branch: &str) -> String {
        format!("fluree:file://{NS_VERSION}/{ledger_name}/{branch}.index.json")
    }

    /// Get the filesystem path for the main ns record (for directory walking).
    fn ns_path(&self, ledger_name: &str, branch: &str) -> PathBuf {
        self.storage
            .base_path()
            .join(NS_VERSION)
            .join(ledger_name)
            .join(format!("{branch}.json"))
    }

    /// Get the filesystem path for the index ns record (for directory walking).
    fn index_path(&self, ledger_name: &str, branch: &str) -> PathBuf {
        self.storage
            .base_path()
            .join(NS_VERSION)
            .join(ledger_name)
            .join(format!("{branch}.index.json"))
    }

    /// Recursively walk `root` and return the relative paths of main ns record
    /// `.json` files, skipping index, snapshot, lock, and tmp files.
    ///
    /// Returns an empty vec if `root` does not exist.
    async fn walk_ns_json_files(root: &Path) -> Result<Vec<PathBuf>> {
        if !root.exists() {
            return Ok(vec![]);
        }

        let mut paths = Vec::new();
        let mut stack = vec![root.to_path_buf()];

        while let Some(current_dir) = stack.pop() {
            let mut dir_entries = tokio::fs::read_dir(&current_dir).await.map_err(|e| {
                NameServiceError::storage(format!("Failed to read directory {current_dir:?}: {e}"))
            })?;

            while let Some(entry) = dir_entries.next_entry().await.map_err(|e| {
                NameServiceError::storage(format!("Failed to read directory entry: {e}"))
            })? {
                let path = entry.path();

                if path.is_dir() {
                    stack.push(path);
                    continue;
                }

                if !path.is_file() {
                    continue;
                }

                let file_name = entry.file_name().to_string_lossy().to_string();

                if file_name.ends_with(".index.json")
                    || file_name.ends_with(".snapshots.json")
                    || file_name.ends_with(".lock")
                    || file_name.ends_with(".tmp")
                    || !file_name.ends_with(".json")
                {
                    continue;
                }

                if let Ok(relative) = path.strip_prefix(root) {
                    paths.push(relative.to_path_buf());
                }
            }
        }

        Ok(paths)
    }

    /// Read and deserialize JSON from a `fluree:file://` address.
    ///
    /// Returns `None` if the address does not exist (NotFound error from storage).
    async fn read_json_from_address<T: for<'de> Deserialize<'de>>(
        &self,
        address: &str,
    ) -> Result<Option<T>> {
        use fluree_db_core::StorageRead;
        match self.storage.read_bytes(address).await {
            Ok(bytes) => {
                let parsed = serde_json::from_slice(&bytes)?;
                Ok(Some(parsed))
            }
            Err(fluree_db_core::Error::NotFound(_)) => Ok(None),
            Err(e) => Err(NameServiceError::from(e)),
        }
    }

    /// Load and merge main record with index file
    async fn load_record(&self, ledger_name: &str, branch: &str) -> Result<Option<NsRecord>> {
        let main_address = Self::ns_address(ledger_name, branch);
        let index_address = Self::index_address(ledger_name, branch);

        // Read main record
        let main_file: Option<NsFileV2> = self.read_json_from_address(&main_address).await?;

        let Some(main) = main_file else {
            return Ok(None);
        };

        // Read index file (if exists)
        let index_file: Option<NsIndexFileV2> = self.read_json_from_address(&index_address).await?;

        // Convert to NsRecord
        let mut record = NsRecord {
            ledger_id: format_ledger_id(ledger_name, branch),
            name: main.ledger.id.clone(),
            branch: main.branch,
            commit_head_id: main
                .commit_cid
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
            config_id: main
                .config_cid
                .as_deref()
                .and_then(|s| s.parse::<ContentId>().ok()),
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

    /// Check if a record file is a graph source record (based on @type).
    /// Matches "f:IndexSource"/"f:MappedSource" compact prefixes and full IRIs.
    async fn is_graph_source_record(&self, name: &str, branch: &str) -> Result<bool> {
        let main_path = self.ns_path(name, branch);
        if !main_path.exists() {
            return Ok(false);
        }

        let content = tokio::fs::read_to_string(&main_path)
            .await
            .map_err(|e| NameServiceError::storage(format!("Failed to read {main_path:?}: {e}")))?;

        // Parse just enough to check @type
        let parsed: serde_json::Value = serde_json::from_str(&content)?;
        Ok(Self::is_graph_source_from_json(&parsed))
    }

    /// Check if parsed JSON represents a graph source record (exact match).
    /// Matches `"f:"` compact prefixes and full IRIs.
    fn is_graph_source_from_json(parsed: &serde_json::Value) -> bool {
        if let Some(types) = parsed.get("@type").and_then(|t| t.as_array()) {
            for t in types {
                if let Some(s) = t.as_str() {
                    // Match on kind types (f: compact prefix and full IRIs)
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

    /// Load a graph source config record and merge with index file
    async fn load_graph_source_record(
        &self,
        name: &str,
        branch: &str,
    ) -> Result<Option<GraphSourceRecord>> {
        let main_address = Self::ns_address(name, branch);
        let index_address = Self::index_address(name, branch);

        // Read main record
        let main_file: Option<GraphSourceNsFileV2> =
            self.read_json_from_address(&main_address).await?;

        let Some(main) = main_file else {
            return Ok(None);
        };

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

        // Read and merge graph source index file (if exists)
        let index_file: Option<GraphSourceIndexFileV2WithT> =
            self.read_json_from_address(&index_address).await?;
        if let Some(index_data) = index_file {
            if index_data.index_t > record.index_t {
                record.index_id = index_data.index.cid.parse::<ContentId>().ok();
                record.index_t = index_data.index_t;
            }
        }

        Ok(Some(record))
    }
}

#[async_trait]
impl NameService for FileNameService {
    async fn lookup(&self, ledger_id: &str) -> Result<Option<NsRecord>> {
        let (ledger_name, branch) = split_ledger_id(ledger_id)?;
        self.load_record(&ledger_name, &branch).await
    }

    async fn list_branches(&self, ledger_name: &str) -> Result<Vec<NsRecord>> {
        let ledger_dir = self.storage.base_path().join(NS_VERSION).join(ledger_name);
        let mut records = Vec::new();

        for relative in Self::walk_ns_json_files(&ledger_dir).await? {
            let branch = relative
                .to_string_lossy()
                .trim_end_matches(".json")
                .to_string();

            if self.is_graph_source_record(ledger_name, &branch).await? {
                continue;
            }

            if let Ok(Some(record)) = self.load_record(ledger_name, &branch).await {
                if !record.retracted {
                    records.push(record);
                }
            }
        }

        Ok(records)
    }

    async fn all_records(&self) -> Result<Vec<NsRecord>> {
        let ns_dir = self.storage.base_path().join(NS_VERSION);
        let mut records = Vec::new();

        for relative in Self::walk_ns_json_files(&ns_dir).await? {
            let file_stem = relative
                .file_stem()
                .map(|s| s.to_string_lossy().to_string())
                .unwrap_or_default();
            let parent = relative
                .parent()
                .map(|p| p.to_string_lossy().to_string())
                .unwrap_or_default();
            if parent.is_empty() {
                continue;
            }

            if self.is_graph_source_record(&parent, &file_stem).await? {
                continue;
            }

            if let Ok(Some(record)) = self.load_record(&parent, &file_stem).await {
                records.push(record);
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
        let address = Self::ns_address(ledger_name, new_branch);
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
        let bytes = serde_json::to_vec_pretty(&file)?;

        let created = self.storage.insert(&address, &bytes).await?;
        if !created {
            return Err(NameServiceError::ledger_already_exists(&normalized_id));
        }

        // Increment source branch's child count
        let source_address = Self::ns_address(ledger_name, source_branch);
        let outcome = self
            .storage
            .compare_and_swap(&source_address, |bytes| {
                let Some(data) = bytes else {
                    return Ok(CasAction::Abort(()));
                };
                let mut file: NsFileV2 = deserialize_json(data)?;
                file.branches += 1;
                let new_bytes = serialize_json(&file)?;
                Ok(CasAction::Write(new_bytes))
            })
            .await?;

        if matches!(outcome, CasOutcome::Aborted(())) {
            // Source branch doesn't exist; clean up the file we just created
            let created_path = self.ns_path(ledger_name, new_branch);
            let _ = tokio::fs::remove_file(&created_path).await;
            return Err(NameServiceError::not_found(format!(
                "source branch {ledger_name}:{source_branch}"
            )));
        }

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

        // Remove the NS files (purge)
        let main_path = self.ns_path(&ledger_name, &branch);
        let _ = tokio::fs::remove_file(&main_path).await;
        let idx_path = self.index_path(&ledger_name, &branch);
        let _ = tokio::fs::remove_file(&idx_path).await;

        // Decrement parent's child count if this branch had a parent
        match parent_source {
            Some(source) => {
                let parent_address = Self::ns_address(&ledger_name, &source);
                let outcome = self
                    .storage
                    .compare_and_swap(&parent_address, |bytes| {
                        let Some(data) = bytes else {
                            return Ok(CasAction::Abort(()));
                        };
                        let mut file: NsFileV2 = deserialize_json(data)?;
                        file.branches = file.branches.saturating_sub(1);
                        let new_bytes = serialize_json(&file)?;
                        Ok(CasAction::Write(new_bytes))
                    })
                    .await?;

                if matches!(outcome, CasOutcome::Aborted(())) {
                    // Parent was already deleted — nothing to decrement
                    return Ok(None);
                }

                // Re-read the parent to get the updated count
                let parent_record = self.load_record(&ledger_name, &source).await?;
                Ok(parent_record.map(|r| r.branches))
            }
            None => Ok(None),
        }
    }

    async fn reset_head(&self, ledger_id: &str, snapshot: crate::NsRecordSnapshot) -> Result<()> {
        let (ledger_name, branch) = split_ledger_id(ledger_id)?;
        let address = Self::ns_address(&ledger_name, &branch);

        let outcome = self
            .storage
            .compare_and_swap(&address, |bytes| {
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
impl Publisher for FileNameService {
    async fn publish_ledger_init(&self, ledger_id: &str) -> Result<()> {
        let (ledger_name, branch) = split_ledger_id(ledger_id)?;
        let address = Self::ns_address(&ledger_name, &branch);
        let normalized_address = format_ledger_id(&ledger_name, &branch);

        // Create a fresh record with no commits
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
            // v2 extension fields - initialize with defaults
            status_v: Some(1), // Initial status
            status_meta: None,
            config_v: Some(0), // Unborn config
            config_meta: None,
            source_branch: None,
            branch_point: None,
            branches: 0,
        };
        let bytes = serde_json::to_vec_pretty(&file)?;

        // Atomic create-if-absent: returns false if file already exists.
        let created = self.storage.insert(&address, &bytes).await?;
        if !created {
            return Err(NameServiceError::ledger_already_exists(normalized_address));
        }

        Ok(())
    }

    async fn publish_commit(
        &self,
        ledger_id: &str,
        commit_t: i64,
        commit_id: &ContentId,
    ) -> Result<()> {
        let (ledger_name, branch) = split_ledger_id(ledger_id)?;
        let address = Self::ns_address(&ledger_name, &branch);
        let ledger_name_c = ledger_name.clone();
        let branch_c = branch.clone();
        let cid_str = commit_id.to_string();

        self.storage
            .compare_and_swap(&address, |bytes| {
                let cid_val = Some(cid_str.clone());

                match bytes {
                    Some(data) => {
                        let mut file: NsFileV2 = deserialize_json(data)?;
                        // Strictly monotonic update
                        if commit_t > file.t {
                            file.commit_cid = cid_val;
                            file.t = commit_t;
                            let new_bytes = serialize_json(&file)?;
                            Ok(CasAction::Write(new_bytes))
                        } else {
                            Ok(CasAction::Abort(()))
                        }
                    }
                    None => {
                        // Create new record (always write)
                        let file = NsFileV2 {
                            context: ns_context(),
                            id: format_ledger_id(&ledger_name_c, &branch_c),
                            record_type: vec!["f:LedgerSource".to_string()],
                            ledger: LedgerRef {
                                id: ledger_name_c.clone(),
                            },
                            branch: branch_c.clone(),
                            commit_cid: cid_val,
                            config_cid: None,
                            t: commit_t,
                            index: None,
                            status: "ready".to_string(),
                            default_context_cid: None,
                            // v2 extension fields - initialize with defaults
                            status_v: Some(1),
                            status_meta: None,
                            config_v: Some(0),
                            config_meta: None,
                            source_branch: None,
                            branch_point: None,
                            branches: 0,
                        };
                        let new_bytes = serialize_json(&file)?;
                        Ok(CasAction::Write(new_bytes))
                    }
                }
            })
            .await?;

        Ok(())
    }

    async fn publish_index(
        &self,
        ledger_id: &str,
        index_t: i64,
        index_id: &ContentId,
    ) -> Result<()> {
        let (ledger_name, branch) = split_ledger_id(ledger_id)?;
        let address = Self::index_address(&ledger_name, &branch);
        let cid_str = index_id.to_string();

        self.storage
            .compare_and_swap(&address, |bytes| {
                if let Some(data) = bytes {
                    let existing: NsIndexFileV2 = deserialize_json(data)?;
                    if index_t <= existing.index.t {
                        return Ok(CasAction::Abort(()));
                    }
                }

                let file = NsIndexFileV2 {
                    context: ns_context(),
                    index: IndexRef {
                        cid: Some(cid_str.clone()),
                        t: index_t,
                    },
                };
                let new_bytes = serialize_json(&file)?;
                Ok(CasAction::Write(new_bytes))
            })
            .await?;

        Ok(())
    }

    async fn retract(&self, ledger_id: &str) -> Result<()> {
        let (ledger_name, branch) = split_ledger_id(ledger_id)?;
        let address = Self::ns_address(&ledger_name, &branch);

        self.storage
            .compare_and_swap(&address, |bytes| {
                let Some(data) = bytes else {
                    return Ok(CasAction::Abort(()));
                };
                let mut file: NsFileV2 = deserialize_json(data)?;
                if file.status == "retracted" {
                    return Ok(CasAction::Abort(()));
                }
                file.status = "retracted".to_string();
                // Advance status_v when retracting
                let current_v = file.status_v.unwrap_or(1);
                file.status_v = Some(current_v + 1);
                let new_bytes = serialize_json(&file)?;
                Ok(CasAction::Write(new_bytes))
            })
            .await?;

        Ok(())
    }

    async fn purge(&self, ledger_id: &str) -> Result<()> {
        // First retract (updates status, fires event)
        self.retract(ledger_id).await?;
        // Then remove the NS file so the alias can be reused
        let (ledger_name, branch) = split_ledger_id(ledger_id)?;
        let main_path = self.ns_path(&ledger_name, &branch);
        let _ = tokio::fs::remove_file(&main_path).await;
        // Also remove the index sidecar if present
        let idx_path = self.index_path(&ledger_name, &branch);
        let _ = tokio::fs::remove_file(&idx_path).await;
        Ok(())
    }

    fn publishing_ledger_id(&self, ledger_id: &str) -> Option<String> {
        // File nameservice returns the normalized ledger ID for publishing
        Some(normalize_ledger_id(ledger_id).unwrap_or_else(|_| ledger_id.to_string()))
    }
}

#[async_trait]
impl AdminPublisher for FileNameService {
    async fn publish_index_allow_equal(
        &self,
        ledger_id: &str,
        index_t: i64,
        index_id: &ContentId,
    ) -> Result<()> {
        let (ledger_name, branch) = split_ledger_id(ledger_id)?;
        let address = Self::index_address(&ledger_name, &branch);
        let cid_str = index_id.to_string();

        self.storage
            .compare_and_swap(&address, |bytes| {
                let should_update = match bytes {
                    Some(data) => {
                        let existing: NsIndexFileV2 = deserialize_json(data)?;
                        index_t >= existing.index.t // Allow equal
                    }
                    None => true,
                };

                if should_update {
                    let file = NsIndexFileV2 {
                        context: ns_context(),
                        index: IndexRef {
                            cid: Some(cid_str.clone()),
                            t: index_t,
                        },
                    };
                    let new_bytes = serialize_json(&file)?;
                    Ok(CasAction::Write(new_bytes))
                } else {
                    Ok(CasAction::Abort(()))
                }
            })
            .await?;

        Ok(())
    }
}

#[async_trait]
impl GraphSourcePublisher for FileNameService {
    async fn publish_graph_source(
        &self,
        name: &str,
        branch: &str,
        source_type: GraphSourceType,
        config: &str,
        dependencies: &[String],
    ) -> Result<()> {
        let address = Self::ns_address(name, branch);
        let name_c = name.to_string();
        let branch_c = branch.to_string();
        let config_c = config.to_string();
        let dependencies_c = dependencies.to_vec();
        let kind_type_str = match source_type.kind() {
            crate::GraphSourceKind::Index => "f:IndexSource".to_string(),
            crate::GraphSourceKind::Mapped => "f:MappedSource".to_string(),
            crate::GraphSourceKind::Ledger => "f:LedgerSource".to_string(),
        };
        let source_type_str = source_type.to_type_string();

        self.storage
            .compare_and_swap::<(), _>(&address, |bytes| {
                // For graph source config, we always update (config changes are allowed)
                // Only preserve retracted status if already set
                let status = match bytes {
                    Some(data) => {
                        let existing: GraphSourceNsFileV2 = deserialize_json(data)?;
                        if existing.status == "retracted" {
                            "retracted".to_string()
                        } else {
                            "ready".to_string()
                        }
                    }
                    None => "ready".to_string(),
                };

                let file = GraphSourceNsFileV2 {
                    context: ns_context(),
                    id: format_ledger_id(&name_c, &branch_c),
                    record_type: vec![kind_type_str.clone(), source_type_str.clone()],
                    name: name_c.clone(),
                    branch: branch_c.clone(),
                    config: ConfigRef {
                        value: config_c.clone(),
                    },
                    dependencies: dependencies_c.clone(),
                    status,
                };
                let new_bytes = serialize_json(&file)?;
                Ok(CasAction::<()>::Write(new_bytes))
            })
            .await?;

        Ok(())
    }

    async fn publish_graph_source_index(
        &self,
        name: &str,
        branch: &str,
        index_id: &ContentId,
        index_t: i64,
    ) -> Result<()> {
        let address = Self::index_address(name, branch);
        let cid_str = index_id.to_string();
        let name_c = name.to_string();
        let branch_c = branch.to_string();

        self.storage
            .compare_and_swap(&address, |bytes| {
                // Strictly monotonic: only update if new_t > existing_t
                if let Some(data) = bytes {
                    let existing: GraphSourceIndexFileV2WithT = deserialize_json(data)?;
                    if index_t <= existing.index_t {
                        return Ok(CasAction::Abort(()));
                    }
                }

                let file = GraphSourceIndexFileV2WithT {
                    context: ns_context(),
                    id: format_ledger_id(&name_c, &branch_c),
                    index: GraphSourceIndexRef {
                        cid: cid_str.clone(),
                    },
                    index_t,
                };
                let new_bytes = serialize_json(&file)?;
                Ok(CasAction::Write(new_bytes))
            })
            .await?;

        Ok(())
    }

    async fn retract_graph_source(&self, name: &str, branch: &str) -> Result<()> {
        let address = Self::ns_address(name, branch);
        self.storage
            .compare_and_swap(&address, |bytes| {
                let Some(data) = bytes else {
                    return Ok(CasAction::Abort(()));
                };
                let mut file: GraphSourceNsFileV2 = deserialize_json(data)?;
                if file.status == "retracted" {
                    return Ok(CasAction::Abort(()));
                }
                file.status = "retracted".to_string();
                let new_bytes = serialize_json(&file)?;
                Ok(CasAction::Write(new_bytes))
            })
            .await?;

        Ok(())
    }
}

#[async_trait]
impl GraphSourceLookup for FileNameService {
    async fn lookup_graph_source(
        &self,
        graph_source_id: &str,
    ) -> Result<Option<GraphSourceRecord>> {
        let (name, branch) = split_ledger_id(graph_source_id)?;

        // First check if it's a graph source record
        if !self.is_graph_source_record(&name, &branch).await? {
            return Ok(None);
        }

        self.load_graph_source_record(&name, &branch).await
    }

    async fn lookup_any(&self, resource_id: &str) -> Result<NsLookupResult> {
        let (name, branch) = split_ledger_id(resource_id)?;
        let main_path = self.ns_path(&name, &branch);

        if !main_path.exists() {
            return Ok(NsLookupResult::NotFound);
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
        let ns_dir = self.storage.base_path().join(NS_VERSION);

        if !ns_dir.exists() {
            return Ok(vec![]);
        }

        let mut records = Vec::new();

        // Walk the ns@v2 directory recursively
        let mut stack = vec![ns_dir];

        while let Some(current_dir) = stack.pop() {
            let mut dir_entries = tokio::fs::read_dir(&current_dir).await.map_err(|e| {
                NameServiceError::storage(format!("Failed to read directory {current_dir:?}: {e}"))
            })?;

            while let Some(entry) = dir_entries.next_entry().await.map_err(|e| {
                NameServiceError::storage(format!("Failed to read directory entry: {e}"))
            })? {
                let path = entry.path();

                if path.is_dir() {
                    // Add subdirectory to stack for recursive processing
                    stack.push(path);
                } else if path.is_file() {
                    let file_name = entry.file_name().to_string_lossy().to_string();

                    // Skip index files and snapshot files, only process main .json files
                    if file_name.ends_with(".index.json") || file_name.ends_with(".snapshots.json")
                    {
                        continue;
                    }

                    if file_name.ends_with(".json") {
                        // Extract name and branch from path
                        // Path structure: ns@v2/{name}/{branch}.json or ns@v2/{name}/{subdir}/.../{branch}.json
                        let ns_dir_base = self.storage.base_path().join(NS_VERSION);
                        if let Ok(relative_path) = path.strip_prefix(&ns_dir_base) {
                            // relative_path is like "gs-name/main.json" or "tenant/gs/main.json"
                            let parent = relative_path
                                .parent()
                                .map(|p| p.to_string_lossy().to_string())
                                .unwrap_or_default();
                            let branch = file_name.trim_end_matches(".json");

                            // Check if this is a graph source record
                            if self.is_graph_source_record(&parent, branch).await? {
                                if let Ok(Some(record)) =
                                    self.load_graph_source_record(&parent, branch).await
                                {
                                    records.push(record);
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(records)
    }
}

#[async_trait]
impl RefLookup for FileNameService {
    async fn get_ref(&self, ledger_id: &str, kind: RefKind) -> Result<Option<RefValue>> {
        let (ledger_name, branch) = split_ledger_id(ledger_id)?;
        match kind {
            RefKind::CommitHead => {
                let address = Self::ns_address(&ledger_name, &branch);
                let main_file: Option<NsFileV2> = self.read_json_from_address(&address).await?;
                match main_file {
                    None => Ok(None),
                    Some(f) => Ok(Some(RefValue {
                        id: f
                            .commit_cid
                            .as_deref()
                            .and_then(|s| s.parse::<ContentId>().ok()),
                        t: f.t,
                    })),
                }
            }
            RefKind::IndexHead => {
                // Check separate index file first, then fall back to main file.
                let index_address = Self::index_address(&ledger_name, &branch);
                let index_file: Option<NsIndexFileV2> =
                    self.read_json_from_address(&index_address).await?;

                if let Some(idx) = index_file {
                    return Ok(Some(RefValue {
                        id: idx
                            .index
                            .cid
                            .as_deref()
                            .and_then(|s| s.parse::<ContentId>().ok()),
                        t: idx.index.t,
                    }));
                }

                // Fall back to main file's inline index.
                let main_address = Self::ns_address(&ledger_name, &branch);
                let main_file: Option<NsFileV2> =
                    self.read_json_from_address(&main_address).await?;
                match main_file {
                    None => Ok(None),
                    Some(f) => Ok(Some(RefValue {
                        id: f
                            .index
                            .as_ref()
                            .and_then(|i| i.cid.as_deref())
                            .and_then(|s| s.parse::<ContentId>().ok()),
                        t: f.index.as_ref().map(|i| i.t).unwrap_or(0),
                    })),
                }
            }
        }
    }
}

#[async_trait]
impl RefPublisher for FileNameService {
    async fn compare_and_set_ref(
        &self,
        ledger_id: &str,
        kind: RefKind,
        expected: Option<&RefValue>,
        new: &RefValue,
    ) -> Result<CasResult> {
        let (ledger_name, branch) = split_ledger_id(ledger_id)?;
        let expected_clone = expected.cloned();
        let new_clone = new.clone();
        let normalized_address = format_ledger_id(&ledger_name, &branch);

        match kind {
            RefKind::CommitHead => {
                let address = Self::ns_address(&ledger_name, &branch);
                let ledger_name_c = ledger_name.clone();
                let branch_c = branch.clone();
                let address_c = normalized_address.clone();

                let outcome = self
                    .storage
                    .compare_and_swap(&address, |bytes| {
                        let existing: Option<NsFileV2> = bytes.map(deserialize_json).transpose()?;

                        let current_ref = existing.as_ref().map(|f| RefValue {
                            id: f
                                .commit_cid
                                .as_deref()
                                .and_then(|s| s.parse::<ContentId>().ok()),
                            t: f.t,
                        });

                        if let Some(conflict) = check_cas_expectation(
                            &expected_clone,
                            &current_ref,
                            true,
                            ref_values_match,
                            |actual| CasResult::Conflict { actual },
                        ) {
                            return Ok(CasAction::Abort(conflict));
                        }

                        // Monotonic guard: CommitHead requires strict new.t > current.t
                        if let Some(ref cur) = current_ref {
                            if new_clone.t <= cur.t {
                                return Ok(CasAction::Abort(CasResult::Conflict {
                                    actual: Some(cur.clone()),
                                }));
                            }
                        }

                        // Apply update.
                        let mut file = existing.unwrap_or_else(|| NsFileV2 {
                            context: ns_context(),
                            id: address_c.clone(),
                            record_type: vec!["f:LedgerSource".to_string()],
                            ledger: LedgerRef {
                                id: ledger_name_c.clone(),
                            },
                            branch: branch_c.clone(),
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
                        });

                        // CID goes into the commit_cid field.
                        file.commit_cid =
                            new_clone.id.as_ref().map(std::string::ToString::to_string);
                        file.t = new_clone.t;

                        let new_bytes = serialize_json(&file)?;
                        Ok(CasAction::Write(new_bytes))
                    })
                    .await?;

                let result = match outcome {
                    CasOutcome::Written => CasResult::Updated,
                    CasOutcome::Aborted(r) => r,
                };

                Ok(result)
            }

            RefKind::IndexHead => {
                let address = Self::index_address(&ledger_name, &branch);

                // Pre-read the main file's inline index ref so we can fall
                // back to it when the separate index file doesn't exist yet.
                // This matches the fallback logic in `get_ref(IndexHead)`.
                let main_address = Self::ns_address(&ledger_name, &branch);
                let main_file_inline_ref: Option<RefValue> = {
                    let main_file: Option<NsFileV2> =
                        self.read_json_from_address(&main_address).await?;
                    main_file.map(|f| RefValue {
                        id: f
                            .index
                            .as_ref()
                            .and_then(|i| i.cid.as_deref())
                            .and_then(|s| s.parse::<ContentId>().ok()),
                        t: f.index.as_ref().map(|i| i.t).unwrap_or(0),
                    })
                };

                let outcome = self
                    .storage
                    .compare_and_swap(&address, |bytes| {
                        let existing: Option<NsIndexFileV2> =
                            bytes.map(deserialize_json).transpose()?;

                        // When the separate index file doesn't exist yet,
                        // use the main file's inline index ref (matches get_ref
                        // fallback). This handles freshly created ledgers where
                        // the main file has index: None but no separate index
                        // file has been written.
                        let current_ref = match existing.as_ref() {
                            Some(f) => Some(RefValue {
                                id: f
                                    .index
                                    .cid
                                    .as_deref()
                                    .and_then(|s| s.parse::<ContentId>().ok()),
                                t: f.index.t,
                            }),
                            None => main_file_inline_ref.clone(),
                        };

                        if let Some(conflict) = check_cas_expectation(
                            &expected_clone,
                            &current_ref,
                            true,
                            ref_values_match,
                            |actual| CasResult::Conflict { actual },
                        ) {
                            return Ok(CasAction::Abort(conflict));
                        }

                        // Monotonic guard: IndexHead allows new.t >= current.t
                        if let Some(ref cur) = current_ref {
                            if new_clone.t < cur.t {
                                return Ok(CasAction::Abort(CasResult::Conflict {
                                    actual: Some(cur.clone()),
                                }));
                            }
                        }

                        // Apply update: CID goes into IndexRef.cid.
                        let file = NsIndexFileV2 {
                            context: ns_context(),
                            index: IndexRef {
                                cid: new_clone.id.as_ref().map(std::string::ToString::to_string),
                                t: new_clone.t,
                            },
                        };
                        let new_bytes = serialize_json(&file)?;
                        Ok(CasAction::Write(new_bytes))
                    })
                    .await?;

                let result = match outcome {
                    CasOutcome::Written => CasResult::Updated,
                    CasOutcome::Aborted(r) => r,
                };

                Ok(result)
            }
        }
    }
}

// ---------------------------------------------------------------------------
// V2 Extension: StatusPublisher and ConfigPublisher
// ---------------------------------------------------------------------------

#[async_trait]
impl StatusLookup for FileNameService {
    async fn get_status(&self, ledger_id: &str) -> Result<Option<StatusValue>> {
        let (ledger_name, branch) = split_ledger_id(ledger_id)?;
        let address = Self::ns_address(&ledger_name, &branch);

        let main_file: Option<NsFileV2> = self.read_json_from_address(&address).await?;

        Ok(main_file.map(|f| f.to_status_value()))
    }
}

#[async_trait]
impl StatusPublisher for FileNameService {
    async fn push_status(
        &self,
        ledger_id: &str,
        expected: Option<&StatusValue>,
        new: &StatusValue,
    ) -> Result<StatusCasResult> {
        let (ledger_name, branch) = split_ledger_id(ledger_id)?;
        let address = Self::ns_address(&ledger_name, &branch);

        // Clone values for the closure
        let expected_clone = expected.cloned();
        let new_clone = new.clone();

        let outcome = self
            .storage
            .compare_and_swap(&address, |bytes| {
                let existing: Option<NsFileV2> = bytes.map(deserialize_json).transpose()?;

                let current = existing.as_ref().map(NsFileV2::to_status_value);

                if let Some(conflict) = check_cas_expectation(
                    &expected_clone,
                    &current,
                    false,
                    |exp, actual| exp.v == actual.v && exp.payload == actual.payload,
                    |actual| StatusCasResult::Conflict { actual },
                ) {
                    return Ok(CasAction::Abort(conflict));
                }

                // Monotonic guard: new.v > current.v
                let current_v = current.as_ref().map(|c| c.v).unwrap_or(0);
                if new_clone.v <= current_v {
                    return Ok(CasAction::Abort(StatusCasResult::Conflict {
                        actual: current,
                    }));
                }

                // Apply update
                let mut file = existing.unwrap();
                file.status = new_clone.payload.state.clone();
                file.status_v = Some(new_clone.v);
                file.status_meta = if new_clone.payload.extra.is_empty() {
                    None
                } else {
                    Some(new_clone.payload.extra.clone())
                };

                let new_bytes = serialize_json(&file)?;
                Ok(CasAction::Write(new_bytes))
            })
            .await?;

        match outcome {
            CasOutcome::Written => Ok(StatusCasResult::Updated),
            CasOutcome::Aborted(r) => Ok(r),
        }
    }
}

#[async_trait]
impl ConfigLookup for FileNameService {
    async fn get_config(&self, ledger_id: &str) -> Result<Option<ConfigValue>> {
        let (ledger_name, branch) = split_ledger_id(ledger_id)?;
        let address = Self::ns_address(&ledger_name, &branch);

        let main_file: Option<NsFileV2> = self.read_json_from_address(&address).await?;

        Ok(main_file.map(|f| f.to_config_value()))
    }
}

#[async_trait]
impl ConfigPublisher for FileNameService {
    async fn push_config(
        &self,
        ledger_id: &str,
        expected: Option<&ConfigValue>,
        new: &ConfigValue,
    ) -> Result<ConfigCasResult> {
        let (ledger_name, branch) = split_ledger_id(ledger_id)?;
        let address = Self::ns_address(&ledger_name, &branch);

        // Clone values for the closure
        let expected_clone = expected.cloned();
        let new_clone = new.clone();

        let outcome = self
            .storage
            .compare_and_swap(&address, |bytes| {
                let existing: Option<NsFileV2> = bytes.map(deserialize_json).transpose()?;

                let current = existing.as_ref().map(NsFileV2::to_config_value);

                if let Some(conflict) = check_cas_expectation(
                    &expected_clone,
                    &current,
                    false,
                    |exp, actual| exp.v == actual.v && exp.payload == actual.payload,
                    |actual| ConfigCasResult::Conflict { actual },
                ) {
                    return Ok(CasAction::Abort(conflict));
                }

                // Monotonic guard: new.v > current.v
                let current_v = current.as_ref().map(|c| c.v).unwrap_or(0);
                if new_clone.v <= current_v {
                    return Ok(CasAction::Abort(ConfigCasResult::Conflict {
                        actual: current,
                    }));
                }

                // Apply update
                let mut file = existing.unwrap();
                file.config_v = Some(new_clone.v);

                if let Some(ref payload) = new_clone.payload {
                    // Write CID to field (CID-only)
                    file.default_context_cid = payload
                        .default_context
                        .as_ref()
                        .map(std::string::ToString::to_string);
                    file.config_meta = if payload.extra.is_empty() {
                        None
                    } else {
                        Some(payload.extra.clone())
                    };
                    file.config_cid = payload
                        .config_id
                        .as_ref()
                        .map(std::string::ToString::to_string);
                } else {
                    file.default_context_cid = None;
                    file.config_meta = None;
                    file.config_cid = None;
                }

                let new_bytes = serialize_json(&file)?;
                Ok(CasAction::Write(new_bytes))
            })
            .await?;

        match outcome {
            CasOutcome::Written => Ok(ConfigCasResult::Updated),
            CasOutcome::Aborted(r) => Ok(r),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::ContentKind;
    use tempfile::TempDir;

    /// Create a test ContentId from a label string (deterministic, reproducible).
    fn test_cid(label: &str) -> ContentId {
        ContentId::new(ContentKind::Commit, label.as_bytes())
    }

    async fn setup() -> (TempDir, FileNameService) {
        let temp_dir = TempDir::new().unwrap();
        let ns = FileNameService::new(temp_dir.path());
        (temp_dir, ns)
    }

    #[tokio::test]
    async fn test_file_ns_publish_commit() {
        let (_temp, ns) = setup().await;

        let cid1 = test_cid("commit-1");
        let cid2 = test_cid("commit-2");
        let cid_old = test_cid("commit-old");

        // First publish
        ns.publish_commit("mydb:main", 1, &cid1).await.unwrap();

        let record = ns.lookup("mydb:main").await.unwrap().unwrap();
        assert_eq!(record.commit_head_id, Some(cid1.clone()));
        assert_eq!(record.commit_t, 1);

        // Higher t should update
        ns.publish_commit("mydb:main", 5, &cid2).await.unwrap();

        let record = ns.lookup("mydb:main").await.unwrap().unwrap();
        assert_eq!(record.commit_head_id, Some(cid2.clone()));
        assert_eq!(record.commit_t, 5);

        // Lower t should be ignored
        ns.publish_commit("mydb:main", 3, &cid_old).await.unwrap();

        let record = ns.lookup("mydb:main").await.unwrap().unwrap();
        assert_eq!(record.commit_head_id, Some(cid2.clone()));
        assert_eq!(record.commit_t, 5);
    }

    #[tokio::test]
    async fn test_file_ns_separate_index_file() {
        let (_temp, ns) = setup().await;

        let commit_cid = test_cid("commit-1");
        let index_cid = ContentId::new(ContentKind::IndexRoot, b"index-1");

        // Publish commit
        ns.publish_commit("mydb:main", 10, &commit_cid)
            .await
            .unwrap();

        // Publish index (written to separate file)
        ns.publish_index("mydb:main", 5, &index_cid).await.unwrap();

        let record = ns.lookup("mydb:main").await.unwrap().unwrap();
        assert_eq!(record.commit_t, 10);
        assert_eq!(record.index_t, 5);
        assert_eq!(record.index_head_id, Some(index_cid));
        assert!(record.has_novelty());
    }

    #[tokio::test]
    async fn test_file_ns_index_merge_rule() {
        let (temp, ns) = setup().await;

        let commit_cid = test_cid("commit-1");
        let index_new_cid = ContentId::new(ContentKind::IndexRoot, b"index-new");

        // Publish commit with embedded index
        ns.publish_commit("mydb:main", 10, &commit_cid)
            .await
            .unwrap();

        // Manually add index to main file
        let main_path = temp.path().join("ns@v2/mydb/main.json");
        let index_old_cid = ContentId::new(ContentKind::IndexRoot, b"index-old");
        let mut content: NsFileV2 =
            serde_json::from_str(&tokio::fs::read_to_string(&main_path).await.unwrap()).unwrap();
        content.index = Some(IndexRef {
            cid: Some(index_old_cid.to_string()),
            t: 5,
        });
        tokio::fs::write(&main_path, serde_json::to_string_pretty(&content).unwrap())
            .await
            .unwrap();

        // Publish newer index to separate file
        ns.publish_index("mydb:main", 8, &index_new_cid)
            .await
            .unwrap();

        // Lookup should prefer the index file (8 >= 5)
        let record = ns.lookup("mydb:main").await.unwrap().unwrap();
        assert_eq!(record.index_head_id, Some(index_new_cid));
        assert_eq!(record.index_t, 8);
    }

    #[tokio::test]
    async fn test_file_ns_all_records() {
        let (_temp, ns) = setup().await;

        ns.publish_commit("db1:main", 1, &test_cid("commit-1"))
            .await
            .unwrap();
        ns.publish_commit("db2:main", 1, &test_cid("commit-2"))
            .await
            .unwrap();
        ns.publish_commit("db3:dev", 1, &test_cid("commit-3"))
            .await
            .unwrap();

        let records = ns.all_records().await.unwrap();
        assert_eq!(records.len(), 3);
    }

    #[tokio::test]
    async fn test_file_ns_ledger_with_slash() {
        let (_temp, ns) = setup().await;

        ns.publish_commit("tenant/customers:main", 1, &test_cid("commit-1"))
            .await
            .unwrap();

        let record = ns.lookup("tenant/customers:main").await.unwrap().unwrap();
        assert_eq!(record.name, "tenant/customers");
        assert_eq!(record.branch, "main");
    }

    // ========== Graph Source Tests ==========

    #[tokio::test]
    async fn test_graph_source_publish_and_lookup() {
        let (_temp, ns) = setup().await;

        let config = r#"{"k1":1.2,"b":0.75}"#;
        let deps = vec!["source-ledger:main".to_string()];

        ns.publish_graph_source("my-search", "main", GraphSourceType::Bm25, config, &deps)
            .await
            .unwrap();

        let record = ns
            .lookup_graph_source("my-search:main")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(record.name, "my-search");
        assert_eq!(record.branch, "main");
        assert_eq!(record.source_type, GraphSourceType::Bm25);
        assert_eq!(record.config, config);
        assert_eq!(record.dependencies, deps);
        assert_eq!(record.index_id, None);
        assert_eq!(record.index_t, 0);
        assert!(!record.retracted);
    }

    #[tokio::test]
    async fn test_graph_source_publish_index_merge() {
        let (_temp, ns) = setup().await;

        let config = r#"{"k1":1.2}"#;
        let deps = vec!["source:main".to_string()];

        // Publish graph source config
        ns.publish_graph_source("my-gs", "main", GraphSourceType::Bm25, config, &deps)
            .await
            .unwrap();

        // Publish graph source index
        let gs_index_cid = ContentId::new(ContentKind::IndexRoot, b"gs-index-1");
        ns.publish_graph_source_index("my-gs", "main", &gs_index_cid, 42)
            .await
            .unwrap();

        // Lookup should merge config + index
        let record = ns.lookup_graph_source("my-gs:main").await.unwrap().unwrap();
        assert_eq!(record.config, config);
        assert_eq!(record.index_id, Some(gs_index_cid));
        assert_eq!(record.index_t, 42);
    }

    #[tokio::test]
    async fn test_graph_source_index_monotonic_update() {
        let (_temp, ns) = setup().await;

        let config = r"{}";
        ns.publish_graph_source("gs", "main", GraphSourceType::Bm25, config, &[])
            .await
            .unwrap();

        // First index publish
        let gs_cid_v1 = ContentId::new(ContentKind::IndexRoot, b"index-v1");
        let gs_cid_v2 = ContentId::new(ContentKind::IndexRoot, b"index-v2");
        let gs_cid_old = ContentId::new(ContentKind::IndexRoot, b"index-old");
        let gs_cid_same = ContentId::new(ContentKind::IndexRoot, b"index-same");

        ns.publish_graph_source_index("gs", "main", &gs_cid_v1, 10)
            .await
            .unwrap();

        // Higher t should update
        ns.publish_graph_source_index("gs", "main", &gs_cid_v2, 20)
            .await
            .unwrap();

        let record = ns.lookup_graph_source("gs:main").await.unwrap().unwrap();
        assert_eq!(record.index_id, Some(gs_cid_v2.clone()));
        assert_eq!(record.index_t, 20);

        // Lower t should be ignored (monotonic rule)
        ns.publish_graph_source_index("gs", "main", &gs_cid_old, 15)
            .await
            .unwrap();

        let record = ns.lookup_graph_source("gs:main").await.unwrap().unwrap();
        assert_eq!(record.index_id, Some(gs_cid_v2.clone()));
        assert_eq!(record.index_t, 20);

        // Equal t should also be ignored
        ns.publish_graph_source_index("gs", "main", &gs_cid_same, 20)
            .await
            .unwrap();

        let record = ns.lookup_graph_source("gs:main").await.unwrap().unwrap();
        assert_eq!(record.index_id, Some(gs_cid_v2));
    }

    #[tokio::test]
    async fn test_graph_source_retract() {
        let (_temp, ns) = setup().await;

        ns.publish_graph_source("gs", "main", GraphSourceType::Bm25, "{}", &[])
            .await
            .unwrap();

        let record = ns.lookup_graph_source("gs:main").await.unwrap().unwrap();
        assert!(!record.retracted);

        ns.retract_graph_source("gs", "main").await.unwrap();

        let record = ns.lookup_graph_source("gs:main").await.unwrap().unwrap();
        assert!(record.retracted);
    }

    #[tokio::test]
    async fn test_graph_source_lookup_any_distinguishes_types() {
        let (_temp, ns) = setup().await;

        // Create a regular ledger
        ns.publish_commit("ledger:main", 1, &test_cid("commit-1"))
            .await
            .unwrap();

        // Create a graph source
        ns.publish_graph_source("gs", "main", GraphSourceType::Bm25, "{}", &[])
            .await
            .unwrap();

        // lookup_any should return correct type
        match ns.lookup_any("ledger:main").await.unwrap() {
            NsLookupResult::Ledger(r) => assert_eq!(r.name, "ledger"),
            other => panic!("Expected Ledger, got {other:?}"),
        }

        match ns.lookup_any("gs:main").await.unwrap() {
            NsLookupResult::GraphSource(r) => assert_eq!(r.name, "gs"),
            other => panic!("Expected GraphSource, got {other:?}"),
        }

        // Non-existent should return NotFound
        match ns.lookup_any("nonexistent:main").await.unwrap() {
            NsLookupResult::NotFound => {}
            other => panic!("Expected NotFound, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_graph_source_lookup_returns_none_for_ledger() {
        let (_temp, ns) = setup().await;

        // Create a regular ledger
        ns.publish_commit("ledger:main", 1, &test_cid("commit-1"))
            .await
            .unwrap();

        // lookup_graph_source should return None for a ledger
        let result = ns.lookup_graph_source("ledger:main").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_graph_source_config_update_preserves_index() {
        let (_temp, ns) = setup().await;

        // Publish initial config
        ns.publish_graph_source("gs", "main", GraphSourceType::Bm25, r#"{"v":1}"#, &[])
            .await
            .unwrap();

        // Publish index
        let gs_idx_cid = ContentId::new(ContentKind::IndexRoot, b"gs-index-1");
        ns.publish_graph_source_index("gs", "main", &gs_idx_cid, 10)
            .await
            .unwrap();

        // Update config (should not affect index)
        ns.publish_graph_source("gs", "main", GraphSourceType::Bm25, r#"{"v":2}"#, &[])
            .await
            .unwrap();

        let record = ns.lookup_graph_source("gs:main").await.unwrap().unwrap();
        assert_eq!(record.config, r#"{"v":2}"#);
        // Index should still be present (from separate file)
        assert_eq!(record.index_id, Some(gs_idx_cid));
        assert_eq!(record.index_t, 10);
    }

    #[tokio::test]
    async fn test_graph_source_type_variants() {
        let (_temp, ns) = setup().await;

        // Test different graph source types
        ns.publish_graph_source("bm25-gs", "main", GraphSourceType::Bm25, "{}", &[])
            .await
            .unwrap();
        ns.publish_graph_source("r2rml-gs", "main", GraphSourceType::R2rml, "{}", &[])
            .await
            .unwrap();
        ns.publish_graph_source("iceberg-gs", "main", GraphSourceType::Iceberg, "{}", &[])
            .await
            .unwrap();
        ns.publish_graph_source(
            "custom-gs",
            "main",
            GraphSourceType::Unknown("f:CustomType".to_string()),
            "{}",
            &[],
        )
        .await
        .unwrap();

        assert_eq!(
            ns.lookup_graph_source("bm25-gs:main")
                .await
                .unwrap()
                .unwrap()
                .source_type,
            GraphSourceType::Bm25
        );
        assert_eq!(
            ns.lookup_graph_source("r2rml-gs:main")
                .await
                .unwrap()
                .unwrap()
                .source_type,
            GraphSourceType::R2rml
        );
        assert_eq!(
            ns.lookup_graph_source("iceberg-gs:main")
                .await
                .unwrap()
                .unwrap()
                .source_type,
            GraphSourceType::Iceberg
        );
        assert_eq!(
            ns.lookup_graph_source("custom-gs:main")
                .await
                .unwrap()
                .unwrap()
                .source_type,
            GraphSourceType::Unknown("f:CustomType".to_string())
        );
    }

    // =========================================================================
    // RefPublisher tests
    // =========================================================================

    #[tokio::test]
    async fn test_file_ref_get_ref_unknown_alias() {
        let (_dir, ns) = setup().await;
        let result = ns
            .get_ref("nonexistent:main", RefKind::CommitHead)
            .await
            .unwrap();
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn test_file_ref_get_ref_after_publish() {
        let (_dir, ns) = setup().await;
        let cid1 = test_cid("commit-1");
        ns.publish_commit("mydb:main", 5, &cid1).await.unwrap();

        let commit = ns
            .get_ref("mydb:main", RefKind::CommitHead)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(commit.id, Some(cid1));
        assert_eq!(commit.t, 5);
    }

    #[tokio::test]
    async fn test_file_ref_cas_create_new() {
        let (_dir, ns) = setup().await;
        let new_ref = RefValue { id: None, t: 1 };

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
        assert_eq!(current.id, None);
        assert_eq!(current.t, 1);
    }

    #[tokio::test]
    async fn test_file_ref_cas_conflict_already_exists() {
        let (_dir, ns) = setup().await;
        let cid1 = test_cid("commit-1");
        ns.publish_commit("mydb:main", 1, &cid1).await.unwrap();

        let new_ref = RefValue { id: None, t: 2 };
        let result = ns
            .compare_and_set_ref("mydb:main", RefKind::CommitHead, None, &new_ref)
            .await
            .unwrap();
        match result {
            CasResult::Conflict { actual } => {
                let a = actual.unwrap();
                assert_eq!(a.id, Some(cid1));
            }
            _ => panic!("expected conflict"),
        }
    }

    #[tokio::test]
    async fn test_file_ref_cas_cid_mismatch() {
        let (_dir, ns) = setup().await;
        let cid1 = test_cid("commit-1");
        ns.publish_commit("mydb:main", 1, &cid1).await.unwrap();

        let wrong_cid = test_cid("wrong");
        let expected = RefValue {
            id: Some(wrong_cid),
            t: 1,
        };
        let new_ref = RefValue {
            id: Some(test_cid("commit-2")),
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
    async fn test_file_ref_cas_success() {
        let (_dir, ns) = setup().await;
        let cid1 = test_cid("commit-1");
        ns.publish_commit("mydb:main", 1, &cid1).await.unwrap();

        // Expected must match what's stored: id from CID
        let expected = RefValue {
            id: Some(cid1.clone()),
            t: 1,
        };
        let cid2 = test_cid("commit-2");
        let new_ref = RefValue {
            id: Some(cid2.clone()),
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
        assert_eq!(current.id, Some(cid2));
        assert_eq!(current.t, 2);
    }

    #[tokio::test]
    async fn test_file_ref_cas_commit_strict_monotonic() {
        let (_dir, ns) = setup().await;
        let cid1 = test_cid("commit-1");
        ns.publish_commit("mydb:main", 5, &cid1).await.unwrap();

        let expected = RefValue {
            id: Some(cid1),
            t: 5,
        };
        let new_ref = RefValue {
            id: Some(test_cid("commit-2")),
            t: 5,
        };
        let result = ns
            .compare_and_set_ref("mydb:main", RefKind::CommitHead, Some(&expected), &new_ref)
            .await
            .unwrap();
        match result {
            CasResult::Conflict { .. } => {}
            _ => panic!("expected conflict for same t"),
        }
    }

    #[tokio::test]
    async fn test_file_ref_cas_index_allows_equal_t() {
        let (_dir, ns) = setup().await;
        let commit_cid = test_cid("commit-1");
        let index_cid = ContentId::new(ContentKind::IndexRoot, b"index-1");
        ns.publish_commit("mydb:main", 5, &commit_cid)
            .await
            .unwrap();
        ns.publish_index("mydb:main", 5, &index_cid).await.unwrap();

        // Expected must match stored: id from CID
        let expected = RefValue {
            id: Some(index_cid),
            t: 5,
        };
        let index_cid2 = ContentId::new(ContentKind::IndexRoot, b"index-2");
        let new_ref = RefValue {
            id: Some(index_cid2),
            t: 5,
        };
        let result = ns
            .compare_and_set_ref("mydb:main", RefKind::IndexHead, Some(&expected), &new_ref)
            .await
            .unwrap();
        assert_eq!(result, CasResult::Updated);
    }

    #[tokio::test]
    async fn test_file_ref_fast_forward_commit() {
        let (_dir, ns) = setup().await;
        let cid1 = test_cid("commit-1");
        ns.publish_commit("mydb:main", 1, &cid1).await.unwrap();

        let new_ref = RefValue {
            id: Some(test_cid("commit-5")),
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
    async fn test_file_ref_fast_forward_rejected_stale() {
        let (_dir, ns) = setup().await;
        let cid1 = test_cid("commit-1");
        ns.publish_commit("mydb:main", 10, &cid1).await.unwrap();

        let new_ref = RefValue {
            id: Some(test_cid("old")),
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
    async fn test_file_ref_cas_index_get_ref() {
        let (_dir, ns) = setup().await;
        let commit_cid = test_cid("commit-1");
        let index_cid = ContentId::new(ContentKind::IndexRoot, b"index-1");
        ns.publish_commit("mydb:main", 5, &commit_cid)
            .await
            .unwrap();
        ns.publish_index("mydb:main", 3, &index_cid).await.unwrap();

        let index = ns
            .get_ref("mydb:main", RefKind::IndexHead)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(index.id, Some(index_cid));
        assert_eq!(index.t, 3);
    }

    // =========================================================================
    // StatusPublisher tests
    // =========================================================================

    #[tokio::test]
    async fn test_file_status_get_nonexistent() {
        let (_dir, ns) = setup().await;
        let result = ns.get_status("nonexistent:main").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_file_status_get_initial() {
        let (_dir, ns) = setup().await;
        ns.publish_ledger_init("mydb:main").await.unwrap();

        let status = ns.get_status("mydb:main").await.unwrap().unwrap();
        assert_eq!(status.v, 1);
        assert_eq!(status.payload.state, "ready");
    }

    #[tokio::test]
    async fn test_file_status_push_update() {
        let (_dir, ns) = setup().await;
        ns.publish_ledger_init("mydb:main").await.unwrap();

        let initial = ns.get_status("mydb:main").await.unwrap().unwrap();

        // Push new status
        let new_status = crate::StatusValue::new(2, crate::StatusPayload::new("indexing"));
        let result = ns
            .push_status("mydb:main", Some(&initial), &new_status)
            .await
            .unwrap();
        assert!(matches!(result, crate::StatusCasResult::Updated));

        // Verify update
        let current = ns.get_status("mydb:main").await.unwrap().unwrap();
        assert_eq!(current.v, 2);
        assert_eq!(current.payload.state, "indexing");
    }

    #[tokio::test]
    async fn test_file_status_push_conflict() {
        let (_dir, ns) = setup().await;
        ns.publish_ledger_init("mydb:main").await.unwrap();

        // Try to push with wrong expected value
        let wrong_expected = crate::StatusValue::new(5, crate::StatusPayload::new("wrong"));
        let new_status = crate::StatusValue::new(6, crate::StatusPayload::new("indexing"));
        let result = ns
            .push_status("mydb:main", Some(&wrong_expected), &new_status)
            .await
            .unwrap();

        match result {
            crate::StatusCasResult::Conflict { actual } => {
                let a = actual.unwrap();
                assert_eq!(a.v, 1);
                assert_eq!(a.payload.state, "ready");
            }
            _ => panic!("expected conflict"),
        }
    }

    #[tokio::test]
    async fn test_file_retract_bumps_status_v() {
        let (_dir, ns) = setup().await;
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

    // =========================================================================
    // ConfigPublisher tests
    // =========================================================================

    #[tokio::test]
    async fn test_file_config_get_nonexistent() {
        let (_dir, ns) = setup().await;
        let result = ns.get_config("nonexistent:main").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_file_config_get_unborn() {
        let (_dir, ns) = setup().await;
        ns.publish_ledger_init("mydb:main").await.unwrap();

        let config = ns.get_config("mydb:main").await.unwrap().unwrap();
        assert!(config.is_unborn());
        assert_eq!(config.v, 0);
    }

    #[tokio::test]
    async fn test_file_config_push_from_unborn() {
        let (_dir, ns) = setup().await;
        ns.publish_ledger_init("mydb:main").await.unwrap();

        let unborn = ns.get_config("mydb:main").await.unwrap().unwrap();
        assert!(unborn.is_unborn());

        // Push first config
        let ctx_cid = ContentId::new(fluree_db_core::ContentKind::LedgerConfig, b"test-ctx-v1");
        let new_config = crate::ConfigValue::new(
            1,
            Some(crate::ConfigPayload::with_default_context(ctx_cid.clone())),
        );
        let result = ns
            .push_config("mydb:main", Some(&unborn), &new_config)
            .await
            .unwrap();
        assert!(matches!(result, crate::ConfigCasResult::Updated));

        // Verify update
        let current = ns.get_config("mydb:main").await.unwrap().unwrap();
        assert!(!current.is_unborn());
        assert_eq!(current.v, 1);
        assert_eq!(
            current.payload.as_ref().unwrap().default_context,
            Some(ctx_cid)
        );
    }

    #[tokio::test]
    async fn test_file_config_push_conflict() {
        let (_dir, ns) = setup().await;
        ns.publish_ledger_init("mydb:main").await.unwrap();

        // Try to push with wrong expected value
        let wrong_expected = crate::ConfigValue::new(5, Some(crate::ConfigPayload::new()));
        let new_config = crate::ConfigValue::new(6, Some(crate::ConfigPayload::new()));
        let result = ns
            .push_config("mydb:main", Some(&wrong_expected), &new_config)
            .await
            .unwrap();

        match result {
            crate::ConfigCasResult::Conflict { actual } => {
                let a = actual.unwrap();
                assert!(a.is_unborn());
            }
            _ => panic!("expected conflict"),
        }
    }

    // =========================================================================
    // Branch tests
    // =========================================================================

    #[tokio::test]
    async fn test_file_create_branch_from_main() {
        let (_temp, ns) = setup().await;
        ns.publish_ledger_init("mydb:main").await.unwrap();
        let cid = test_cid("commit-5");
        ns.publish_commit("mydb:main", 5, &cid).await.unwrap();

        ns.create_branch("mydb", "feature-x", "main", None)
            .await
            .unwrap();

        let record = ns.lookup("mydb:feature-x").await.unwrap().unwrap();
        assert_eq!(record.name, "mydb");
        assert_eq!(record.branch, "feature-x");
        assert_eq!(record.commit_head_id, Some(cid.clone()));
        assert_eq!(record.commit_t, 5);
        assert_eq!(record.source_branch.as_deref(), Some("main"));
    }

    #[tokio::test]
    async fn test_file_create_branch_duplicate_fails() {
        let (_temp, ns) = setup().await;
        ns.publish_ledger_init("mydb:main").await.unwrap();
        let cid = test_cid("commit-1");
        ns.publish_commit("mydb:main", 1, &cid).await.unwrap();

        ns.create_branch("mydb", "dev", "main", None).await.unwrap();

        let result = ns.create_branch("mydb", "dev", "main", None).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_file_list_branches() {
        let (_temp, ns) = setup().await;
        ns.publish_ledger_init("mydb:main").await.unwrap();
        let cid = test_cid("commit-3");
        ns.publish_commit("mydb:main", 3, &cid).await.unwrap();

        ns.create_branch("mydb", "dev", "main", None).await.unwrap();
        ns.create_branch("mydb", "staging", "main", None)
            .await
            .unwrap();

        // Also create a different ledger to ensure filtering works
        ns.publish_ledger_init("other:main").await.unwrap();

        let branches = ns.list_branches("mydb").await.unwrap();
        assert_eq!(branches.len(), 3);
        let mut names: Vec<&str> = branches.iter().map(|r| r.branch.as_str()).collect();
        names.sort();
        assert_eq!(names, vec!["dev", "main", "staging"]);
    }

    #[tokio::test]
    async fn test_file_list_branches_with_slashes() {
        let (_temp, ns) = setup().await;
        ns.publish_ledger_init("mydb:main").await.unwrap();
        let cid = test_cid("commit-1");
        ns.publish_commit("mydb:main", 1, &cid).await.unwrap();

        ns.create_branch("mydb", "release/v1.0", "main", None)
            .await
            .unwrap();
        ns.create_branch("mydb", "feature/auth", "main", None)
            .await
            .unwrap();

        let branches = ns.list_branches("mydb").await.unwrap();
        assert_eq!(branches.len(), 3);
        let mut names: Vec<&str> = branches.iter().map(|r| r.branch.as_str()).collect();
        names.sort();
        assert_eq!(names, vec!["feature/auth", "main", "release/v1.0"]);
    }

    #[tokio::test]
    async fn test_file_list_branches_unknown_ledger() {
        let (_temp, ns) = setup().await;
        let branches = ns.list_branches("nonexistent").await.unwrap();
        assert!(branches.is_empty());
    }

    #[tokio::test]
    async fn test_file_list_branches_excludes_retracted() {
        let (_temp, ns) = setup().await;
        ns.publish_ledger_init("mydb:main").await.unwrap();
        let cid = test_cid("commit-1");
        ns.publish_commit("mydb:main", 1, &cid).await.unwrap();

        ns.create_branch("mydb", "dead", "main", None)
            .await
            .unwrap();
        ns.retract("mydb:dead").await.unwrap();

        let branches = ns.list_branches("mydb").await.unwrap();
        assert_eq!(branches.len(), 1);
        assert_eq!(branches[0].branch, "main");
    }

    #[tokio::test]
    async fn test_file_branch_point_persists_across_reload() {
        let (temp, ns) = setup().await;
        ns.publish_ledger_init("mydb:main").await.unwrap();
        let cid = test_cid("commit-2");
        ns.publish_commit("mydb:main", 2, &cid).await.unwrap();

        ns.create_branch("mydb", "persisted", "main", None)
            .await
            .unwrap();

        // Create a new FileNameService pointing to the same directory
        let ns2 = FileNameService::new(temp.path());
        let record = ns2.lookup("mydb:persisted").await.unwrap().unwrap();
        assert_eq!(record.source_branch.as_deref(), Some("main"));
        assert_eq!(record.commit_head_id, Some(cid));
        assert_eq!(record.commit_t, 2);
    }
}
