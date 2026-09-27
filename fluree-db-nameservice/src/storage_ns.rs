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
//! Uses the ns@v3 format compatible with legacy implementations:
//! - `{prefix}/ns@v3/{ledger-name}/{branch}.json` - Main record (commit info)
//! - `{prefix}/ns@v3/{ledger-name}/{branch}.index.json` - Index record (separate for indexer)
//!
//! # Concurrency
//!
//! Uses ETag-based compare-and-swap (CAS) operations for atomic updates.
//! Under contention, operations will retry with exponential backoff.

use crate::binding::{
    BranchRecordStore, DroppedLedger, Fence, FenceOutcome, LedgerRegistry, NameBinding,
    RegistryCas, Versioned,
};
use crate::ns_cas::{self, index_admits, main_admits, FenceRefused, RecordKeys};
use crate::ns_format::{
    merge_heads, ns_context, IndexRef, LedgerRef, NsFileV2, NsIndexFileV2, NS_VERSION,
};
use crate::{
    deserialize_json, serialize_json, AdminPublisher, BranchLifecycle, CasResult, CommitPublisher,
    ConfigCasResult, ConfigLookup, ConfigPublisher, ConfigValue, GraphSourceLookup,
    GraphSourcePublisher, GraphSourceRecord, GraphSourceType, IndexPublisher, LedgerHeads,
    NameServiceError, NameServiceLookup, NsLookupResult, NsRecord, RefKind, RefLookup,
    RefPublisher, RefValue, Result, StatusCasResult, StatusLookup, StatusPublisher, StatusValue,
};
use async_trait::async_trait;
use fluree_db_core::ledger_id::{format_ledger_id, split_ledger_id};
use fluree_db_core::{
    CasAction, CasOutcome, ContentId, Error as CoreError, InstanceId, LedgerId, StorageCas,
    StorageList, StorageRead, StorageWrite,
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
// Graph Source File Structures (ns@v3 format)
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
    /// The key of `relative` under the versioned nameservice root.
    fn ns_root_key(&self, relative: &str) -> String {
        if self.prefix.is_empty() {
            format!("{NS_VERSION}/{relative}")
        } else {
            format!("{}/{NS_VERSION}/{relative}", self.prefix)
        }
    }

    fn binding_key(&self, name: &str) -> String {
        self.ns_root_key(&format!("{name}/@binding.json"))
    }

    fn dropped_key(&self, instance: &InstanceId) -> String {
        self.ns_root_key(&format!("@dropped/{instance}.json"))
    }

    /// Whether `key` is a binding or registry file rather than a record.
    /// `@` is reserved in names and branches, so no record path has a
    /// segment starting with it.
    fn is_registry_key(&self, key: &str) -> bool {
        key.strip_prefix(&self.ns_root_key(""))
            .is_some_and(|rest| rest.split('/').any(|segment| segment.starts_with('@')))
    }

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
            fence: None,
            extra: Default::default(),
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

        // Convert to GraphSourceRecord. The file is authoritative for
        // identity; a different name/branch at this path is another source.
        let graph_source_id = LedgerId::from_parts(&main.name, &main.branch)?;
        if graph_source_id.name() != name || graph_source_id.branch() != branch {
            return Ok(None);
        }
        let mut record = GraphSourceRecord {
            graph_source_id,
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

    /// Head pointers only: same keys and merge rule as `load_record`, minus
    /// the config/context/status fields.
    async fn load_heads(&self, ledger_name: &str, branch: &str) -> Result<Option<LedgerHeads>> {
        let main_key = self.ns_key(ledger_name, branch);
        let main_bytes = match self.storage.read_bytes(&main_key).await {
            Ok(bytes) => bytes,
            Err(CoreError::NotFound(_)) => return Ok(None),
            Err(e) => {
                return Err(NameServiceError::storage(format!(
                    "Failed to read {main_key}: {e}"
                )))
            }
        };
        if Self::is_graph_source_from_bytes(&main_bytes) {
            return Ok(None);
        }
        let main: NsFileV2 = serde_json::from_slice(&main_bytes)?;
        let index_file: Option<NsIndexFileV2> =
            self.read_json(&self.index_key(ledger_name, branch)).await?;
        Ok(Some(merge_heads(&main, index_file.as_ref())))
    }

    /// Load and merge main record with index file
    async fn load_record(&self, ledger_name: &str, branch: &str) -> Result<Option<NsRecord>> {
        let record = self
            .read_record_at(&self.ns_key(ledger_name, branch))
            .await?;
        // The file is authoritative for identity. A file whose name/branch
        // differ from the requested ones belongs to another ledger that maps
        // to the same path (`a:b/c` vs `a/b:c`), so this ledger does not exist.
        Ok(record.filter(|r| r.name == ledger_name && r.branch == branch))
    }

    /// Read the ledger record stored at `main_key`, taking its identity from
    /// the file rather than the path: a path does not determine `name:branch`
    /// once names and branches may both contain `/`.
    async fn read_record_at(&self, main_key: &str) -> Result<Option<NsRecord>> {
        let index_key = main_key
            .strip_suffix(".json")
            .map(|stem| format!("{stem}.index.json"))
            .ok_or_else(|| {
                NameServiceError::storage(format!("not an ns record key: {main_key}"))
            })?;

        // Read the main record bytes once.
        let main_bytes = match self.storage.read_bytes(main_key).await {
            Ok(bytes) => bytes,
            Err(CoreError::NotFound(_)) => return Ok(None),
            Err(e) => {
                return Err(NameServiceError::storage(format!(
                    "Failed to read {main_key}: {e}"
                )))
            }
        };

        // A graph-source record shares the `ns@v3/{name}/{branch}.json` key space
        // with ledger records but uses a different schema (no `f:ledger`). Report
        // it as "not a ledger" (Ok(None)) so single-alias resolution yields a
        // clean not-found and callers fall back to graph-source resolution —
        // instead of failing to deserialize NsFileV2 with a "missing field
        // `f:ledger`" error. Single guard shared by all ledger read paths.
        if Self::is_graph_source_from_bytes(&main_bytes) {
            return Ok(None);
        }

        // Enumeration hands us sidecar keys too (`{branch}.index.json`,
        // `{gs}.snapshots.json`): a branch may legitimately be named
        // `x.index`, so the suffix alone cannot say which file this is.
        if crate::ns_format::has_sidecar_suffix(main_key)
            && !crate::ns_format::is_ledger_main_record(&main_bytes)
        {
            return Ok(None);
        }

        let main: NsFileV2 = serde_json::from_slice(&main_bytes)?;

        // Read index file (if exists)
        let index_file: Option<NsIndexFileV2> = self.read_json(&index_key).await?;

        main.into_record(index_file)
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
}

impl<S> StorageNameService<S>
where
    S: StorageCas + Debug + Send + Sync,
{
    /// [`cas_update`](Self::cas_update) for a write presenting a fence:
    /// refused with [`NameServiceError::Fenced`] unless `admits` passes the
    /// current value.
    async fn cas_update_fenced<T, A, F>(
        &self,
        ledger_id: &str,
        key: &str,
        admits: A,
        update_fn: F,
    ) -> Result<()>
    where
        T: Serialize + for<'de> Deserialize<'de>,
        A: Fn(Option<&T>) -> bool + Send + Sync,
        F: Fn(Option<T>) -> Option<T> + Send + Sync,
    {
        self.cas_update_with_outcome_fenced(ledger_id, key, admits, |current| {
            match update_fn(current) {
                Some(value) => CasUpdateDecision::Apply(value),
                None => CasUpdateDecision::Skip(CasResult::Updated),
            }
        })
        .await?;
        Ok(())
    }

    /// Atomic read-modify-write for a write presenting a fence, returning an
    /// outcome the closure decides; see
    /// [`cas_update_fenced`](Self::cas_update_fenced).
    async fn cas_update_with_outcome_fenced<T, A, F>(
        &self,
        ledger_id: &str,
        key: &str,
        admits: A,
        update_fn: F,
    ) -> Result<CasUpdateOutcome>
    where
        T: Serialize + for<'de> Deserialize<'de>,
        A: Fn(Option<&T>) -> bool + Send + Sync,
        F: Fn(Option<T>) -> CasUpdateDecision<T> + Send + Sync,
    {
        let outcome = self
            .storage
            .compare_and_swap(key, |current_bytes| {
                let current: Option<T> = current_bytes.map(deserialize_json).transpose()?;
                if !admits(current.as_ref()) {
                    return Ok(CasAction::Abort(Err(FenceRefused)));
                }
                match update_fn(current) {
                    CasUpdateDecision::Apply(value) => {
                        Ok(CasAction::Write(serialize_json(&value)?))
                    }
                    CasUpdateDecision::Skip(result) => Ok(CasAction::Abort(Ok(result))),
                }
            })
            .await
            .map_err(|e| NameServiceError::storage(format!("CAS update failed for {key}: {e}")))?;

        match outcome {
            CasOutcome::Written => Ok(CasUpdateOutcome::Updated),
            CasOutcome::Aborted(Ok(result)) => Ok(CasUpdateOutcome::Skipped(result)),
            CasOutcome::Aborted(Err(FenceRefused)) => Err(NameServiceError::fenced(ledger_id)),
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
impl<S> NameServiceLookup for StorageNameService<S>
where
    S: StorageRead + StorageWrite + StorageList + StorageCas + Debug + Send + Sync,
{
    async fn lookup(&self, ledger_id: &str) -> Result<Option<NsRecord>> {
        let (ledger_name, branch) = split_ledger_id(ledger_id)?;
        // A graph-source record is not a ledger (#1369). `load_record` reports it
        // as Ok(None) so the caller can fall back to the graph-source path.
        let record = self.load_record(&ledger_name, &branch).await?;
        crate::read_resolved(self, record).await
    }

    async fn heads(&self, ledger_id: &str) -> Result<Option<LedgerHeads>> {
        let (ledger_name, branch) = split_ledger_id(ledger_id)?;
        self.load_heads(&ledger_name, &branch).await
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
            if !key.ends_with(".json") || self.is_registry_key(&key) {
                continue;
            }

            // Keys under `{ledger_name}/` also include nested ledgers
            // (`{ledger_name}/sub/main.json`); the record says which it is.
            if let Ok(Some(record)) = self.read_record_at(&key).await {
                if record.name == ledger_name {
                    records.push(record);
                }
            }
        }

        let mut records = crate::read_all_resolved(self, records).await?;
        records.retain(|r| !r.retracted);
        Ok(records)
    }

    async fn all_records(&self) -> Result<Vec<NsRecord>> {
        let records = BranchRecordStore::all_raw_records(self).await?;
        crate::read_all_resolved(self, records).await
    }
}

impl<S> StorageNameService<S>
where
    S: StorageRead + StorageWrite + StorageList + StorageCas + Debug + Send + Sync,
{
    /// Bring a store written before name bindings to the current format,
    /// once: see [`crate::migration`]. Returns what it bound, or `None` when
    /// the store was already current. Safe to repeat, to resume, and to run
    /// from several processes at once.
    pub async fn migrate(&self) -> Result<Option<crate::lifecycle::MigrationReport>> {
        use crate::migration::{FormatMarker, FORMAT_MARKER, LEGACY_NS_VERSION};

        let marker_key = self.ns_root_key(FORMAT_MARKER);
        match self.storage.read_bytes(&marker_key).await {
            Ok(bytes) => {
                FormatMarker::parse(&bytes)?;
                return Ok(None);
            }
            Err(CoreError::NotFound(_)) => {}
            Err(e) => {
                return Err(NameServiceError::storage(format!(
                    "reading the format marker: {e}"
                )))
            }
        }

        let legacy_root = if self.prefix.is_empty() {
            format!("{LEGACY_NS_VERSION}/")
        } else {
            format!("{}/{LEGACY_NS_VERSION}/", self.prefix)
        };
        let keys = StorageList::list_prefix(&self.storage, &legacy_root)
            .await
            .map_err(|e| NameServiceError::storage(format!("listing {legacy_root}: {e}")))?;
        for key in keys {
            let Some(relative) = key.strip_prefix(&legacy_root) else {
                continue;
            };
            if relative.ends_with(".lock") || relative.ends_with(".tmp") {
                continue;
            }
            let bytes = match self.storage.read_bytes(&key).await {
                Ok(bytes) => bytes,
                Err(CoreError::NotFound(_)) => continue,
                Err(e) => {
                    return Err(NameServiceError::storage(format!("reading {key}: {e}")));
                }
            };
            self.storage
                .insert(&self.ns_root_key(relative), &bytes)
                .await
                .map_err(|e| NameServiceError::storage(format!("copying {key}: {e}")))?;
        }

        let report = crate::lifecycle::migrate_legacy(self).await?;
        let marker = serde_json::to_vec_pretty(&FormatMarker::current())?;
        self.storage
            .insert(&marker_key, &marker)
            .await
            .map_err(|e| NameServiceError::storage(format!("writing the format marker: {e}")))?;
        if !report.is_empty() {
            tracing::info!(
                bound = report.bound.len(),
                dropped = report.dropped.len(),
                "nameservice migrated from {LEGACY_NS_VERSION} to {NS_VERSION}"
            );
        }
        Ok(Some(report))
    }

    async fn list_raw_records(&self) -> Result<Vec<NsRecord>> {
        let prefix = if self.prefix.is_empty() {
            NS_VERSION.to_string()
        } else {
            format!("{}/{}", self.prefix, NS_VERSION)
        };

        // List all files under ns@v3
        let keys = StorageList::list_prefix(&self.storage, &prefix)
            .await
            .map_err(|e| NameServiceError::storage(format!("Failed to list records: {e}")))?;

        let mut records = Vec::new();

        for key in keys {
            if !key.ends_with(".json") || self.is_registry_key(&key) {
                continue;
            }

            // A read failure must not silently shrink the result: callers
            // that decide what to delete treat a missing branch as one with
            // nothing to protect. `Ok(None)` is a legitimate skip
            // (graph-source record, or a branch dropped since the listing).
            if let Some(record) = self.read_record_at(&key).await? {
                records.push(record);
            }
        }
        Ok(records)
    }
}

#[async_trait]
impl<S> LedgerRegistry for StorageNameService<S>
where
    S: StorageRead + StorageWrite + StorageList + StorageCas + Debug + Send + Sync,
{
    async fn get_binding(&self, name: &str) -> Result<Option<Versioned<NameBinding>>> {
        ns_cas::read_versioned(&self.storage, &self.binding_key(name)).await
    }

    async fn cas_binding(
        &self,
        name: &str,
        expected: Option<u64>,
        new: Option<&NameBinding>,
    ) -> Result<RegistryCas<NameBinding>> {
        ns_cas::cas_versioned(&self.storage, &self.binding_key(name), expected, new).await
    }

    async fn list_bindings(&self) -> Result<Vec<(String, Versioned<NameBinding>)>> {
        let root = self.ns_root_key("");
        let keys = StorageList::list_prefix(&self.storage, &root)
            .await
            .map_err(|e| NameServiceError::storage(format!("Failed to list bindings: {e}")))?;
        let mut found = Vec::new();
        for key in keys {
            let Some(name) = key
                .strip_prefix(&root)
                .and_then(|rest| rest.strip_suffix("/@binding.json"))
            else {
                continue;
            };
            if let Some(binding) = self.get_binding(name).await? {
                found.push((name.to_string(), binding));
            }
        }
        Ok(found)
    }

    async fn get_dropped(&self, instance: &InstanceId) -> Result<Option<Versioned<DroppedLedger>>> {
        ns_cas::read_versioned(&self.storage, &self.dropped_key(instance)).await
    }

    async fn cas_dropped(
        &self,
        instance: &InstanceId,
        expected: Option<u64>,
        new: Option<&DroppedLedger>,
    ) -> Result<RegistryCas<DroppedLedger>> {
        ns_cas::cas_versioned(&self.storage, &self.dropped_key(instance), expected, new).await
    }

    async fn list_dropped(&self) -> Result<Vec<Versioned<DroppedLedger>>> {
        let dir = self.ns_root_key("@dropped/");
        let keys = StorageList::list_prefix(&self.storage, &dir)
            .await
            .map_err(|e| {
                NameServiceError::storage(format!("Failed to list dropped ledgers: {e}"))
            })?;
        let mut found = Vec::new();
        for key in keys {
            let Some(instance) = key
                .strip_prefix(&dir)
                .and_then(|rest| rest.strip_suffix(".json"))
                .and_then(|stem| InstanceId::parse(stem).ok())
            else {
                continue;
            };
            if let Some(dropped) = self.get_dropped(&instance).await? {
                found.push(dropped);
            }
        }
        Ok(found)
    }
}

#[async_trait]
impl<S> BranchRecordStore for StorageNameService<S>
where
    S: StorageRead + StorageWrite + StorageList + StorageCas + Debug + Send + Sync,
{
    async fn all_raw_records(&self) -> Result<Vec<NsRecord>> {
        self.list_raw_records().await
    }

    async fn raw_record(&self, ledger_id: &str) -> Result<Option<NsRecord>> {
        let (name, branch) = split_ledger_id(ledger_id)?;
        let (main, index) = (self.ns_key(&name, &branch), self.index_key(&name, &branch));
        ns_cas::raw_record(
            &self.storage,
            RecordKeys {
                main: &main,
                index: &index,
            },
        )
        .await
    }

    async fn insert_record(&self, record: &NsRecord) -> Result<Option<NsRecord>> {
        let (main, index) = (
            self.ns_key(&record.name, &record.branch),
            self.index_key(&record.name, &record.branch),
        );
        ns_cas::insert_record(
            &self.storage,
            RecordKeys {
                main: &main,
                index: &index,
            },
            record,
        )
        .await
    }

    async fn adopt_record(&self, ledger_id: &str, fence: Fence) -> Result<FenceOutcome> {
        let (name, branch) = split_ledger_id(ledger_id)?;
        let (main, index) = (self.ns_key(&name, &branch), self.index_key(&name, &branch));
        ns_cas::adopt_record(
            &self.storage,
            RecordKeys {
                main: &main,
                index: &index,
            },
            fence,
        )
        .await
    }

    async fn freeze_record(&self, ledger_id: &str, fence: Fence) -> Result<FenceOutcome> {
        let (name, branch) = split_ledger_id(ledger_id)?;
        let (main, index) = (self.ns_key(&name, &branch), self.index_key(&name, &branch));
        ns_cas::freeze_record(
            &self.storage,
            RecordKeys {
                main: &main,
                index: &index,
            },
            fence,
        )
        .await
    }

    async fn delete_record(&self, ledger_id: &str, fence: Fence) -> Result<FenceOutcome> {
        let (name, branch) = split_ledger_id(ledger_id)?;
        ns_cas::delete_record(&self.storage, &self.ns_key(&name, &branch), fence).await
    }

    async fn adjust_children(
        &self,
        ledger_id: &str,
        fence: Fence,
        delta: i32,
    ) -> Result<FenceOutcome> {
        let (name, branch) = split_ledger_id(ledger_id)?;
        ns_cas::adjust_children(&self.storage, &self.ns_key(&name, &branch), fence, delta).await
    }
}

#[async_trait]
impl<S> BranchLifecycle for StorageNameService<S>
where
    S: StorageRead + StorageWrite + StorageList + StorageCas + Debug + Send + Sync,
{
    async fn reset_head_fenced(
        &self,
        ledger_id: &str,
        fence: Option<Fence>,
        snapshot: crate::NsRecordSnapshot,
    ) -> Result<()> {
        let (ledger_name, branch) = split_ledger_id(ledger_id)?;
        let key = self.ns_key(&ledger_name, &branch);

        let outcome = self
            .storage
            .compare_and_swap(&key, |bytes| {
                let current: Option<NsFileV2> = bytes.map(deserialize_json).transpose()?;
                let Some(mut file) = current.filter(|f| !f.is_deleted()) else {
                    return Ok(CasAction::Abort(fence.is_some()));
                };
                if !file.admits(fence) {
                    return Ok(CasAction::Abort(true));
                }
                file.apply_snapshot(&snapshot);
                let new_bytes = serialize_json(&file)?;
                Ok(CasAction::Write(new_bytes))
            })
            .await?;

        match outcome {
            CasOutcome::Written => Ok(()),
            CasOutcome::Aborted(true) => Err(NameServiceError::fenced(ledger_id)),
            CasOutcome::Aborted(false) => Err(NameServiceError::not_found(ledger_id)),
        }
    }
}

#[async_trait]
impl<S> CommitPublisher for StorageNameService<S>
where
    S: StorageRead + StorageWrite + StorageList + StorageCas + Debug + Send + Sync,
{
    async fn publish_commit_fenced(
        &self,
        ledger_id: &str,
        fence: Option<Fence>,
        commit_t: i64,
        commit_id: &ContentId,
    ) -> Result<()> {
        let (ledger_name, branch) = split_ledger_id(ledger_id)?;
        let key = self.ns_key(&ledger_name, &branch);

        let ledger_name_clone = ledger_name.clone();
        let branch_clone = branch.clone();
        let cid_str = commit_id.to_string();

        self.cas_update_fenced::<NsFileV2, _, _>(
            ledger_id,
            &key,
            |current| main_admits(current, fence),
            move |existing| match existing.filter(|f| !f.is_deleted()) {
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
            },
        )
        .await
    }

    fn publishing_ledger_id(&self, ledger_id: &str) -> Option<String> {
        // Return normalized ledger ID for publishing
        LedgerId::parse(ledger_id).ok().map(String::from)
    }
}

#[async_trait]
impl<S> IndexPublisher for StorageNameService<S>
where
    S: StorageRead + StorageWrite + StorageList + StorageCas + Debug + Send + Sync,
{
    async fn publish_index_fenced(
        &self,
        ledger_id: &str,
        fence: Option<Fence>,
        index_t: i64,
        index_id: &ContentId,
    ) -> Result<()> {
        let (ledger_name, branch) = split_ledger_id(ledger_id)?;
        let key = self.index_key(&ledger_name, &branch);

        let cid_str = index_id.to_string();

        self.cas_update_fenced::<NsIndexFileV2, _, _>(
            ledger_id,
            &key,
            |current| index_admits(current, fence),
            move |existing| {
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
                    fence,
                    frozen: false,
                    extra: Default::default(),
                })
            },
        )
        .await
    }
}

#[async_trait]
impl<S> AdminPublisher for StorageNameService<S>
where
    S: StorageRead + StorageWrite + StorageList + StorageCas + Debug + Send + Sync,
{
    async fn publish_index_allow_equal_fenced(
        &self,
        ledger_id: &str,
        fence: Option<Fence>,
        index_t: i64,
        index_id: &ContentId,
    ) -> Result<()> {
        let (ledger_name, branch) = split_ledger_id(ledger_id)?;
        let index_key = self.index_key(&ledger_name, &branch);
        let cid_str = index_id.to_string();

        self.cas_update_fenced::<NsIndexFileV2, _, _>(
            ledger_id,
            &index_key,
            |current| index_admits(current, fence),
            |existing| {
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
                        fence,
                        frozen: false,
                        extra: Default::default(),
                    })
                } else {
                    None
                }
            },
        )
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
    async fn compare_and_set_ref_fenced(
        &self,
        ledger_id: &str,
        fence: Option<Fence>,
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
                    .cas_update_with_outcome_fenced::<NsFileV2, _, _>(
                        ledger_id,
                        &key,
                        |current| main_admits(current, fence),
                        move |existing| {
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
                            file.commit_cid =
                                new_cid.as_ref().map(std::string::ToString::to_string);
                            file.t = new_t;
                            CasUpdateDecision::Apply(file)
                        },
                    )
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
                    .cas_update_with_outcome_fenced::<NsIndexFileV2, _, _>(
                        ledger_id,
                        &key,
                        |current| index_admits(current, fence),
                        move |existing| {
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
                                            cid: new_cid
                                                .as_ref()
                                                .map(std::string::ToString::to_string),
                                            t: new_t,
                                        },
                                        fence,
                                        frozen: false,
                                        extra: Default::default(),
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
                                            cid: new_cid
                                                .as_ref()
                                                .map(std::string::ToString::to_string),
                                            t: new_t,
                                        },
                                        fence,
                                        frozen: false,
                                        extra: Default::default(),
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
                        },
                    )
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

            // Publishing config creates or reconfigures, so the record is
            // active — a retraction from an earlier drop does not survive
            // it (see `FileNameService::publish_graph_source`).
            let _ = &existing;
            let status = "ready".to_string();

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
            let record = self.load_record(&name, &branch).await?;
            match crate::read_resolved(self, record).await? {
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

        // List all files under ns@v3
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
            // Key format: {prefix}/ns@v3/{name}/{branch}.json
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
    async fn push_status_fenced(
        &self,
        ledger_id: &str,
        fence: Option<Fence>,
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
                let current_file: Option<NsFileV2> =
                    current_bytes.map(deserialize_json).transpose()?;
                if !main_admits(current_file.as_ref(), fence) {
                    return Ok(CasAction::Abort(Err(FenceRefused)));
                }
                let Some(mut file) = current_file else {
                    return Ok(CasAction::Abort(Ok(StatusCasResult::Conflict {
                        actual: None,
                    })));
                };

                let current = file.to_status_value();

                // Compare expected with current
                match &expected {
                    None => {
                        return Ok(CasAction::Abort(Ok(StatusCasResult::Conflict {
                            actual: Some(current),
                        })));
                    }
                    Some(exp) => {
                        if exp.v != current.v || exp.payload != current.payload {
                            return Ok(CasAction::Abort(Ok(StatusCasResult::Conflict {
                                actual: Some(current),
                            })));
                        }
                    }
                }

                // Monotonic guard: new.v > current.v
                if new.v <= current.v {
                    return Ok(CasAction::Abort(Ok(StatusCasResult::Conflict {
                        actual: Some(current),
                    })));
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
            CasOutcome::Aborted(Ok(result)) => Ok(result),
            CasOutcome::Aborted(Err(FenceRefused)) => Err(NameServiceError::fenced(ledger_id)),
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
    async fn push_config_fenced(
        &self,
        ledger_id: &str,
        fence: Option<Fence>,
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
                let current_file: Option<NsFileV2> =
                    current_bytes.map(deserialize_json).transpose()?;
                if !main_admits(current_file.as_ref(), fence) {
                    return Ok(CasAction::Abort(Err(FenceRefused)));
                }
                let Some(mut file) = current_file else {
                    return Ok(CasAction::Abort(Ok(ConfigCasResult::Conflict {
                        actual: None,
                    })));
                };

                let current = file.to_config_value();

                // Compare expected with current
                match &expected {
                    None => {
                        return Ok(CasAction::Abort(Ok(ConfigCasResult::Conflict {
                            actual: Some(current),
                        })));
                    }
                    Some(exp) => {
                        if exp.v != current.v || exp.payload != current.payload {
                            return Ok(CasAction::Abort(Ok(ConfigCasResult::Conflict {
                                actual: Some(current),
                            })));
                        }
                    }
                }

                // Monotonic guard: new.v > current.v
                if new.v <= current.v {
                    return Ok(CasAction::Abort(Ok(ConfigCasResult::Conflict {
                        actual: Some(current),
                    })));
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
            CasOutcome::Aborted(Ok(result)) => Ok(result),
            CasOutcome::Aborted(Err(FenceRefused)) => Err(NameServiceError::fenced(ledger_id)),
        }
    }
}

#[cfg(test)]
mod tests {
    mod lifecycle_conformance {
        crate::lifecycle_conformance_tests!((
            super::StorageNameService::new(super::MemoryCasStorage::new(), "test"),
            ()
        ));
    }

    use super::*;
    use crate::testing::CurrentFence;
    use crate::{CasResult, ConfigPayload, RefPublisher, RefValue, StatusPayload};
    use fluree_db_core::StorageExtError;

    async fn publish_commit(
        ns: &(impl RefPublisher + NameServiceLookup),
        ledger_id: &str,
        t: i64,
        cid: &ContentId,
    ) {
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
        let expected = format!("{prefix}/ns@v3/mydb/main.json");
        assert_eq!(
            expected,
            format!("{}/{}/{}/{}.json", prefix, NS_VERSION, "mydb", "main")
        );
    }

    #[test]
    fn test_ns_key_without_prefix() {
        let expected = format!("{}/{}/{}.json", NS_VERSION, "mydb", "main");
        assert_eq!(expected, "ns@v3/mydb/main.json");
    }

    #[test]
    fn test_index_key() {
        let expected = format!("{}/{}/{}.index.json", NS_VERSION, "mydb", "main");
        assert_eq!(expected, "ns@v3/mydb/main.index.json");
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
        fn permits_plaintext_cache(&self) -> bool {
            true
        }

        fn encryption_admin(&self) -> Option<std::sync::Arc<dyn fluree_db_core::EncryptionAdmin>> {
            None
        }

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
            _namespace: &fluree_db_core::StorageNamespace,
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

    #[tokio::test]
    async fn migrate_moves_a_legacy_store_to_the_current_address() {
        let ns = make_storage_ns();
        let mut record = NsRecord::new(LedgerId::parse("mydb:main").unwrap());
        record.commit_t = 2;
        record.commit_head_id = Some(ContentId::new(fluree_db_core::ContentKind::Commit, b"c"));
        let legacy_key = "test/ns@v2/mydb/main.json";
        ns.storage
            .write_bytes(
                legacy_key,
                &serde_json::to_vec(&NsFileV2::for_record(&record)).unwrap(),
            )
            .await
            .unwrap();

        let report = ns.migrate().await.unwrap().expect("migrated");
        assert_eq!(report.bound, vec!["mydb".to_string()]);
        let migrated = ns.lookup("mydb:main").await.unwrap().expect("bound");
        assert!(migrated.fence.is_some());
        assert_eq!(migrated.commit_t, 2);
        assert!(ns.storage.read_bytes(legacy_key).await.is_ok());
        assert!(ns.migrate().await.unwrap().is_none());
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
        fn permits_plaintext_cache(&self) -> bool {
            self.inner.permits_plaintext_cache()
        }

        fn encryption_admin(&self) -> Option<std::sync::Arc<dyn fluree_db_core::EncryptionAdmin>> {
            self.inner.encryption_admin()
        }

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
            namespace: &fluree_db_core::StorageNamespace,
            content_hash_hex: &str,
            bytes: &[u8],
        ) -> fluree_db_core::Result<fluree_db_core::ContentWriteResult> {
            fluree_db_core::ContentAddressedWrite::content_write_bytes_with_hash(
                &self.inner,
                kind,
                namespace,
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
        crate::testing::create(&ns, "mydb:main").await.unwrap();
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
        crate::testing::create(&ns, "mydb:main").await.unwrap();
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

    /// Enumeration takes identity from each record, never from its path:
    /// nested names, legacy `/` branches, and branches spelled like sidecar
    /// files all round-trip (a record missing here reads as unprotected to GC).
    #[tokio::test]
    async fn test_storage_ns_enumeration_round_trips_ambiguous_layouts() {
        let ns = make_storage_ns();
        for id in [
            "acme:main",
            "acme/inventory:main",
            "mydb:release/v1.0",
            "mydb:feature.index",
            "mydb:main.json",
        ] {
            crate::testing::create(&ns, id).await.unwrap();
            publish_commit(&ns, id, 1, &dummy_cid(id)).await;
        }

        let mut all: Vec<String> = ns
            .all_records()
            .await
            .unwrap()
            .into_iter()
            .map(|r| {
                assert_eq!(r.ledger_id, format!("{}:{}", r.name, r.branch));
                r.ledger_id.to_string()
            })
            .collect();
        all.sort();
        assert_eq!(
            all,
            [
                "acme/inventory:main",
                "acme:main",
                "mydb:feature.index",
                "mydb:main.json",
                "mydb:release/v1.0"
            ]
        );

        let acme: Vec<_> = ns.list_branches("acme").await.unwrap();
        assert_eq!(acme.len(), 1, "nested ledger is not a branch: {acme:?}");
        let mut mydb: Vec<String> = ns
            .list_branches("mydb")
            .await
            .unwrap()
            .into_iter()
            .map(|r| r.branch)
            .collect();
        mydb.sort();
        assert_eq!(mydb, ["feature.index", "main.json", "release/v1.0"]);

        // Same path, different identity: not this ledger.
        assert!(ns.lookup("mydb/release:v1.0").await.unwrap().is_none());
        assert!(ns.lookup("mydb:release/v1.0").await.unwrap().is_some());
    }

    #[tokio::test]
    async fn test_storage_ref_get_ref_after_publish() {
        let ns = make_storage_ns();
        crate::testing::create(&ns, "mydb:main").await.unwrap();
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

        crate::testing::create(&ns, "mydb:main").await.unwrap();

        let unborn = ns.get_ref("mydb:main", RefKind::CommitHead).await.unwrap();

        let result = ns
            .compare_and_set_ref("mydb:main", RefKind::CommitHead, unborn.as_ref(), &new_ref)
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
        crate::testing::create(&ns, "mydb:main").await.unwrap();
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
        crate::testing::create(&ns, "mydb:main").await.unwrap();
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
        crate::testing::create(&ns, "mydb:main").await.unwrap();
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
        crate::testing::create(&ns, "mydb:main").await.unwrap();
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
        crate::testing::create(&ns, "mydb:main").await.unwrap();
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
        crate::testing::create(&ns, "mydb:main").await.unwrap();
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
        crate::testing::create(&ns, "mydb:main").await.unwrap();
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
        crate::testing::create(&ns, "mydb:main").await.unwrap();
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
    async fn test_storage_heads_matches_lookup() {
        let ns = make_storage_ns();
        assert_eq!(ns.heads("mydb:main").await.unwrap(), None);

        crate::testing::create(&ns, "mydb:main").await.unwrap();
        publish_commit(&ns, "mydb:main", 5, &dummy_cid("commit-1")).await;
        let heads = ns.heads("mydb:main").await.unwrap().unwrap();
        assert_eq!(heads.commit.t, 5);
        assert_eq!(heads.index, RefValue { id: None, t: 0 });

        ns.publish_index("mydb:main", 3, &dummy_cid("index-1"))
            .await
            .unwrap();
        let heads = ns.heads("mydb:main").await.unwrap().unwrap();
        let record = ns.lookup("mydb:main").await.unwrap().unwrap();
        assert_eq!(heads, LedgerHeads::from_record(&record));
        assert_eq!(heads.index.id, Some(dummy_cid("index-1")));
        assert_eq!(heads.index.t, 3);
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
        crate::testing::create(&ns, "mydb:main").await.unwrap();
        let result = ns
            .compare_and_set_ref("mydb:main", RefKind::CommitHead, Some(&expected), &new_ref)
            .await
            .unwrap();
        match result {
            CasResult::Conflict { actual } => {
                assert!(actual.as_ref().is_none_or(|a| a.id.is_none()), "{actual:?}");
            }
            _ => panic!("expected conflict when ref doesn't exist"),
        }
    }

    // =========================================================================
    // StatusPublisher tests
    // =========================================================================

    /// Regression (#1369): a graph-source record shares the
    /// `ns@v3/{name}/{branch}.json` key space with ledger records but uses a
    /// different schema (no `f:ledger`). `lookup` must report it as a clean
    /// not-found (`Ok(None)`) instead of failing to deserialize `NsFileV2`
    /// ("missing field `f:ledger`"), so single-alias query/`use` resolution can
    /// fall back to graph-source resolution. Mirrors the file-backend twin.
    #[tokio::test]
    async fn test_storage_ns_lookup_skips_graph_source_record() {
        let ns = make_storage_ns();

        crate::testing::create(&ns, "realdb:main").await.unwrap();
        publish_commit(&ns, "realdb:main", 1, &dummy_cid("commit-1")).await;
        ns.publish_graph_source(
            "gs",
            "main",
            GraphSourceType::Iceberg,
            r#"{"catalog":"https://example.invalid","table":"ns.t"}"#,
            &["realdb:main".to_string()],
        )
        .await
        .unwrap();

        // The bug: lookup of a graph-source alias used to fail to deserialize the
        // ledger `NsFileV2`. It must now be a clean not-found.
        let result = ns.lookup("gs:main").await;
        assert!(
            matches!(result, Ok(None)),
            "lookup of a graph-source alias should be Ok(None), got {result:?}"
        );

        // The regular ledger still resolves, and the type-aware resolver still
        // classifies each correctly.
        assert!(ns.lookup("realdb:main").await.unwrap().is_some());
        assert!(matches!(
            ns.lookup_any("gs:main").await.unwrap(),
            NsLookupResult::GraphSource(_)
        ));
        assert!(matches!(
            ns.lookup_any("realdb:main").await.unwrap(),
            NsLookupResult::Ledger(_)
        ));
    }
}
