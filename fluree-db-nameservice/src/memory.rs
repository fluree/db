//! In-memory nameservice implementation for testing
//!
//! This implementation stores all records in memory using `Arc<RwLock>` for
//! interior mutability, making it thread-safe and suitable for multi-threaded
//! async runtimes.

use crate::{
    check_cas_expectation, ref_values_match, AdminPublisher, CasResult, ConfigCasResult,
    ConfigLookup, ConfigPublisher, ConfigValue, GraphSourceLookup, GraphSourcePublisher,
    GraphSourceRecord, GraphSourceType, NameService, NsLookupResult, NsRecord, Publisher, RefKind,
    RefLookup, RefPublisher, RefValue, Result, StatusCasResult, StatusLookup, StatusPayload,
    StatusPublisher, StatusValue,
};
use async_trait::async_trait;
use fluree_db_core::format_ledger_id;
use fluree_db_core::ledger_id as core_ledger_id;
use fluree_db_core::ContentId;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
/// In-memory nameservice for testing
///
/// Stores all records in a `HashMap` with `Arc<RwLock>` for interior mutability.
/// This implementation is thread-safe and suitable for multi-threaded runtimes.
#[derive(Clone)]
pub struct MemoryNameService {
    /// Ledger records keyed by canonical address (e.g., "mydb:main")
    records: Arc<RwLock<HashMap<String, NsRecord>>>,
    /// Graph source records keyed by canonical address (e.g., "my-search:main")
    graph_source_records: Arc<RwLock<HashMap<String, GraphSourceRecord>>>,
    /// Status values keyed by canonical address (v2 extension)
    status_values: Arc<RwLock<HashMap<String, StatusValue>>>,
    /// Config values keyed by canonical address (v2 extension)
    config_values: Arc<RwLock<HashMap<String, ConfigValue>>>,
}

impl Default for MemoryNameService {
    fn default() -> Self {
        Self {
            records: Arc::new(RwLock::new(HashMap::new())),
            graph_source_records: Arc::new(RwLock::new(HashMap::new())),
            status_values: Arc::new(RwLock::new(HashMap::new())),
            config_values: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl Debug for MemoryNameService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let records = self.records.read();
        let graph_source_records = self.graph_source_records.read();
        let status_values = self.status_values.read();
        let config_values = self.config_values.read();
        f.debug_struct("MemoryNameService")
            .field("record_count", &records.len())
            .field("graph_source_record_count", &graph_source_records.len())
            .field("status_count", &status_values.len())
            .field("config_count", &config_values.len())
            .finish()
    }
}

impl MemoryNameService {
    /// Create a new empty in-memory nameservice
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a record for a new ledger
    ///
    /// This is a convenience method for tests to bootstrap a ledger.
    pub fn create_ledger(&self, ledger_id: &str) -> Result<()> {
        let (ledger_name, branch) = core_ledger_id::split_ledger_id(ledger_id)?;
        let record = NsRecord::new(ledger_name, branch);
        self.records
            .write()
            .insert(record.ledger_id.clone(), record);
        Ok(())
    }

    /// Get a record by address (internal helper)
    fn get_record(&self, ledger_id: &str) -> Option<NsRecord> {
        // Try direct lookup first
        if let Some(record) = self.records.read().get(ledger_id).cloned() {
            return Some(record);
        }

        // Try with default branch
        let with_branch = match core_ledger_id::normalize_ledger_id(ledger_id) {
            Ok(value) => value,
            Err(_) => return None,
        };

        self.records.read().get(&with_branch).cloned()
    }

    /// Normalize ledger ID to canonical form
    fn normalize_ledger_id(&self, ledger_id: &str) -> String {
        core_ledger_id::normalize_ledger_id(ledger_id).unwrap_or_else(|_| ledger_id.to_string())
    }
}

#[async_trait]
impl NameService for MemoryNameService {
    async fn lookup(&self, ledger_id: &str) -> Result<Option<NsRecord>> {
        Ok(self.get_record(ledger_id))
    }

    async fn all_records(&self) -> Result<Vec<NsRecord>> {
        Ok(self.records.read().values().cloned().collect())
    }

    async fn list_branches(&self, ledger_name: &str) -> Result<Vec<NsRecord>> {
        Ok(self
            .records
            .read()
            .values()
            .filter(|r| r.name == ledger_name && !r.retracted)
            .cloned()
            .collect())
    }

    async fn create_branch(
        &self,
        ledger_name: &str,
        new_branch: &str,
        source_branch: &str,
        at_commit: Option<(ContentId, i64)>,
    ) -> Result<()> {
        let new_id = format_ledger_id(ledger_name, new_branch);
        let key = self.normalize_ledger_id(&new_id);

        let source_key = self.normalize_ledger_id(&format_ledger_id(ledger_name, source_branch));

        let mut records = self.records.write();

        if records.contains_key(&key) {
            return Err(crate::NameServiceError::ledger_already_exists(&key));
        }

        // Increment source branch's child count and pick the starting commit
        // head — either the caller-supplied historical commit or the source's
        // current HEAD.
        let source = records.get_mut(&source_key).ok_or_else(|| {
            crate::NameServiceError::not_found(format!(
                "source branch {ledger_name}:{source_branch}"
            ))
        })?;
        source.branches += 1;
        let (commit_head_id, commit_t) = match at_commit {
            Some((id, t)) => (Some(id), t),
            None => (source.commit_head_id.clone(), source.commit_t),
        };

        let mut record = NsRecord::new(ledger_name, new_branch);
        record.commit_head_id = commit_head_id;
        record.commit_t = commit_t;
        record.source_branch = Some(source_branch.to_string());
        records.insert(key, record);

        Ok(())
    }

    async fn drop_branch(&self, ledger_id: &str) -> Result<Option<u32>> {
        let key = self.normalize_ledger_id(ledger_id);
        let mut records = self.records.write();

        let record = records
            .remove(&key)
            .ok_or_else(|| crate::NameServiceError::not_found(&key))?;

        // Decrement parent's child count if this branch had a parent
        let parent_new_count = record.source_branch.as_ref().and_then(|source| {
            let parent_id = format_ledger_id(&record.name, source);
            let parent_key = self.normalize_ledger_id(&parent_id);
            let parent = records.get_mut(&parent_key)?;
            parent.branches = parent.branches.saturating_sub(1);
            Some(parent.branches)
        });

        Ok(parent_new_count)
    }

    async fn reset_head(&self, ledger_id: &str, snapshot: crate::NsRecordSnapshot) -> Result<()> {
        let key = self.normalize_ledger_id(ledger_id);
        let mut records = self.records.write();

        let record = records
            .get_mut(&key)
            .ok_or_else(|| crate::NameServiceError::not_found(&key))?;

        record.commit_head_id = snapshot.commit_head_id;
        record.commit_t = snapshot.commit_t;
        record.index_head_id = snapshot.index_head_id;
        record.index_t = snapshot.index_t;
        Ok(())
    }
}

#[async_trait]
impl Publisher for MemoryNameService {
    async fn publish_ledger_init(&self, ledger_id: &str) -> Result<()> {
        let key = self.normalize_ledger_id(ledger_id);

        // Check if record already exists — reject even if retracted (soft-dropped).
        // A hard drop removes the record entirely, which is required to reuse the alias.
        if self.records.read().contains_key(&key) {
            return Err(crate::NameServiceError::ledger_already_exists(&key));
        }

        // Create (or reset) to a fresh NsRecord
        let (ledger_name, branch) = core_ledger_id::split_ledger_id(ledger_id)?;
        let record = NsRecord::new(ledger_name, branch);
        self.records.write().insert(key, record);

        Ok(())
    }

    async fn publish_commit(
        &self,
        ledger_id: &str,
        commit_t: i64,
        commit_id: &ContentId,
    ) -> Result<()> {
        let key = self.normalize_ledger_id(ledger_id);
        let mut records = self.records.write();

        if let Some(record) = records.get_mut(&key) {
            // Only update if new_t > existing_t (strictly monotonic)
            if commit_t > record.commit_t {
                record.commit_head_id = Some(commit_id.clone());
                record.commit_t = commit_t;
            }
            // If commit_t <= existing, silently ignore (monotonic guarantee)
        } else {
            // Create new record
            let (ledger_name, branch) = core_ledger_id::split_ledger_id(ledger_id)?;
            let mut record = NsRecord::new(ledger_name, branch);
            record.commit_head_id = Some(commit_id.clone());
            record.commit_t = commit_t;
            records.insert(key, record);
        }

        Ok(())
    }

    async fn publish_index(
        &self,
        ledger_id: &str,
        index_t: i64,
        index_id: &ContentId,
    ) -> Result<()> {
        let key = self.normalize_ledger_id(ledger_id);
        let mut records = self.records.write();

        if let Some(record) = records.get_mut(&key) {
            // Only update if new_t > existing_t (strictly monotonic)
            if index_t > record.index_t {
                record.index_head_id = Some(index_id.clone());
                record.index_t = index_t;
            }
            // If index_t <= existing, silently ignore (monotonic guarantee)
        } else {
            // Create new record
            let (ledger_name, branch) = core_ledger_id::split_ledger_id(ledger_id)?;
            let mut record = NsRecord::new(ledger_name, branch);
            record.index_head_id = Some(index_id.clone());
            record.index_t = index_t;
            records.insert(key, record);
        }

        Ok(())
    }

    async fn retract(&self, ledger_id: &str) -> Result<()> {
        let key = self.normalize_ledger_id(ledger_id);
        let mut records = self.records.write();
        let mut did_update = false;

        if let Some(record) = records.get_mut(&key) {
            if !record.retracted {
                record.retracted = true;
                did_update = true;
            }
        }

        if did_update {
            // Advance status_v when retracting
            let mut status_values = self.status_values.write();
            let current_v = status_values.get(&key).map(|s| s.v).unwrap_or(1); // Default to 1 if no status exists
            status_values.insert(
                key,
                StatusValue::new(current_v + 1, StatusPayload::new("retracted")),
            );
        }
        Ok(())
    }

    async fn purge(&self, ledger_id: &str) -> Result<()> {
        let key = self.normalize_ledger_id(ledger_id);
        self.records.write().remove(&key);
        self.status_values.write().remove(&key);
        self.config_values.write().remove(&key);
        Ok(())
    }

    fn publishing_ledger_id(&self, ledger_id: &str) -> Option<String> {
        // Memory nameservice always returns the normalized ledger ID for publishing
        Some(self.normalize_ledger_id(ledger_id))
    }
}

#[async_trait]
impl AdminPublisher for MemoryNameService {
    async fn publish_index_allow_equal(
        &self,
        ledger_id: &str,
        index_t: i64,
        index_id: &ContentId,
    ) -> Result<()> {
        let key = self.normalize_ledger_id(ledger_id);
        let mut records = self.records.write();

        if let Some(record) = records.get_mut(&key) {
            // Allow update when new_t >= existing_t (not strictly monotonic)
            if index_t >= record.index_t {
                record.index_head_id = Some(index_id.clone());
                record.index_t = index_t;
            }
            // If index_t < existing, silently ignore (protect time-travel invariants)
        } else {
            // Create new record (same as publish_index)
            let (ledger_name, branch) = core_ledger_id::split_ledger_id(ledger_id)?;
            let mut record = NsRecord::new(ledger_name, branch);
            record.index_head_id = Some(index_id.clone());
            record.index_t = index_t;
            records.insert(key, record);
        }

        Ok(())
    }
}

#[async_trait]
impl RefLookup for MemoryNameService {
    async fn get_ref(&self, ledger_id: &str, kind: RefKind) -> Result<Option<RefValue>> {
        let key = self.normalize_ledger_id(ledger_id);
        let records = self.records.read();

        match records.get(&key) {
            None => Ok(None),
            Some(record) => match kind {
                RefKind::CommitHead => Ok(Some(RefValue {
                    id: record.commit_head_id.clone(),
                    t: record.commit_t,
                })),
                RefKind::IndexHead => Ok(Some(RefValue {
                    id: record.index_head_id.clone(),
                    t: record.index_t,
                })),
            },
        }
    }
}

#[async_trait]
impl RefPublisher for MemoryNameService {
    async fn compare_and_set_ref(
        &self,
        ledger_id: &str,
        kind: RefKind,
        expected: Option<&RefValue>,
        new: &RefValue,
    ) -> Result<CasResult> {
        let key = self.normalize_ledger_id(ledger_id);
        let mut records = self.records.write();

        let current_ref = records.get(&key).map(|r| match kind {
            RefKind::CommitHead => RefValue {
                id: r.commit_head_id.clone(),
                t: r.commit_t,
            },
            RefKind::IndexHead => RefValue {
                id: r.index_head_id.clone(),
                t: r.index_t,
            },
        });

        // Compare expected with current.
        match (&expected, &current_ref) {
            (None, None) => {
                // Creating a new ref — record must not exist yet.
                // Initialize the ledger record.
                let (ledger_name, branch) = core_ledger_id::split_ledger_id(ledger_id)?;
                let mut record = NsRecord::new(ledger_name, branch);
                match kind {
                    RefKind::CommitHead => {
                        record.commit_head_id = new.id.clone();
                        record.commit_t = new.t;
                    }
                    RefKind::IndexHead => {
                        record.index_head_id = new.id.clone();
                        record.index_t = new.t;
                    }
                }
                records.insert(key, record);
                return Ok(CasResult::Updated);
            }
            (None, Some(actual)) => {
                // Expected None but record exists — conflict.
                return Ok(CasResult::Conflict {
                    actual: Some(actual.clone()),
                });
            }
            (Some(_), None) => {
                // Expected a value but record doesn't exist — conflict.
                return Ok(CasResult::Conflict { actual: None });
            }
            (Some(exp), Some(actual)) => {
                if !ref_values_match(exp, actual) {
                    return Ok(CasResult::Conflict {
                        actual: Some(actual.clone()),
                    });
                }
            }
        }

        // Identity matches — check monotonic guard.
        let current = current_ref.as_ref().unwrap();
        let guard_ok = match kind {
            RefKind::CommitHead => new.t > current.t,
            RefKind::IndexHead => new.t >= current.t,
        };
        if !guard_ok {
            return Ok(CasResult::Conflict {
                actual: Some(current.clone()),
            });
        }

        // Apply the update.
        let record = records.get_mut(&key).unwrap();
        match kind {
            RefKind::CommitHead => {
                record.commit_head_id = new.id.clone();
                record.commit_t = new.t;
            }
            RefKind::IndexHead => {
                record.index_head_id = new.id.clone();
                record.index_t = new.t;
            }
        }

        Ok(CasResult::Updated)
    }
}

#[async_trait]
impl GraphSourcePublisher for MemoryNameService {
    async fn publish_graph_source(
        &self,
        name: &str,
        branch: &str,
        source_type: GraphSourceType,
        config: &str,
        dependencies: &[String],
    ) -> Result<()> {
        let key = core_ledger_id::format_ledger_id(name, branch);
        let mut graph_source_records = self.graph_source_records.write();

        if let Some(record) = graph_source_records.get_mut(&key) {
            // Update config but preserve retracted status if already set
            record.source_type = source_type.clone();
            record.config = config.to_string();
            record.dependencies = dependencies.to_vec();
        } else {
            // Create new graph source record
            let record = GraphSourceRecord::new(
                name,
                branch,
                source_type.clone(),
                config,
                dependencies.to_vec(),
            );
            graph_source_records.insert(key, record);
        }

        Ok(())
    }

    async fn publish_graph_source_index(
        &self,
        name: &str,
        branch: &str,
        index_id: &ContentId,
        index_t: i64,
    ) -> Result<()> {
        let key = core_ledger_id::format_ledger_id(name, branch);
        let mut graph_source_records = self.graph_source_records.write();

        if let Some(record) = graph_source_records.get_mut(&key) {
            // Strictly monotonic: only update if new_t > existing_t
            if index_t > record.index_t {
                record.index_id = Some(index_id.clone());
                record.index_t = index_t;
            }
        }
        // If graph source doesn't exist, silently ignore (index requires config first)

        Ok(())
    }

    async fn retract_graph_source(&self, name: &str, branch: &str) -> Result<()> {
        let key = core_ledger_id::format_ledger_id(name, branch);
        let mut graph_source_records = self.graph_source_records.write();

        if let Some(record) = graph_source_records.get_mut(&key) {
            if !record.retracted {
                record.retracted = true;
            }
        }

        Ok(())
    }
}

#[async_trait]
impl GraphSourceLookup for MemoryNameService {
    async fn lookup_graph_source(
        &self,
        graph_source_id: &str,
    ) -> Result<Option<GraphSourceRecord>> {
        let key = self.normalize_ledger_id(graph_source_id);
        Ok(self.graph_source_records.read().get(&key).cloned())
    }

    async fn lookup_any(&self, resource_id: &str) -> Result<NsLookupResult> {
        let key = self.normalize_ledger_id(resource_id);

        if let Some(record) = self.graph_source_records.read().get(&key).cloned() {
            return Ok(NsLookupResult::GraphSource(record));
        }

        if let Some(record) = self.records.read().get(&key).cloned() {
            return Ok(NsLookupResult::Ledger(record));
        }

        Ok(NsLookupResult::NotFound)
    }

    async fn all_graph_source_records(&self) -> Result<Vec<GraphSourceRecord>> {
        Ok(self.graph_source_records.read().values().cloned().collect())
    }
}

#[async_trait]
impl StatusLookup for MemoryNameService {
    async fn get_status(&self, ledger_id: &str) -> Result<Option<StatusValue>> {
        let key = self.normalize_ledger_id(ledger_id);
        let status_values = self.status_values.read();

        // If status exists, return it
        if let Some(status) = status_values.get(&key).cloned() {
            return Ok(Some(status));
        }

        // If the ledger record exists but no status, return initial status (v=1)
        let records = self.records.read();
        if records.contains_key(&key) {
            return Ok(Some(StatusValue::initial()));
        }

        // No record exists
        Ok(None)
    }
}

#[async_trait]
impl StatusPublisher for MemoryNameService {
    async fn push_status(
        &self,
        ledger_id: &str,
        expected: Option<&StatusValue>,
        new: &StatusValue,
    ) -> Result<StatusCasResult> {
        let key = self.normalize_ledger_id(ledger_id);

        // Get current status (or initial if record exists but no status)
        let current = {
            let status_values = self.status_values.read();
            let records = self.records.read();

            if let Some(status) = status_values.get(&key).cloned() {
                Some(status)
            } else if records.contains_key(&key) {
                // Record exists but no status → treat as initial
                Some(StatusValue::initial())
            } else {
                None
            }
        };

        if let Some(conflict) = check_cas_expectation(
            &expected.cloned(),
            &current,
            false,
            |exp, actual| exp.v == actual.v && exp.payload == actual.payload,
            |actual| StatusCasResult::Conflict { actual },
        ) {
            return Ok(conflict);
        }

        // Validate monotonic constraint: new.v > current.v
        let current_v = current.as_ref().map(|c| c.v).unwrap_or(0);
        if new.v <= current_v {
            return Ok(StatusCasResult::Conflict { actual: current });
        }

        // Apply the update
        self.status_values.write().insert(key, new.clone());

        Ok(StatusCasResult::Updated)
    }
}

#[async_trait]
impl ConfigLookup for MemoryNameService {
    async fn get_config(&self, ledger_id: &str) -> Result<Option<ConfigValue>> {
        let key = self.normalize_ledger_id(ledger_id);
        let config_values = self.config_values.read();

        // If config exists, return it
        if let Some(config) = config_values.get(&key).cloned() {
            return Ok(Some(config));
        }

        // If the ledger record exists but no config, return unborn config (v=0)
        let records = self.records.read();
        if records.contains_key(&key) {
            return Ok(Some(ConfigValue::unborn()));
        }

        // No record exists
        Ok(None)
    }
}

#[async_trait]
impl ConfigPublisher for MemoryNameService {
    async fn push_config(
        &self,
        ledger_id: &str,
        expected: Option<&ConfigValue>,
        new: &ConfigValue,
    ) -> Result<ConfigCasResult> {
        let key = self.normalize_ledger_id(ledger_id);

        // Get current config (or unborn if record exists but no config)
        let current = {
            let config_values = self.config_values.read();
            let records = self.records.read();

            if let Some(config) = config_values.get(&key).cloned() {
                Some(config)
            } else if records.contains_key(&key) {
                // Record exists but no config → treat as unborn
                Some(ConfigValue::unborn())
            } else {
                None
            }
        };

        if let Some(conflict) = check_cas_expectation(
            &expected.cloned(),
            &current,
            false,
            |exp, actual| exp.v == actual.v && exp.payload == actual.payload,
            |actual| ConfigCasResult::Conflict { actual },
        ) {
            return Ok(conflict);
        }

        // Validate monotonic constraint: new.v > current.v
        let current_v = current.as_ref().map(|c| c.v).unwrap_or(0);
        if new.v <= current_v {
            return Ok(ConfigCasResult::Conflict { actual: current });
        }

        // Apply the update to config_values
        self.config_values.write().insert(key.clone(), new.clone());

        // Sync default_context and config_id to NsRecord
        let new_default_context = new.payload.as_ref().and_then(|p| p.default_context.clone());
        let new_config_id = new.payload.as_ref().and_then(|p| p.config_id.clone());
        if let Some(record) = self.records.write().get_mut(&key) {
            record.default_context = new_default_context;
            record.config_id = new_config_id;
        }

        Ok(ConfigCasResult::Updated)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ConfigPayload, StatusPayload};
    use fluree_db_core::ContentKind;

    fn test_commit_id(label: &str) -> ContentId {
        ContentId::new(ContentKind::Commit, label.as_bytes())
    }

    fn test_index_id(label: &str) -> ContentId {
        ContentId::new(ContentKind::IndexRoot, label.as_bytes())
    }

    #[tokio::test]
    async fn test_memory_ns_publish_commit() {
        let ns = MemoryNameService::new();

        // First publish
        ns.publish_commit("mydb:main", 1, &test_commit_id("commit-1"))
            .await
            .unwrap();

        let record = ns.lookup("mydb:main").await.unwrap().unwrap();
        assert_eq!(record.commit_head_id, Some(test_commit_id("commit-1")));
        assert_eq!(record.commit_t, 1);

        // Higher t should update
        ns.publish_commit("mydb:main", 5, &test_commit_id("commit-2"))
            .await
            .unwrap();

        let record = ns.lookup("mydb:main").await.unwrap().unwrap();
        assert_eq!(record.commit_head_id, Some(test_commit_id("commit-2")));
        assert_eq!(record.commit_t, 5);

        // Lower t should be ignored (monotonic)
        ns.publish_commit("mydb:main", 3, &test_commit_id("commit-old"))
            .await
            .unwrap();

        let record = ns.lookup("mydb:main").await.unwrap().unwrap();
        assert_eq!(record.commit_head_id, Some(test_commit_id("commit-2")));
        assert_eq!(record.commit_t, 5);
    }

    #[tokio::test]
    async fn test_memory_ns_publish_index_separate() {
        let ns = MemoryNameService::new();

        // Publish commit first
        ns.publish_commit("mydb:main", 10, &test_commit_id("commit-1"))
            .await
            .unwrap();

        // Publish index (can lag behind commit)
        ns.publish_index("mydb:main", 5, &test_index_id("index-1"))
            .await
            .unwrap();

        let record = ns.lookup("mydb:main").await.unwrap().unwrap();
        assert_eq!(record.commit_t, 10);
        assert_eq!(record.index_t, 5);
        assert!(record.has_novelty()); // commit_t > index_t
    }

    #[tokio::test]
    async fn test_memory_ns_lookup_default_branch() {
        let ns = MemoryNameService::new();

        ns.publish_commit("mydb:main", 1, &test_commit_id("commit-1"))
            .await
            .unwrap();

        // Lookup without branch should find default
        let record = ns.lookup("mydb").await.unwrap();
        assert!(record.is_some());

        // Lookup with branch should also work
        let record = ns.lookup("mydb:main").await.unwrap();
        assert!(record.is_some());
    }

    #[tokio::test]
    async fn test_memory_ns_retract() {
        let ns = MemoryNameService::new();

        ns.publish_commit("mydb:main", 1, &test_commit_id("commit-1"))
            .await
            .unwrap();

        let record = ns.lookup("mydb:main").await.unwrap().unwrap();
        assert!(!record.retracted);

        ns.retract("mydb:main").await.unwrap();

        let record = ns.lookup("mydb:main").await.unwrap().unwrap();
        assert!(record.retracted);
    }

    #[tokio::test]
    async fn test_memory_ns_all_records() {
        let ns = MemoryNameService::new();

        ns.publish_commit("db1:main", 1, &test_commit_id("commit-1"))
            .await
            .unwrap();
        ns.publish_commit("db2:main", 1, &test_commit_id("commit-2"))
            .await
            .unwrap();
        ns.publish_commit("db3:dev", 1, &test_commit_id("commit-3"))
            .await
            .unwrap();

        let records = ns.all_records().await.unwrap();
        assert_eq!(records.len(), 3);
    }

    #[tokio::test]
    async fn test_memory_ns_publishing_ledger_id() {
        let ns = MemoryNameService::new();

        assert_eq!(
            ns.publishing_ledger_id("mydb"),
            Some("mydb:main".to_string())
        );
        assert_eq!(
            ns.publishing_ledger_id("mydb:dev"),
            Some("mydb:dev".to_string())
        );
    }

    // ========== Graph Source Tests ==========

    #[tokio::test]
    async fn test_memory_graph_source_publish_and_lookup() {
        let ns = MemoryNameService::new();

        ns.publish_graph_source(
            "my-search",
            "main",
            GraphSourceType::Bm25,
            r#"{"k1":1.2}"#,
            &["source:main".to_string()],
        )
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
        assert_eq!(record.config, r#"{"k1":1.2}"#);
        assert!(!record.retracted);
    }

    #[tokio::test]
    async fn test_memory_graph_source_index_monotonic() {
        let ns = MemoryNameService::new();

        ns.publish_graph_source("gs", "main", GraphSourceType::Bm25, "{}", &[])
            .await
            .unwrap();

        let id_v1 = ContentId::new(ContentKind::IndexRoot, b"index-v1");
        let id_v2 = ContentId::new(ContentKind::IndexRoot, b"index-v2");
        let id_old = ContentId::new(ContentKind::IndexRoot, b"index-old");

        ns.publish_graph_source_index("gs", "main", &id_v1, 10)
            .await
            .unwrap();

        ns.publish_graph_source_index("gs", "main", &id_v2, 20)
            .await
            .unwrap();

        let record = ns.lookup_graph_source("gs:main").await.unwrap().unwrap();
        assert_eq!(record.index_id, Some(id_v2.clone()));
        assert_eq!(record.index_t, 20);

        // Lower t should be ignored
        ns.publish_graph_source_index("gs", "main", &id_old, 15)
            .await
            .unwrap();

        let record = ns.lookup_graph_source("gs:main").await.unwrap().unwrap();
        assert_eq!(record.index_id, Some(id_v2));
        assert_eq!(record.index_t, 20);
    }

    #[tokio::test]
    async fn test_memory_graph_source_retract() {
        let ns = MemoryNameService::new();

        ns.publish_graph_source("gs", "main", GraphSourceType::Bm25, "{}", &[])
            .await
            .unwrap();

        ns.retract_graph_source("gs", "main").await.unwrap();

        let record = ns.lookup_graph_source("gs:main").await.unwrap().unwrap();
        assert!(record.retracted);
    }

    #[tokio::test]
    async fn test_memory_graph_source_lookup_any() {
        let ns = MemoryNameService::new();

        ns.publish_commit("ledger:main", 1, &test_commit_id("commit-1"))
            .await
            .unwrap();
        ns.publish_graph_source("gs", "main", GraphSourceType::Bm25, "{}", &[])
            .await
            .unwrap();

        match ns.lookup_any("ledger:main").await.unwrap() {
            NsLookupResult::Ledger(r) => assert_eq!(r.name, "ledger"),
            other => panic!("Expected Ledger, got {other:?}"),
        }

        match ns.lookup_any("gs:main").await.unwrap() {
            NsLookupResult::GraphSource(r) => assert_eq!(r.name, "gs"),
            other => panic!("Expected GraphSource, got {other:?}"),
        }

        match ns.lookup_any("nonexistent:main").await.unwrap() {
            NsLookupResult::NotFound => {}
            other => panic!("Expected NotFound, got {other:?}"),
        }
    }

    // =========================================================================
    // RefPublisher tests
    // =========================================================================

    #[tokio::test]
    async fn test_ref_get_ref_unknown_alias() {
        let ns = MemoryNameService::new();
        let result = ns
            .get_ref("nonexistent:main", RefKind::CommitHead)
            .await
            .unwrap();
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn test_ref_get_ref_after_publish() {
        let ns = MemoryNameService::new();
        ns.publish_commit("mydb:main", 5, &test_commit_id("commit-1"))
            .await
            .unwrap();

        let commit = ns
            .get_ref("mydb:main", RefKind::CommitHead)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(commit.id, Some(test_commit_id("commit-1")));
        assert_eq!(commit.t, 5);

        // Index should exist but be at t=0 with no id (unborn-like)
        let index = ns
            .get_ref("mydb:main", RefKind::IndexHead)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(index.id, None);
        assert_eq!(index.t, 0);
    }

    #[tokio::test]
    async fn test_ref_cas_create_new() {
        let ns = MemoryNameService::new();
        let new_ref = RefValue {
            id: Some(test_commit_id("commit-1")),
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
        assert_eq!(current.id, Some(test_commit_id("commit-1")));
        assert_eq!(current.t, 1);
    }

    #[tokio::test]
    async fn test_ref_cas_conflict_already_exists() {
        let ns = MemoryNameService::new();
        ns.publish_commit("mydb:main", 1, &test_commit_id("commit-1"))
            .await
            .unwrap();

        let new_ref = RefValue {
            id: Some(test_commit_id("commit-2")),
            t: 2,
        };
        // expected=None but record exists → conflict
        let result = ns
            .compare_and_set_ref("mydb:main", RefKind::CommitHead, None, &new_ref)
            .await
            .unwrap();
        match result {
            CasResult::Conflict { actual } => {
                let a = actual.unwrap();
                assert_eq!(a.id, Some(test_commit_id("commit-1")));
                assert_eq!(a.t, 1);
            }
            _ => panic!("expected conflict"),
        }
    }

    #[tokio::test]
    async fn test_ref_cas_conflict_id_mismatch() {
        let ns = MemoryNameService::new();
        ns.publish_commit("mydb:main", 1, &test_commit_id("commit-1"))
            .await
            .unwrap();

        let expected = RefValue {
            id: Some(test_commit_id("wrong-id")),
            t: 1,
        };
        let new_ref = RefValue {
            id: Some(test_commit_id("commit-2")),
            t: 2,
        };
        let result = ns
            .compare_and_set_ref("mydb:main", RefKind::CommitHead, Some(&expected), &new_ref)
            .await
            .unwrap();
        match result {
            CasResult::Conflict { actual } => {
                assert_eq!(actual.unwrap().id, Some(test_commit_id("commit-1")));
            }
            _ => panic!("expected conflict"),
        }
    }

    #[tokio::test]
    async fn test_ref_cas_success_id_matches() {
        let ns = MemoryNameService::new();
        ns.publish_commit("mydb:main", 1, &test_commit_id("commit-1"))
            .await
            .unwrap();

        let expected = RefValue {
            id: Some(test_commit_id("commit-1")),
            t: 1,
        };
        let new_ref = RefValue {
            id: Some(test_commit_id("commit-2")),
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
        assert_eq!(current.id, Some(test_commit_id("commit-2")));
        assert_eq!(current.t, 2);
    }

    #[tokio::test]
    async fn test_ref_cas_commit_strict_monotonic() {
        let ns = MemoryNameService::new();
        ns.publish_commit("mydb:main", 5, &test_commit_id("commit-1"))
            .await
            .unwrap();

        let expected = RefValue {
            id: Some(test_commit_id("commit-1")),
            t: 5,
        };
        // Same t should fail (strict for CommitHead)
        let new_ref = RefValue {
            id: Some(test_commit_id("commit-2")),
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

        // Lower t should also fail
        let new_ref = RefValue {
            id: Some(test_commit_id("commit-2")),
            t: 3,
        };
        let result = ns
            .compare_and_set_ref("mydb:main", RefKind::CommitHead, Some(&expected), &new_ref)
            .await
            .unwrap();
        match result {
            CasResult::Conflict { .. } => {}
            _ => panic!("expected conflict for lower t on CommitHead"),
        }
    }

    #[tokio::test]
    async fn test_ref_cas_index_allows_equal_t() {
        let ns = MemoryNameService::new();
        ns.publish_commit("mydb:main", 5, &test_commit_id("commit-1"))
            .await
            .unwrap();
        ns.publish_index("mydb:main", 5, &test_index_id("index-1"))
            .await
            .unwrap();

        let expected = RefValue {
            id: Some(test_index_id("index-1")),
            t: 5,
        };
        // Same t should succeed for IndexHead (non-strict)
        let new_ref = RefValue {
            id: Some(test_index_id("index-2")),
            t: 5,
        };
        let result = ns
            .compare_and_set_ref("mydb:main", RefKind::IndexHead, Some(&expected), &new_ref)
            .await
            .unwrap();
        assert_eq!(result, CasResult::Updated);
    }

    #[tokio::test]
    async fn test_ref_fast_forward_commit_success() {
        let ns = MemoryNameService::new();
        ns.publish_commit("mydb:main", 1, &test_commit_id("commit-1"))
            .await
            .unwrap();

        let new_ref = RefValue {
            id: Some(test_commit_id("commit-5")),
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
    async fn test_ref_fast_forward_commit_rejected_stale() {
        let ns = MemoryNameService::new();
        ns.publish_commit("mydb:main", 10, &test_commit_id("commit-1"))
            .await
            .unwrap();

        let new_ref = RefValue {
            id: Some(test_commit_id("commit-old")),
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
            _ => panic!("expected conflict for stale fast-forward"),
        }
    }

    #[tokio::test]
    async fn test_ref_cas_expected_some_but_missing() {
        let ns = MemoryNameService::new();
        let expected = RefValue {
            id: Some(test_commit_id("commit-1")),
            t: 1,
        };
        let new_ref = RefValue {
            id: Some(test_commit_id("commit-2")),
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
    async fn test_status_get_nonexistent() {
        let ns = MemoryNameService::new();
        let result = ns.get_status("nonexistent:main").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_status_get_initial() {
        let ns = MemoryNameService::new();
        ns.publish_ledger_init("mydb:main").await.unwrap();

        let status = ns.get_status("mydb:main").await.unwrap().unwrap();
        assert_eq!(status.v, 1);
        assert_eq!(status.payload.state, "ready");
    }

    #[tokio::test]
    async fn test_status_push_update() {
        let ns = MemoryNameService::new();
        ns.publish_ledger_init("mydb:main").await.unwrap();

        // Get initial status
        let initial = ns.get_status("mydb:main").await.unwrap().unwrap();
        assert_eq!(initial.v, 1);

        // Push new status
        let new_status = StatusValue::new(2, StatusPayload::new("ready"));
        let result = ns
            .push_status("mydb:main", Some(&initial), &new_status)
            .await
            .unwrap();
        assert!(matches!(result, StatusCasResult::Updated));

        // Verify update
        let current = ns.get_status("mydb:main").await.unwrap().unwrap();
        assert_eq!(current.v, 2);
        assert_eq!(current.payload.state, "ready");
    }

    #[tokio::test]
    async fn test_status_push_conflict_wrong_expected() {
        let ns = MemoryNameService::new();
        ns.publish_ledger_init("mydb:main").await.unwrap();

        // Try to push with wrong expected value
        let wrong_expected = StatusValue::new(5, StatusPayload::new("wrong"));
        let new_status = StatusValue::new(6, StatusPayload::new("ready"));
        let result = ns
            .push_status("mydb:main", Some(&wrong_expected), &new_status)
            .await
            .unwrap();

        match result {
            StatusCasResult::Conflict { actual } => {
                let a = actual.unwrap();
                assert_eq!(a.v, 1); // Initial status
                assert_eq!(a.payload.state, "ready");
            }
            _ => panic!("expected conflict"),
        }
    }

    #[tokio::test]
    async fn test_status_push_conflict_non_monotonic() {
        let ns = MemoryNameService::new();
        ns.publish_ledger_init("mydb:main").await.unwrap();

        let initial = ns.get_status("mydb:main").await.unwrap().unwrap();

        // Push with v=2 (valid)
        let status_v2 = StatusValue::new(2, StatusPayload::new("ready"));
        ns.push_status("mydb:main", Some(&initial), &status_v2)
            .await
            .unwrap();

        // Try to push with v=1 (non-monotonic)
        let status_v1 = StatusValue::new(1, StatusPayload::new("old"));
        let result = ns
            .push_status("mydb:main", Some(&status_v2), &status_v1)
            .await
            .unwrap();

        match result {
            StatusCasResult::Conflict { actual } => {
                assert_eq!(actual.unwrap().v, 2);
            }
            _ => panic!("expected conflict for non-monotonic update"),
        }
    }

    #[tokio::test]
    async fn test_status_push_with_extra_metadata() {
        let ns = MemoryNameService::new();
        ns.publish_ledger_init("mydb:main").await.unwrap();

        let initial = ns.get_status("mydb:main").await.unwrap().unwrap();

        // Push status with extra metadata
        let mut extra = std::collections::HashMap::new();
        extra.insert("queue_depth".to_string(), serde_json::json!(5));
        extra.insert("last_commit_ms".to_string(), serde_json::json!(42));

        let new_status = StatusValue::new(2, StatusPayload::with_extra("indexing", extra));
        ns.push_status("mydb:main", Some(&initial), &new_status)
            .await
            .unwrap();

        let current = ns.get_status("mydb:main").await.unwrap().unwrap();
        assert_eq!(current.payload.state, "indexing");
        assert_eq!(
            current.payload.extra.get("queue_depth"),
            Some(&serde_json::json!(5))
        );
    }

    // =========================================================================
    // ConfigPublisher tests
    // =========================================================================

    #[tokio::test]
    async fn test_config_get_nonexistent() {
        let ns = MemoryNameService::new();
        let result = ns.get_config("nonexistent:main").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_config_get_unborn() {
        let ns = MemoryNameService::new();
        ns.publish_ledger_init("mydb:main").await.unwrap();

        let config = ns.get_config("mydb:main").await.unwrap().unwrap();
        assert!(config.is_unborn());
        assert_eq!(config.v, 0);
        assert!(config.payload.is_none());
    }

    #[tokio::test]
    async fn test_config_push_from_unborn() {
        let ns = MemoryNameService::new();
        ns.publish_ledger_init("mydb:main").await.unwrap();

        let unborn = ns.get_config("mydb:main").await.unwrap().unwrap();
        assert!(unborn.is_unborn());

        // Push first config
        let ctx_cid_v1 = ContentId::new(fluree_db_core::ContentKind::LedgerConfig, b"test-ctx-v1");
        let new_config = ConfigValue::new(
            1,
            Some(ConfigPayload::with_default_context(ctx_cid_v1.clone())),
        );
        let result = ns
            .push_config("mydb:main", Some(&unborn), &new_config)
            .await
            .unwrap();
        assert!(matches!(result, ConfigCasResult::Updated));

        // Verify update
        let current = ns.get_config("mydb:main").await.unwrap().unwrap();
        assert!(!current.is_unborn());
        assert_eq!(current.v, 1);
        assert_eq!(
            current.payload.as_ref().unwrap().default_context,
            Some(ctx_cid_v1.clone())
        );
    }

    #[tokio::test]
    async fn test_config_push_update() {
        let ns = MemoryNameService::new();
        ns.publish_ledger_init("mydb:main").await.unwrap();

        let unborn = ns.get_config("mydb:main").await.unwrap().unwrap();

        // Push first config
        let ctx_cid_v1 = ContentId::new(fluree_db_core::ContentKind::LedgerConfig, b"test-ctx-v1");
        let config_v1 = ConfigValue::new(
            1,
            Some(ConfigPayload::with_default_context(ctx_cid_v1.clone())),
        );
        ns.push_config("mydb:main", Some(&unborn), &config_v1)
            .await
            .unwrap();

        // Push updated config
        let ctx_cid_v2 = ContentId::new(fluree_db_core::ContentKind::LedgerConfig, b"test-ctx-v2");
        let config_v2 = ConfigValue::new(
            2,
            Some(ConfigPayload::with_default_context(ctx_cid_v2.clone())),
        );
        let result = ns
            .push_config("mydb:main", Some(&config_v1), &config_v2)
            .await
            .unwrap();
        assert!(matches!(result, ConfigCasResult::Updated));

        let current = ns.get_config("mydb:main").await.unwrap().unwrap();
        assert_eq!(current.v, 2);
        assert_eq!(
            current.payload.as_ref().unwrap().default_context,
            Some(ctx_cid_v2)
        );
    }

    #[tokio::test]
    async fn test_config_push_conflict_wrong_expected() {
        let ns = MemoryNameService::new();
        ns.publish_ledger_init("mydb:main").await.unwrap();

        // Try to push with wrong expected value
        let wrong_expected = ConfigValue::new(5, Some(ConfigPayload::new()));
        let new_config = ConfigValue::new(6, Some(ConfigPayload::new()));
        let result = ns
            .push_config("mydb:main", Some(&wrong_expected), &new_config)
            .await
            .unwrap();

        match result {
            ConfigCasResult::Conflict { actual } => {
                let a = actual.unwrap();
                assert!(a.is_unborn()); // Was unborn
            }
            _ => panic!("expected conflict"),
        }
    }

    #[tokio::test]
    async fn test_config_push_conflict_non_monotonic() {
        let ns = MemoryNameService::new();
        ns.publish_ledger_init("mydb:main").await.unwrap();

        let unborn = ns.get_config("mydb:main").await.unwrap().unwrap();

        // Push v=1
        let config_v1 = ConfigValue::new(1, Some(ConfigPayload::new()));
        ns.push_config("mydb:main", Some(&unborn), &config_v1)
            .await
            .unwrap();

        // Push v=2
        let config_v2 = ConfigValue::new(2, Some(ConfigPayload::new()));
        ns.push_config("mydb:main", Some(&config_v1), &config_v2)
            .await
            .unwrap();

        // Try to push v=1 (non-monotonic)
        let config_old = ConfigValue::new(1, Some(ConfigPayload::new()));
        let result = ns
            .push_config("mydb:main", Some(&config_v2), &config_old)
            .await
            .unwrap();

        match result {
            ConfigCasResult::Conflict { actual } => {
                assert_eq!(actual.unwrap().v, 2);
            }
            _ => panic!("expected conflict for non-monotonic update"),
        }
    }

    #[tokio::test]
    async fn test_config_push_no_record() {
        let ns = MemoryNameService::new();

        // Try to push config without ledger record
        let new_config = ConfigValue::new(1, Some(ConfigPayload::new()));
        let result = ns
            .push_config("nonexistent:main", None, &new_config)
            .await
            .unwrap();

        match result {
            ConfigCasResult::Conflict { actual } => {
                assert!(actual.is_none());
            }
            _ => panic!("expected conflict when no record exists"),
        }
    }

    // =========================================================================
    // Branch tests
    // =========================================================================

    #[tokio::test]
    async fn test_create_branch_from_main() {
        let ns = MemoryNameService::new();
        ns.publish_ledger_init("mydb:main").await.unwrap();
        let cid = test_commit_id("commit-5");
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
    async fn test_create_branch_duplicate_fails() {
        let ns = MemoryNameService::new();
        ns.publish_ledger_init("mydb:main").await.unwrap();
        let cid = test_commit_id("commit-1");
        ns.publish_commit("mydb:main", 1, &cid).await.unwrap();

        ns.create_branch("mydb", "dev", "main", None).await.unwrap();

        let result = ns.create_branch("mydb", "dev", "main", None).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_list_branches() {
        let ns = MemoryNameService::new();
        ns.publish_ledger_init("mydb:main").await.unwrap();
        let cid = test_commit_id("commit-3");
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
    async fn test_list_branches_unknown_ledger() {
        let ns = MemoryNameService::new();
        let branches = ns.list_branches("nonexistent").await.unwrap();
        assert!(branches.is_empty());
    }

    #[tokio::test]
    async fn test_list_branches_excludes_retracted() {
        let ns = MemoryNameService::new();
        ns.publish_ledger_init("mydb:main").await.unwrap();
        let cid = test_commit_id("commit-1");
        ns.publish_commit("mydb:main", 1, &cid).await.unwrap();

        ns.create_branch("mydb", "dead", "main", None)
            .await
            .unwrap();
        ns.retract("mydb:dead").await.unwrap();

        let branches = ns.list_branches("mydb").await.unwrap();
        assert_eq!(branches.len(), 1);
        assert_eq!(branches[0].branch, "main");
    }
}
