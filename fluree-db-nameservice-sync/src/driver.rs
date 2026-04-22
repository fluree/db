//! Sync driver orchestrating fetch/pull/push operations
//!
//! The `SyncDriver` coordinates between local nameservice state, remote
//! nameservice clients, and tracking stores to provide git-like sync semantics.

use crate::client::RemoteNameserviceClient;
use crate::config::SyncConfigStore;
use crate::error::{Result, SyncError};
use fluree_db_nameservice::{
    CasResult, RefKind, RefPublisher, RefValue, RemoteName, RemoteTrackingStore, TrackingRecord,
};
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

/// Result of a fetch operation
#[derive(Debug)]
pub struct FetchResult {
    /// Ledger IDs whose tracking refs were updated
    pub updated: Vec<(String, TrackingRecord)>,
    /// Ledger IDs that were unchanged
    pub unchanged: Vec<String>,
}

/// Result of a pull operation
#[derive(Debug)]
pub enum PullResult {
    /// Local was fast-forwarded to match remote
    FastForwarded {
        ledger_id: String,
        from: RefValue,
        to: RefValue,
    },
    /// Already up to date
    Current { ledger_id: String },
    /// Cannot fast-forward (local has commits remote does not)
    Diverged {
        ledger_id: String,
        local: RefValue,
        remote: RefValue,
    },
    /// No upstream configured for this ledger ID
    NoUpstream { ledger_id: String },
    /// No tracking data available (need to fetch first)
    NoTracking { ledger_id: String },
}

/// Result of a push operation
#[derive(Debug)]
pub enum PushResult {
    /// Push succeeded
    Pushed { ledger_id: String, value: RefValue },
    /// Remote rejected (CAS conflict)
    Rejected {
        ledger_id: String,
        local: RefValue,
        remote: RefValue,
    },
    /// No upstream configured for this ledger ID
    NoUpstream { ledger_id: String },
}

/// Orchestrates sync operations between local and remote nameservices
pub struct SyncDriver {
    /// Local nameservice ref operations
    local: Arc<dyn RefPublisher>,
    /// Remote tracking store
    tracking: Arc<dyn RemoteTrackingStore>,
    /// Sync configuration
    config: Arc<dyn SyncConfigStore>,
    /// Remote clients keyed by remote name
    clients: HashMap<String, Arc<dyn RemoteNameserviceClient>>,
}

impl Debug for SyncDriver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SyncDriver")
            .field("clients", &self.clients.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl SyncDriver {
    /// Create a new sync driver
    pub fn new(
        local: Arc<dyn RefPublisher>,
        tracking: Arc<dyn RemoteTrackingStore>,
        config: Arc<dyn SyncConfigStore>,
    ) -> Self {
        Self {
            local,
            tracking,
            config,
            clients: HashMap::new(),
        }
    }

    /// Register a client for a remote
    pub fn add_client(&mut self, remote: &RemoteName, client: Arc<dyn RemoteNameserviceClient>) {
        self.clients.insert(remote.as_str().to_string(), client);
    }

    fn get_client(&self, remote: &RemoteName) -> Result<&Arc<dyn RemoteNameserviceClient>> {
        self.clients
            .get(remote.as_str())
            .ok_or_else(|| SyncError::Config(format!("No client for remote '{remote}'")))
    }

    /// Fetch all records from a remote and update tracking refs.
    ///
    /// This is analogous to `git fetch <remote>`. It does NOT modify local refs.
    pub async fn fetch_remote(&self, remote: &RemoteName) -> Result<FetchResult> {
        let client = self.get_client(remote)?;
        let snapshot = client.snapshot().await?;

        let now = chrono_now();
        let mut updated = Vec::new();
        let mut unchanged = Vec::new();

        for record in &snapshot.ledgers {
            let ledger_id = &record.ledger_id;
            let existing = self.tracking.get_tracking(remote, ledger_id).await?;

            let new_commit = Some(RefValue {
                id: record.commit_head_id.clone(),
                t: record.commit_t,
            });
            let new_index = if record.index_t > 0 || record.index_head_id.is_some() {
                Some(RefValue {
                    id: record.index_head_id.clone(),
                    t: record.index_t,
                })
            } else {
                None
            };

            let changed = match &existing {
                None => true,
                Some(tr) => {
                    tr.commit_ref != new_commit
                        || tr.index_ref != new_index
                        || tr.retracted != record.retracted
                }
            };

            if changed {
                let tracking_record = TrackingRecord {
                    schema_version: 1,
                    remote: remote.clone(),
                    ledger_id: ledger_id.clone(),
                    commit_ref: new_commit,
                    index_ref: new_index,
                    retracted: record.retracted,
                    last_fetched: Some(now.clone()),
                };
                self.tracking.set_tracking(&tracking_record).await?;
                updated.push((ledger_id.clone(), tracking_record));
            } else {
                unchanged.push(ledger_id.clone());
            }
        }

        Ok(FetchResult { updated, unchanged })
    }

    /// Pull (fast-forward) a local ledger ID from its upstream tracking ref.
    ///
    /// Analogous to `git pull --ff-only`. Requires a prior `fetch_remote`.
    pub async fn pull_tracked(&self, local_alias: &str) -> Result<PullResult> {
        let upstream = self.config.get_upstream(local_alias).await?;
        let Some(upstream) = upstream else {
            return Ok(PullResult::NoUpstream {
                ledger_id: local_alias.to_string(),
            });
        };

        let tracking = self
            .tracking
            .get_tracking(&upstream.remote, &upstream.remote_alias)
            .await?;
        let Some(tracking) = tracking else {
            return Ok(PullResult::NoTracking {
                ledger_id: local_alias.to_string(),
            });
        };

        let remote_index = tracking.index_ref.clone();

        let Some(remote_commit) = &tracking.commit_ref else {
            return Ok(PullResult::Current {
                ledger_id: local_alias.to_string(),
            });
        };

        let local_ref = self
            .local
            .get_ref(local_alias, RefKind::CommitHead)
            .await
            .map_err(SyncError::Nameservice)?;

        match &local_ref {
            None => {
                // Local doesn't exist yet — create it via CAS
                let result = self
                    .local
                    .compare_and_set_ref(local_alias, RefKind::CommitHead, None, remote_commit)
                    .await
                    .map_err(SyncError::Nameservice)?;
                match result {
                    CasResult::Updated => {
                        // Also fast-forward index head if remote provided one.
                        if let Some(remote_index) = remote_index.as_ref() {
                            let _ = self
                                .local
                                .compare_and_set_ref(
                                    local_alias,
                                    RefKind::IndexHead,
                                    None,
                                    remote_index,
                                )
                                .await
                                .map_err(SyncError::Nameservice)?;
                        }

                        Ok(PullResult::FastForwarded {
                            ledger_id: local_alias.to_string(),
                            from: RefValue { id: None, t: 0 },
                            to: remote_commit.clone(),
                        })
                    }
                    CasResult::Conflict { actual } => {
                        // Someone else created it concurrently
                        Ok(PullResult::Diverged {
                            ledger_id: local_alias.to_string(),
                            local: actual.unwrap_or(RefValue { id: None, t: 0 }),
                            remote: remote_commit.clone(),
                        })
                    }
                }
            }
            Some(local_commit) => {
                if remote_commit.t > local_commit.t {
                    // Remote is ahead — fast-forward
                    let result = self
                        .local
                        .fast_forward_commit(local_alias, remote_commit, 3)
                        .await
                        .map_err(SyncError::Nameservice)?;
                    match result {
                        CasResult::Updated => {
                            // Also fast-forward index head if remote provided one.
                            if let Some(remote_index) = remote_index.as_ref() {
                                self.fast_forward_index(local_alias, remote_index, 3)
                                    .await
                                    .map_err(SyncError::Nameservice)?;
                            }

                            Ok(PullResult::FastForwarded {
                                ledger_id: local_alias.to_string(),
                                from: local_commit.clone(),
                                to: remote_commit.clone(),
                            })
                        }
                        CasResult::Conflict { actual } => Ok(PullResult::Diverged {
                            ledger_id: local_alias.to_string(),
                            local: actual.unwrap_or(local_commit.clone()),
                            remote: remote_commit.clone(),
                        }),
                    }
                } else if remote_commit.t == local_commit.t && remote_commit.id == local_commit.id {
                    // Commit is current, but index may still be behind (reindex at same t).
                    if let Some(remote_index) = remote_index.as_ref() {
                        self.fast_forward_index(local_alias, remote_index, 3)
                            .await
                            .map_err(SyncError::Nameservice)?;
                    }

                    Ok(PullResult::Current {
                        ledger_id: local_alias.to_string(),
                    })
                } else {
                    // Local is ahead or different history
                    Ok(PullResult::Diverged {
                        ledger_id: local_alias.to_string(),
                        local: local_commit.clone(),
                        remote: remote_commit.clone(),
                    })
                }
            }
        }
    }

    /// Fast-forward the index head with a retry loop.
    ///
    /// Index heads are non-strict monotonic (`new.t >= cur.t`), which allows
    /// re-index at the same commit `t`. This helper mirrors `fast_forward_commit`
    /// but uses the `IndexHead` guard.
    async fn fast_forward_index(
        &self,
        ledger_id: &str,
        new: &RefValue,
        max_retries: usize,
    ) -> std::result::Result<CasResult, fluree_db_nameservice::NameServiceError> {
        for _ in 0..max_retries {
            let current = self.local.get_ref(ledger_id, RefKind::IndexHead).await?;

            // If current index is ahead, this is not a fast-forward.
            if let Some(ref cur) = current {
                if new.t < cur.t {
                    return Ok(CasResult::Conflict { actual: current });
                }
            }

            match self
                .local
                .compare_and_set_ref(ledger_id, RefKind::IndexHead, current.as_ref(), new)
                .await?
            {
                CasResult::Updated => return Ok(CasResult::Updated),
                CasResult::Conflict { actual } => {
                    // Another writer advanced the ref — still FF-able?
                    if let Some(ref a) = actual {
                        if new.t < a.t {
                            return Ok(CasResult::Conflict { actual });
                        }
                    }
                    continue;
                }
            }
        }

        let current = self.local.get_ref(ledger_id, RefKind::IndexHead).await?;
        Ok(CasResult::Conflict { actual: current })
    }

    /// Push a local ledger ID to its upstream remote.
    ///
    /// Analogous to `git push`. Uses CAS to ensure no concurrent changes.
    pub async fn push_tracked(&self, local_alias: &str) -> Result<PushResult> {
        let upstream = self.config.get_upstream(local_alias).await?;
        let Some(upstream) = upstream else {
            return Ok(PushResult::NoUpstream {
                ledger_id: local_alias.to_string(),
            });
        };

        let client = self.get_client(&upstream.remote)?;

        let local_ref = self
            .local
            .get_ref(local_alias, RefKind::CommitHead)
            .await
            .map_err(SyncError::Nameservice)?;
        let Some(local_commit) = local_ref else {
            return Err(SyncError::Config(format!(
                "Local ledger ID '{local_alias}' has no commit ref"
            )));
        };

        let local_index = self
            .local
            .get_ref(local_alias, RefKind::IndexHead)
            .await
            .map_err(SyncError::Nameservice)?;

        // Get tracking ref (last known remote state) as CAS expected value
        let tracking = self
            .tracking
            .get_tracking(&upstream.remote, &upstream.remote_alias)
            .await?;
        let expected = tracking.as_ref().and_then(|t| t.commit_ref.as_ref());
        let expected_index = tracking.as_ref().and_then(|t| t.index_ref.clone());

        let result = client
            .push_ref(
                &upstream.remote_alias,
                RefKind::CommitHead,
                expected,
                &local_commit,
            )
            .await?;

        match result {
            CasResult::Updated => {
                // Update tracking ref to reflect the push
                let mut tracking_record = tracking.unwrap_or_else(|| {
                    TrackingRecord::new(upstream.remote.clone(), upstream.remote_alias.clone())
                });
                tracking_record.commit_ref = Some(local_commit.clone());
                tracking_record.last_fetched = Some(chrono_now());

                // Best-effort push of index head if we have one.
                if let Some(local_index) = local_index.as_ref() {
                    if local_index.id.is_some() {
                        if let Ok(CasResult::Updated) = client
                            .push_ref(
                                &upstream.remote_alias,
                                RefKind::IndexHead,
                                expected_index.as_ref(),
                                local_index,
                            )
                            .await
                        {
                            tracking_record.index_ref = Some(local_index.clone());
                        }
                    }
                }

                self.tracking.set_tracking(&tracking_record).await?;

                Ok(PushResult::Pushed {
                    ledger_id: local_alias.to_string(),
                    value: local_commit,
                })
            }
            CasResult::Conflict { actual } => {
                let remote = actual.unwrap_or(RefValue { id: None, t: 0 });
                Ok(PushResult::Rejected {
                    ledger_id: local_alias.to_string(),
                    local: local_commit,
                    remote,
                })
            }
        }
    }
}

/// Simple ISO-8601 timestamp without external chrono dep.
fn chrono_now() -> String {
    // Use a basic format; consumers can parse with chrono if needed.
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    format!("{now}Z")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::RemoteSnapshot;
    use crate::config::{MemorySyncConfigStore, UpstreamConfig};
    use fluree_db_core::{ContentId, ContentKind};
    use fluree_db_nameservice::memory::MemoryNameService;
    use fluree_db_nameservice::{MemoryTrackingStore, NsRecord, Publisher, RefLookup};

    fn origin() -> RemoteName {
        RemoteName::new("origin")
    }

    fn test_commit_id(label: &str) -> ContentId {
        ContentId::new(ContentKind::Commit, label.as_bytes())
    }

    /// Mock remote client backed by an in-memory nameservice
    #[derive(Debug)]
    struct MockRemoteClient {
        ns: MemoryNameService,
    }

    impl MockRemoteClient {
        fn new() -> Self {
            Self {
                ns: MemoryNameService::new(),
            }
        }
    }

    #[async_trait::async_trait]
    impl RemoteNameserviceClient for MockRemoteClient {
        async fn lookup(&self, ledger_id: &str) -> Result<Option<NsRecord>> {
            use fluree_db_nameservice::NameService;
            Ok(self
                .ns
                .lookup(ledger_id)
                .await
                .map_err(SyncError::Nameservice)?)
        }

        async fn snapshot(&self) -> Result<RemoteSnapshot> {
            use fluree_db_nameservice::NameService;
            let records = self
                .ns
                .all_records()
                .await
                .map_err(SyncError::Nameservice)?;
            Ok(RemoteSnapshot {
                ledgers: records,
                graph_sources: vec![],
            })
        }

        async fn push_ref(
            &self,
            ledger_id: &str,
            kind: RefKind,
            expected: Option<&RefValue>,
            new: &RefValue,
        ) -> Result<CasResult> {
            self.ns
                .compare_and_set_ref(ledger_id, kind, expected, new)
                .await
                .map_err(SyncError::Nameservice)
        }

        async fn init_ledger(&self, _ledger_id: &str) -> Result<bool> {
            Ok(true)
        }
    }

    async fn setup_driver() -> (
        Arc<MemoryNameService>,
        Arc<MockRemoteClient>,
        SyncDriver,
        Arc<MemorySyncConfigStore>,
    ) {
        let local = Arc::new(MemoryNameService::new());
        let tracking = Arc::new(MemoryTrackingStore::new());
        let config = Arc::new(MemorySyncConfigStore::new());
        let remote_client = Arc::new(MockRemoteClient::new());

        let mut driver = SyncDriver::new(
            local.clone() as Arc<dyn RefPublisher>,
            tracking as Arc<dyn RemoteTrackingStore>,
            config.clone() as Arc<dyn SyncConfigStore>,
        );
        driver.add_client(
            &origin(),
            remote_client.clone() as Arc<dyn RemoteNameserviceClient>,
        );

        (local, remote_client, driver, config)
    }

    #[tokio::test]
    async fn test_fetch_updates_tracking() {
        let (_local, remote, driver, _config) = setup_driver().await;

        // Publish something on the remote
        remote
            .ns
            .publish_commit("mydb:main", 5, &test_commit_id("commit-1"))
            .await
            .unwrap();

        let result = driver.fetch_remote(&origin()).await.unwrap();
        assert_eq!(result.updated.len(), 1);
        assert_eq!(result.updated[0].0, "mydb:main");
        assert_eq!(result.updated[0].1.commit_ref.as_ref().unwrap().t, 5);
        assert!(result.unchanged.is_empty());
    }

    #[tokio::test]
    async fn test_fetch_idempotent() {
        let (_local, remote, driver, _config) = setup_driver().await;

        remote
            .ns
            .publish_commit("mydb:main", 5, &test_commit_id("commit-1"))
            .await
            .unwrap();

        driver.fetch_remote(&origin()).await.unwrap();
        let result = driver.fetch_remote(&origin()).await.unwrap();

        // Second fetch should find no changes
        assert!(result.updated.is_empty());
        assert_eq!(result.unchanged.len(), 1);
    }

    #[tokio::test]
    async fn test_pull_fast_forwards() {
        let (local, remote, driver, config) = setup_driver().await;

        // Setup upstream
        config
            .set_upstream(&UpstreamConfig {
                local_alias: "mydb:main".to_string(),
                remote: origin(),
                remote_alias: "mydb:main".to_string(),
                auto_pull: false,
            })
            .await
            .unwrap();

        // Create local at t=1
        local
            .publish_commit("mydb:main", 1, &test_commit_id("commit-1"))
            .await
            .unwrap();

        // Remote at t=5
        remote
            .ns
            .publish_commit("mydb:main", 5, &test_commit_id("commit-5"))
            .await
            .unwrap();

        // Fetch first
        driver.fetch_remote(&origin()).await.unwrap();

        // Pull should fast-forward
        match driver.pull_tracked("mydb:main").await.unwrap() {
            PullResult::FastForwarded { from, to, .. } => {
                assert_eq!(from.t, 1);
                assert_eq!(to.t, 5);
            }
            other => panic!("expected FastForwarded, got {other:?}"),
        }

        // Verify local is now at t=5
        let local_ref = local
            .get_ref("mydb:main", RefKind::CommitHead)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(local_ref.t, 5);
    }

    #[tokio::test]
    async fn test_pull_already_current() {
        let (local, remote, driver, config) = setup_driver().await;

        config
            .set_upstream(&UpstreamConfig {
                local_alias: "mydb:main".to_string(),
                remote: origin(),
                remote_alias: "mydb:main".to_string(),
                auto_pull: false,
            })
            .await
            .unwrap();

        // Both at t=5 with same address
        local
            .publish_commit("mydb:main", 5, &test_commit_id("commit-5"))
            .await
            .unwrap();
        remote
            .ns
            .publish_commit("mydb:main", 5, &test_commit_id("commit-5"))
            .await
            .unwrap();

        driver.fetch_remote(&origin()).await.unwrap();

        match driver.pull_tracked("mydb:main").await.unwrap() {
            PullResult::Current { .. } => {}
            other => panic!("expected Current, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_pull_diverged() {
        let (local, remote, driver, config) = setup_driver().await;

        config
            .set_upstream(&UpstreamConfig {
                local_alias: "mydb:main".to_string(),
                remote: origin(),
                remote_alias: "mydb:main".to_string(),
                auto_pull: false,
            })
            .await
            .unwrap();

        // Local ahead at t=10
        local
            .publish_commit("mydb:main", 10, &test_commit_id("commit-10"))
            .await
            .unwrap();

        // Remote at t=5
        remote
            .ns
            .publish_commit("mydb:main", 5, &test_commit_id("commit-5"))
            .await
            .unwrap();

        driver.fetch_remote(&origin()).await.unwrap();

        match driver.pull_tracked("mydb:main").await.unwrap() {
            PullResult::Diverged {
                local: l,
                remote: r,
                ..
            } => {
                assert_eq!(l.t, 10);
                assert_eq!(r.t, 5);
            }
            other => panic!("expected Diverged, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_pull_no_upstream() {
        let (_local, _remote, driver, _config) = setup_driver().await;

        match driver.pull_tracked("mydb:main").await.unwrap() {
            PullResult::NoUpstream { .. } => {}
            other => panic!("expected NoUpstream, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_push_succeeds() {
        let (local, _remote, driver, config) = setup_driver().await;

        config
            .set_upstream(&UpstreamConfig {
                local_alias: "mydb:main".to_string(),
                remote: origin(),
                remote_alias: "mydb:main".to_string(),
                auto_pull: false,
            })
            .await
            .unwrap();

        local
            .publish_commit("mydb:main", 5, &test_commit_id("commit-5"))
            .await
            .unwrap();

        match driver.push_tracked("mydb:main").await.unwrap() {
            PushResult::Pushed { value, .. } => {
                assert_eq!(value.t, 5);
            }
            other => panic!("expected Pushed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_push_conflict() {
        let (local, remote, driver, config) = setup_driver().await;

        config
            .set_upstream(&UpstreamConfig {
                local_alias: "mydb:main".to_string(),
                remote: origin(),
                remote_alias: "mydb:main".to_string(),
                auto_pull: false,
            })
            .await
            .unwrap();

        // Remote has data already
        remote
            .ns
            .publish_commit("mydb:main", 1, &test_commit_id("commit-1"))
            .await
            .unwrap();

        // Local has different data
        local
            .publish_commit("mydb:main", 5, &test_commit_id("commit-5"))
            .await
            .unwrap();

        // Push without fetch first — expected=None but remote has data
        match driver.push_tracked("mydb:main").await.unwrap() {
            PushResult::Rejected {
                local: l,
                remote: r,
                ..
            } => {
                assert_eq!(l.t, 5);
                assert_eq!(r.t, 1);
            }
            other => panic!("expected Rejected, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_push_no_upstream() {
        let (local, _remote, driver, _config) = setup_driver().await;

        local
            .publish_commit("mydb:main", 1, &test_commit_id("commit-1"))
            .await
            .unwrap();

        match driver.push_tracked("mydb:main").await.unwrap() {
            PushResult::NoUpstream { .. } => {}
            other => panic!("expected NoUpstream, got {other:?}"),
        }
    }
}
