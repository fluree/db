//! Decorator that emits [`NameServiceEvent`]s on a [`LedgerEventBus`]
//! after successful nameservice writes.
//!
//! Wrap any nameservice implementation in [`NotifyingNameService`] to get
//! automatic event emission without modifying the backend itself.

use std::sync::Arc;

use async_trait::async_trait;
use fluree_db_core::ContentId;

use crate::{
    event_bus::LedgerEventBus, AdminPublisher, CasResult, ConfigCasResult, ConfigLookup,
    ConfigPublisher, ConfigValue, GraphSourceLookup, GraphSourcePublisher, GraphSourceRecord,
    GraphSourceType, NameService, NameServiceEvent, NsLookupResult, NsRecord, NsRecordSnapshot,
    Publisher, RefKind, RefLookup, RefPublisher, RefValue, Result, StatusCasResult, StatusLookup,
    StatusPublisher, StatusValue, Subscription, SubscriptionScope,
};

/// Decorator that wraps a nameservice and emits events on a [`LedgerEventBus`]
/// after successful write operations.
///
/// Read-only methods delegate directly without side effects.
/// Write methods that mutate nameservice state emit the corresponding
/// [`NameServiceEvent`] variant on success.
#[derive(Debug)]
pub struct NotifyingNameService<N> {
    inner: N,
    event_bus: Arc<LedgerEventBus>,
}

impl<N> NotifyingNameService<N> {
    /// Wrap a nameservice with event notification.
    pub fn new(inner: N, event_bus: Arc<LedgerEventBus>) -> Self {
        Self { inner, event_bus }
    }

    /// Get a reference to the underlying nameservice.
    pub fn inner(&self) -> &N {
        &self.inner
    }

    /// Get a reference to the event bus.
    pub fn event_bus(&self) -> &Arc<LedgerEventBus> {
        &self.event_bus
    }

    /// Subscribe to events with the given scope.
    pub fn subscribe(&self, scope: SubscriptionScope) -> Subscription {
        self.event_bus.subscribe(scope)
    }
}

impl<N: Clone> Clone for NotifyingNameService<N> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            event_bus: Arc::clone(&self.event_bus),
        }
    }
}

// ---------------------------------------------------------------------------
// NameService (read + branch management) — pure delegation
// ---------------------------------------------------------------------------

#[async_trait]
impl<N> NameService for NotifyingNameService<N>
where
    N: NameService,
{
    async fn lookup(&self, ledger_id: &str) -> Result<Option<NsRecord>> {
        self.inner.lookup(ledger_id).await
    }

    async fn all_records(&self) -> Result<Vec<NsRecord>> {
        self.inner.all_records().await
    }

    async fn list_branches(&self, ledger_name: &str) -> Result<Vec<NsRecord>> {
        self.inner.list_branches(ledger_name).await
    }

    async fn create_branch(
        &self,
        ledger_name: &str,
        new_branch: &str,
        source_branch: &str,
    ) -> Result<()> {
        self.inner
            .create_branch(ledger_name, new_branch, source_branch)
            .await
    }

    async fn drop_branch(&self, ledger_id: &str) -> Result<Option<u32>> {
        self.inner.drop_branch(ledger_id).await
    }

    async fn reset_head(&self, ledger_id: &str, snapshot: NsRecordSnapshot) -> Result<()> {
        self.inner.reset_head(ledger_id, snapshot).await
    }
}

// ---------------------------------------------------------------------------
// Publisher — emit events after successful writes
// ---------------------------------------------------------------------------

#[async_trait]
impl<N> Publisher for NotifyingNameService<N>
where
    N: Publisher,
{
    async fn publish_ledger_init(&self, ledger_id: &str) -> Result<()> {
        self.inner.publish_ledger_init(ledger_id).await
    }

    async fn publish_commit(
        &self,
        ledger_id: &str,
        commit_t: i64,
        commit_id: &ContentId,
    ) -> Result<()> {
        self.inner
            .publish_commit(ledger_id, commit_t, commit_id)
            .await?;
        self.event_bus
            .notify(NameServiceEvent::LedgerCommitPublished {
                ledger_id: ledger_id.to_string(),
                commit_id: commit_id.clone(),
                commit_t,
            });
        Ok(())
    }

    async fn publish_index(
        &self,
        ledger_id: &str,
        index_t: i64,
        index_id: &ContentId,
    ) -> Result<()> {
        self.inner
            .publish_index(ledger_id, index_t, index_id)
            .await?;
        self.event_bus
            .notify(NameServiceEvent::LedgerIndexPublished {
                ledger_id: ledger_id.to_string(),
                index_id: index_id.clone(),
                index_t,
            });
        Ok(())
    }

    async fn retract(&self, ledger_id: &str) -> Result<()> {
        self.inner.retract(ledger_id).await?;
        self.event_bus.notify(NameServiceEvent::LedgerRetracted {
            ledger_id: ledger_id.to_string(),
        });
        Ok(())
    }

    async fn purge(&self, ledger_id: &str) -> Result<()> {
        self.inner.purge(ledger_id).await?;
        self.event_bus.notify(NameServiceEvent::LedgerRetracted {
            ledger_id: ledger_id.to_string(),
        });
        Ok(())
    }

    fn publishing_ledger_id(&self, ledger_id: &str) -> Option<String> {
        self.inner.publishing_ledger_id(ledger_id)
    }
}

// ---------------------------------------------------------------------------
// AdminPublisher — emit after successful allow-equal index publish
// ---------------------------------------------------------------------------

#[async_trait]
impl<N> AdminPublisher for NotifyingNameService<N>
where
    N: AdminPublisher,
{
    async fn publish_index_allow_equal(
        &self,
        ledger_id: &str,
        index_t: i64,
        index_id: &ContentId,
    ) -> Result<()> {
        self.inner
            .publish_index_allow_equal(ledger_id, index_t, index_id)
            .await?;
        self.event_bus
            .notify(NameServiceEvent::LedgerIndexPublished {
                ledger_id: ledger_id.to_string(),
                index_id: index_id.clone(),
                index_t,
            });
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// RefLookup — pure delegation
// ---------------------------------------------------------------------------

#[async_trait]
impl<N> RefLookup for NotifyingNameService<N>
where
    N: RefLookup,
{
    async fn get_ref(&self, ledger_id: &str, kind: RefKind) -> Result<Option<RefValue>> {
        self.inner.get_ref(ledger_id, kind).await
    }
}

// ---------------------------------------------------------------------------
// RefPublisher — emit only on successful CAS (Updated)
// ---------------------------------------------------------------------------

#[async_trait]
impl<N> RefPublisher for NotifyingNameService<N>
where
    N: RefPublisher,
{
    async fn compare_and_set_ref(
        &self,
        ledger_id: &str,
        kind: RefKind,
        expected: Option<&RefValue>,
        new: &RefValue,
    ) -> Result<CasResult> {
        let result = self
            .inner
            .compare_and_set_ref(ledger_id, kind, expected, new)
            .await?;

        if matches!(result, CasResult::Updated) {
            if let Some(ref cid) = new.id {
                match kind {
                    RefKind::CommitHead => {
                        self.event_bus
                            .notify(NameServiceEvent::LedgerCommitPublished {
                                ledger_id: ledger_id.to_string(),
                                commit_id: cid.clone(),
                                commit_t: new.t,
                            });
                    }
                    RefKind::IndexHead => {
                        self.event_bus
                            .notify(NameServiceEvent::LedgerIndexPublished {
                                ledger_id: ledger_id.to_string(),
                                index_id: cid.clone(),
                                index_t: new.t,
                            });
                    }
                }
            }
        }

        Ok(result)
    }
}

// ---------------------------------------------------------------------------
// GraphSourceLookup — pure delegation
// ---------------------------------------------------------------------------

#[async_trait]
impl<N> GraphSourceLookup for NotifyingNameService<N>
where
    N: GraphSourceLookup,
{
    async fn lookup_graph_source(
        &self,
        graph_source_id: &str,
    ) -> Result<Option<GraphSourceRecord>> {
        self.inner.lookup_graph_source(graph_source_id).await
    }

    async fn lookup_any(&self, resource_id: &str) -> Result<NsLookupResult> {
        self.inner.lookup_any(resource_id).await
    }

    async fn all_graph_source_records(&self) -> Result<Vec<GraphSourceRecord>> {
        self.inner.all_graph_source_records().await
    }
}

// ---------------------------------------------------------------------------
// GraphSourcePublisher — emit after successful writes
// ---------------------------------------------------------------------------

#[async_trait]
impl<N> GraphSourcePublisher for NotifyingNameService<N>
where
    N: GraphSourcePublisher,
{
    async fn publish_graph_source(
        &self,
        name: &str,
        branch: &str,
        source_type: GraphSourceType,
        config: &str,
        dependencies: &[String],
    ) -> Result<()> {
        self.inner
            .publish_graph_source(name, branch, source_type.clone(), config, dependencies)
            .await?;
        let graph_source_id = format!("{name}:{branch}");
        self.event_bus
            .notify(NameServiceEvent::GraphSourceConfigPublished {
                graph_source_id,
                source_type,
                dependencies: dependencies.to_vec(),
            });
        Ok(())
    }

    async fn publish_graph_source_index(
        &self,
        name: &str,
        branch: &str,
        index_id: &ContentId,
        index_t: i64,
    ) -> Result<()> {
        self.inner
            .publish_graph_source_index(name, branch, index_id, index_t)
            .await?;
        let graph_source_id = format!("{name}:{branch}");
        self.event_bus
            .notify(NameServiceEvent::GraphSourceIndexPublished {
                graph_source_id,
                index_id: index_id.clone(),
                index_t,
            });
        Ok(())
    }

    async fn retract_graph_source(&self, name: &str, branch: &str) -> Result<()> {
        self.inner.retract_graph_source(name, branch).await?;
        let graph_source_id = format!("{name}:{branch}");
        self.event_bus
            .notify(NameServiceEvent::GraphSourceRetracted { graph_source_id });
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// StatusLookup — pure delegation
// ---------------------------------------------------------------------------

#[async_trait]
impl<N> StatusLookup for NotifyingNameService<N>
where
    N: StatusLookup,
{
    async fn get_status(&self, ledger_id: &str) -> Result<Option<StatusValue>> {
        self.inner.get_status(ledger_id).await
    }
}

// ---------------------------------------------------------------------------
// StatusPublisher — pure delegation (no events)
// ---------------------------------------------------------------------------

#[async_trait]
impl<N> StatusPublisher for NotifyingNameService<N>
where
    N: StatusPublisher,
{
    async fn push_status(
        &self,
        ledger_id: &str,
        expected: Option<&StatusValue>,
        new: &StatusValue,
    ) -> Result<StatusCasResult> {
        self.inner.push_status(ledger_id, expected, new).await
    }
}

// ---------------------------------------------------------------------------
// ConfigLookup — pure delegation
// ---------------------------------------------------------------------------

#[async_trait]
impl<N> ConfigLookup for NotifyingNameService<N>
where
    N: ConfigLookup,
{
    async fn get_config(&self, ledger_id: &str) -> Result<Option<ConfigValue>> {
        self.inner.get_config(ledger_id).await
    }
}

// ---------------------------------------------------------------------------
// ConfigPublisher — pure delegation (no events)
// ---------------------------------------------------------------------------

#[async_trait]
impl<N> ConfigPublisher for NotifyingNameService<N>
where
    N: ConfigPublisher,
{
    async fn push_config(
        &self,
        ledger_id: &str,
        expected: Option<&ConfigValue>,
        new: &ConfigValue,
    ) -> Result<ConfigCasResult> {
        self.inner.push_config(ledger_id, expected, new).await
    }
}
