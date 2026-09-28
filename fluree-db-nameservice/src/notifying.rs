//! Decorator that emits [`NameServiceEvent`]s on a [`LedgerEventBus`]
//! after successful nameservice writes.
//!
//! Wrap any nameservice implementation in [`NotifyingNameService`] to get
//! automatic event emission without modifying the backend itself.

use std::sync::Arc;

use crate::Fence;
use async_trait::async_trait;
use fluree_db_core::{ContentId, LedgerId};

use crate::{
    event_bus::LedgerEventBus, AdminPublisher, BranchLifecycle, CasResult, CommitPublisher,
    ConfigCasResult, ConfigLookup, ConfigPublisher, ConfigValue, GraphSourceLookup,
    GraphSourcePublisher, GraphSourceRecord, GraphSourceType, IndexPublisher, LedgerHeads,
    NameServiceEvent, NameServiceLookup, NsLookupResult, NsRecord, NsRecordSnapshot, RefKind,
    RefLookup, RefPublisher, RefValue, Result, StatusCasResult, StatusLookup, StatusPublisher,
    StatusValue, Subscription, SubscriptionScope,
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

impl<N: crate::LedgerRegistry> NotifyingNameService<N> {
    /// The binding of `ledger_id`'s name, when it lists that branch with
    /// `fence`. A failed read gives `None`: the event then goes out without
    /// what the binding would have told it, rather than failing the write.
    async fn listing_binding(
        &self,
        ledger_id: &LedgerId,
        fence: Option<Fence>,
    ) -> Option<crate::NameBinding> {
        let binding = self
            .inner
            .get_binding(&ledger_id.ledger_name())
            .await
            .ok()??
            .value;
        let listed = binding.listing(ledger_id.branch())?;
        (Some(listed.fence) == fence).then_some(binding)
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
// Read + branch lifecycle — pure delegation
// ---------------------------------------------------------------------------

#[async_trait]
impl<N> NameServiceLookup for NotifyingNameService<N>
where
    N: NameServiceLookup,
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

    async fn heads(&self, ledger_id: &str) -> Result<Option<LedgerHeads>> {
        self.inner.heads(ledger_id).await
    }
}

#[async_trait]
impl<N> BranchLifecycle for NotifyingNameService<N>
where
    N: BranchLifecycle,
{
    async fn reset_head_fenced(
        &self,
        ledger_id: &str,
        fence: Option<Fence>,
        snapshot: NsRecordSnapshot,
    ) -> Result<()> {
        self.inner
            .reset_head_fenced(ledger_id, fence, snapshot)
            .await
    }

    // Pass-through wrapper: delegate so the inner backend's commit-CID index
    // (e.g. FileNameService) still accelerates incremental indexing. Falling
    // back to the trait default here would silently force the serial DAG walk
    // for the entire FileStorage server path, which wraps in this notifier.
    async fn pending_commit_cids(
        &self,
        ledger_id: &str,
        since_t: i64,
    ) -> Result<Option<Vec<(i64, ContentId)>>> {
        self.inner.pending_commit_cids(ledger_id, since_t).await
    }

    async fn prune_commit_index(&self, ledger_id: &str, up_to_t: i64) -> Result<()> {
        self.inner.prune_commit_index(ledger_id, up_to_t).await
    }
}

// ---------------------------------------------------------------------------
// Publisher — emit events after successful writes
// ---------------------------------------------------------------------------

#[async_trait]
impl<N> CommitPublisher for NotifyingNameService<N>
where
    N: CommitPublisher,
{
    async fn publish_commit_fenced(
        &self,
        ledger_id: &str,
        fence: Option<Fence>,
        commit_t: i64,
        commit_id: &ContentId,
    ) -> Result<()> {
        self.inner
            .publish_commit_fenced(ledger_id, fence, commit_t, commit_id)
            .await?;
        self.event_bus
            .notify(NameServiceEvent::LedgerCommitPublished {
                ledger_id: LedgerId::parse(ledger_id)?,
                commit_id: commit_id.clone(),
                commit_t,
            });
        Ok(())
    }

    fn publishing_ledger_id(&self, ledger_id: &str) -> Option<String> {
        self.inner.publishing_ledger_id(ledger_id)
    }
}

#[async_trait]
impl<N> IndexPublisher for NotifyingNameService<N>
where
    N: IndexPublisher,
{
    async fn publish_index_fenced(
        &self,
        ledger_id: &str,
        fence: Option<Fence>,
        index_t: i64,
        index_id: &ContentId,
    ) -> Result<()> {
        self.inner
            .publish_index_fenced(ledger_id, fence, index_t, index_id)
            .await?;
        self.event_bus
            .notify(NameServiceEvent::LedgerIndexPublished {
                ledger_id: LedgerId::parse(ledger_id)?,
                index_id: index_id.clone(),
                index_t,
            });
        Ok(())
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
    async fn publish_index_allow_equal_fenced(
        &self,
        ledger_id: &str,
        fence: Option<Fence>,
        index_t: i64,
        index_id: &ContentId,
    ) -> Result<()> {
        self.inner
            .publish_index_allow_equal_fenced(ledger_id, fence, index_t, index_id)
            .await?;
        self.event_bus
            .notify(NameServiceEvent::LedgerIndexPublished {
                ledger_id: LedgerId::parse(ledger_id)?,
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
    async fn compare_and_set_ref_fenced(
        &self,
        ledger_id: &str,
        fence: Option<Fence>,
        kind: RefKind,
        expected: Option<&RefValue>,
        new: &RefValue,
    ) -> Result<CasResult> {
        let result = self
            .inner
            .compare_and_set_ref_fenced(ledger_id, fence, kind, expected, new)
            .await?;

        if matches!(result, CasResult::Updated) {
            if let Some(ref cid) = new.id {
                match kind {
                    RefKind::CommitHead => {
                        self.event_bus
                            .notify(NameServiceEvent::LedgerCommitPublished {
                                ledger_id: LedgerId::parse(ledger_id)?,
                                commit_id: cid.clone(),
                                commit_t: new.t,
                            });
                    }
                    RefKind::IndexHead => {
                        self.event_bus
                            .notify(NameServiceEvent::LedgerIndexPublished {
                                ledger_id: LedgerId::parse(ledger_id)?,
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
        let graph_source_id = LedgerId::from_parts(name, branch)?;
        let canonical_deps = dependencies
            .iter()
            .map(|d| LedgerId::parse(d))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        self.inner
            .publish_graph_source(name, branch, source_type.clone(), config, dependencies)
            .await?;
        self.event_bus
            .notify(NameServiceEvent::GraphSourceConfigPublished {
                graph_source_id,
                source_type,
                dependencies: canonical_deps,
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
        let graph_source_id = LedgerId::from_parts(name, branch)?;
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
        let graph_source_id = LedgerId::from_parts(name, branch)?;
        self.event_bus
            .notify(NameServiceEvent::GraphSourceRetracted { graph_source_id });
        Ok(())
    }

    /// Emits nothing: the source is retracted, and publishing it again
    /// announces it.
    async fn reset_graph_source_index(&self, name: &str, branch: &str) -> Result<()> {
        self.inner.reset_graph_source_index(name, branch).await
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
    async fn push_status_fenced(
        &self,
        ledger_id: &str,
        fence: Option<Fence>,
        expected: Option<&StatusValue>,
        new: &StatusValue,
    ) -> Result<StatusCasResult> {
        self.inner
            .push_status_fenced(ledger_id, fence, expected, new)
            .await
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
    async fn push_config_fenced(
        &self,
        ledger_id: &str,
        fence: Option<Fence>,
        expected: Option<&ConfigValue>,
        new: &ConfigValue,
    ) -> Result<ConfigCasResult> {
        self.inner
            .push_config_fenced(ledger_id, fence, expected, new)
            .await
    }
}

#[async_trait]
impl<N> crate::LedgerRegistry for NotifyingNameService<N>
where
    N: crate::LedgerRegistry,
{
    async fn get_binding(
        &self,
        name: &str,
    ) -> Result<Option<crate::Versioned<crate::NameBinding>>> {
        self.inner.get_binding(name).await
    }

    /// Announces each branch the new binding shows that the one it replaced
    /// did not, as the write confirming a branch's create does. When the
    /// replaced binding cannot be read as it was, every branch shown is
    /// announced.
    async fn cas_binding(
        &self,
        name: &str,
        expected: Option<u64>,
        new: Option<&crate::NameBinding>,
    ) -> Result<crate::RegistryCas<crate::NameBinding>> {
        let before = match new {
            Some(binding) if binding.is_active() => self
                .inner
                .get_binding(name)
                .await
                .ok()
                .flatten()
                .filter(|v| Some(v.version) == expected)
                .map(|v| v.value),
            _ => None,
        };
        let result = self.inner.cas_binding(name, expected, new).await?;
        if let (crate::RegistryCas::Updated { .. }, Some(binding)) = (&result, new) {
            for listing in &binding.branches {
                if !binding.shows(&listing.branch) {
                    continue;
                }
                let shown_before = before.as_ref().is_some_and(|b| {
                    b.instance == binding.instance
                        && b.shows(&listing.branch)
                        && b.fence_of(&listing.branch) == Some(listing.fence)
                });
                if shown_before {
                    continue;
                }
                if let Ok(ledger_id) = LedgerId::from_parts(name, &listing.branch) {
                    self.event_bus.notify(NameServiceEvent::LedgerCreated {
                        ledger_id,
                        instance: binding.instance.clone(),
                    });
                }
            }
        }
        Ok(result)
    }

    async fn list_bindings(&self) -> Result<Vec<(String, crate::Versioned<crate::NameBinding>)>> {
        self.inner.list_bindings().await
    }

    async fn get_dropped(
        &self,
        instance: &fluree_db_core::InstanceId,
    ) -> Result<Option<crate::Versioned<crate::DroppedLedger>>> {
        self.inner.get_dropped(instance).await
    }

    async fn cas_dropped(
        &self,
        instance: &fluree_db_core::InstanceId,
        expected: Option<u64>,
        new: Option<&crate::DroppedLedger>,
    ) -> Result<crate::RegistryCas<crate::DroppedLedger>> {
        self.inner.cas_dropped(instance, expected, new).await
    }

    async fn list_dropped(&self) -> Result<Vec<crate::Versioned<crate::DroppedLedger>>> {
        self.inner.list_dropped().await
    }
}

#[async_trait]
impl<N> crate::BranchRecordStore for NotifyingNameService<N>
where
    N: crate::BranchRecordStore + crate::LedgerRegistry,
{
    async fn raw_record(&self, ledger_id: &str) -> Result<Option<NsRecord>> {
        self.inner.raw_record(ledger_id).await
    }

    async fn all_raw_records(&self) -> Result<Vec<NsRecord>> {
        self.inner.all_raw_records().await
    }

    /// Announces the branch when its binding already shows it, as when a
    /// mirrored copy of a branch is inserted.
    async fn insert_record(&self, record: &NsRecord) -> Result<Option<NsRecord>> {
        let existing = self.inner.insert_record(record).await?;
        if existing.is_none() {
            if let Some(binding) = self.listing_binding(&record.ledger_id, record.fence).await {
                if binding.shows(&record.branch) {
                    self.event_bus.notify(NameServiceEvent::LedgerCreated {
                        ledger_id: record.ledger_id.clone(),
                        instance: binding.instance,
                    });
                }
            }
        }
        Ok(existing)
    }

    async fn adopt_record(
        &self,
        ledger_id: &str,
        fence: crate::Fence,
    ) -> Result<crate::FenceOutcome> {
        self.inner.adopt_record(ledger_id, fence).await
    }

    async fn freeze_record(
        &self,
        ledger_id: &str,
        fence: crate::Fence,
    ) -> Result<crate::FenceOutcome> {
        self.inner.freeze_record(ledger_id, fence).await
    }

    async fn delete_record(
        &self,
        ledger_id: &str,
        fence: crate::Fence,
    ) -> Result<crate::FenceOutcome> {
        let id = LedgerId::parse(ledger_id)?;
        // A drop deletes records while its binding still lists them, which
        // names the ledger they belonged to.
        let instance = self
            .listing_binding(&id, Some(fence))
            .await
            .map(|b| b.instance);
        let outcome = self.inner.delete_record(ledger_id, fence).await?;
        if outcome == crate::FenceOutcome::Applied {
            self.event_bus.notify(NameServiceEvent::LedgerRetracted {
                ledger_id: id,
                instance,
            });
        }
        Ok(outcome)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{lifecycle, memory::MemoryNameService, SubscriptionScope};
    use fluree_db_core::{InstanceId, LedgerName};

    fn drain(sub: &mut Subscription) -> Vec<NameServiceEvent> {
        std::iter::from_fn(|| sub.receiver.try_recv().ok()).collect()
    }

    fn created(ledger_id: &str, instance: &InstanceId) -> NameServiceEvent {
        NameServiceEvent::LedgerCreated {
            ledger_id: LedgerId::parse(ledger_id).unwrap(),
            instance: instance.clone(),
        }
    }

    fn retracted(ledger_id: &str, instance: &InstanceId) -> NameServiceEvent {
        NameServiceEvent::LedgerRetracted {
            ledger_id: LedgerId::parse(ledger_id).unwrap(),
            instance: Some(instance.clone()),
        }
    }

    /// Creates, branches, drops and restores announce the branches they
    /// show or remove, each naming the ledger it belongs to.
    #[tokio::test]
    async fn lifecycle_events_name_the_instance() {
        let bus = Arc::new(LedgerEventBus::new(64));
        let ns = NotifyingNameService::new(MemoryNameService::new(), Arc::clone(&bus));
        let mut sub = bus.subscribe(SubscriptionScope::all());
        let name = LedgerName::parse("mydb").unwrap();
        let main_id = LedgerId::parse("mydb:main").unwrap();

        let main = lifecycle::create_ledger(&ns, &main_id).await.unwrap();
        let instance = main.instance().unwrap();
        assert_eq!(drain(&mut sub), vec![created("mydb:main", &instance)]);

        lifecycle::create_branch(&ns, &name, "dev", "main", None)
            .await
            .unwrap();
        let events = drain(&mut sub);
        assert!(!events.is_empty());
        assert!(events.iter().all(|e| *e == created("mydb:dev", &instance)));

        lifecycle::drop_ledger(&ns, &name, false).await.unwrap();
        let events = drain(&mut sub);
        assert_eq!(events.len(), 2, "{events:?}");
        assert!(events.contains(&retracted("mydb:main", &instance)));
        assert!(events.contains(&retracted("mydb:dev", &instance)));

        lifecycle::restore_dropped(&ns, &instance).await.unwrap();
        let events = drain(&mut sub);
        assert!(events.contains(&created("mydb:main", &instance)));
        assert!(events.contains(&created("mydb:dev", &instance)));
        assert!(events
            .iter()
            .all(|e| matches!(e, NameServiceEvent::LedgerCreated { .. })));

        // A ledger created under the name later is another instance.
        lifecycle::drop_ledger(&ns, &name, true).await.unwrap();
        drain(&mut sub);
        let again = lifecycle::create_ledger(&ns, &main_id).await.unwrap();
        let next = again.instance().unwrap();
        assert_ne!(next, instance);
        assert_eq!(drain(&mut sub), vec![created("mydb:main", &next)]);
    }
}
