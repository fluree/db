//! A deterministic pause immediately before the merge's ref CAS.
//! All other operations use the real nameservice, including rollback.

use async_trait::async_trait;
use fluree_db_core::ContentId;
use fluree_db_nameservice::{
    AdminPublisher, BranchLifecycle, CasResult, CommitPublisher, ConfigCasResult, ConfigLookup,
    ConfigPublisher, ConfigValue, Fence, GraphSourceLookup, GraphSourcePublisher,
    GraphSourceRecord, GraphSourceType, IndexPublisher, LedgerHeads, NameServiceLookup,
    NameServicePublisher, NsLookupResult, NsRecord, NsRecordSnapshot, RefKind, RefLookup,
    RefPublisher, RefValue, Result, StatusCasResult, StatusLookup, StatusPublisher, StatusValue,
};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tokio::sync::Notify;

#[derive(Debug)]
pub(super) struct PausingNameService {
    inner: Arc<dyn NameServicePublisher>,
    armed: AtomicBool,
    pub(super) entered: Notify,
    pub(super) resume: Notify,
}

impl PausingNameService {
    pub(super) fn new(inner: Arc<dyn NameServicePublisher>) -> Self {
        Self {
            inner,
            armed: AtomicBool::new(true),
            entered: Notify::new(),
            resume: Notify::new(),
        }
    }
}

#[async_trait]
impl GraphSourceLookup for PausingNameService {
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

#[async_trait]
impl RefLookup for PausingNameService {
    async fn get_ref(&self, ledger_id: &str, kind: RefKind) -> Result<Option<RefValue>> {
        self.inner.get_ref(ledger_id, kind).await
    }
}

#[async_trait]
impl StatusLookup for PausingNameService {
    async fn get_status(&self, ledger_id: &str) -> Result<Option<StatusValue>> {
        self.inner.get_status(ledger_id).await
    }
}

#[async_trait]
impl ConfigLookup for PausingNameService {
    async fn get_config(&self, ledger_id: &str) -> Result<Option<ConfigValue>> {
        self.inner.get_config(ledger_id).await
    }
}

#[async_trait]
impl NameServiceLookup for PausingNameService {
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
impl BranchLifecycle for PausingNameService {
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

#[async_trait]
impl CommitPublisher for PausingNameService {
    async fn publish_commit_fenced(
        &self,
        ledger_id: &str,
        fence: Option<Fence>,
        commit_t: i64,
        commit_id: &ContentId,
    ) -> Result<()> {
        self.inner
            .publish_commit_fenced(ledger_id, fence, commit_t, commit_id)
            .await
    }

    fn publishing_ledger_id(&self, ledger_id: &str) -> Option<String> {
        self.inner.publishing_ledger_id(ledger_id)
    }
}

#[async_trait]
impl IndexPublisher for PausingNameService {
    async fn publish_index_fenced(
        &self,
        ledger_id: &str,
        fence: Option<Fence>,
        index_t: i64,
        index_id: &ContentId,
    ) -> Result<()> {
        self.inner
            .publish_index_fenced(ledger_id, fence, index_t, index_id)
            .await
    }
}

#[async_trait]
impl AdminPublisher for PausingNameService {
    async fn publish_index_allow_equal_fenced(
        &self,
        ledger_id: &str,
        fence: Option<Fence>,
        index_t: i64,
        index_id: &ContentId,
    ) -> Result<()> {
        self.inner
            .publish_index_allow_equal_fenced(ledger_id, fence, index_t, index_id)
            .await
    }
}

#[async_trait]
impl RefPublisher for PausingNameService {
    async fn compare_and_set_ref_fenced(
        &self,
        ledger_id: &str,
        fence: Option<Fence>,
        kind: RefKind,
        expected: Option<&RefValue>,
        new: &RefValue,
    ) -> Result<CasResult> {
        if ledger_id == "mydb:main"
            && kind == RefKind::CommitHead
            && self.armed.swap(false, Ordering::SeqCst)
        {
            self.entered.notify_one();
            self.resume.notified().await;
        }
        self.inner
            .compare_and_set_ref_fenced(ledger_id, fence, kind, expected, new)
            .await
    }
}

#[async_trait]
impl GraphSourcePublisher for PausingNameService {
    async fn publish_graph_source(
        &self,
        name: &str,
        branch: &str,
        source_type: GraphSourceType,
        config: &str,
        dependencies: &[String],
    ) -> Result<()> {
        self.inner
            .publish_graph_source(name, branch, source_type, config, dependencies)
            .await
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
            .await
    }

    async fn retract_graph_source(&self, name: &str, branch: &str) -> Result<()> {
        self.inner.retract_graph_source(name, branch).await
    }

    async fn reset_graph_source_index(&self, name: &str, branch: &str) -> Result<()> {
        self.inner.reset_graph_source_index(name, branch).await
    }
}

#[async_trait]
impl StatusPublisher for PausingNameService {
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

#[async_trait]
impl ConfigPublisher for PausingNameService {
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
impl fluree_db_nameservice::LedgerRegistry for PausingNameService {
    async fn get_binding(
        &self,
        name: &str,
    ) -> Result<Option<fluree_db_nameservice::Versioned<fluree_db_nameservice::NameBinding>>> {
        self.inner.get_binding(name).await
    }

    async fn cas_binding(
        &self,
        name: &str,
        expected: Option<u64>,
        new: Option<&fluree_db_nameservice::NameBinding>,
    ) -> Result<fluree_db_nameservice::RegistryCas<fluree_db_nameservice::NameBinding>> {
        self.inner.cas_binding(name, expected, new).await
    }

    async fn list_bindings(
        &self,
    ) -> Result<
        Vec<(
            String,
            fluree_db_nameservice::Versioned<fluree_db_nameservice::NameBinding>,
        )>,
    > {
        self.inner.list_bindings().await
    }

    async fn get_dropped(
        &self,
        instance: &fluree_db_core::InstanceId,
    ) -> Result<Option<fluree_db_nameservice::Versioned<fluree_db_nameservice::DroppedLedger>>>
    {
        self.inner.get_dropped(instance).await
    }

    async fn cas_dropped(
        &self,
        instance: &fluree_db_core::InstanceId,
        expected: Option<u64>,
        new: Option<&fluree_db_nameservice::DroppedLedger>,
    ) -> Result<fluree_db_nameservice::RegistryCas<fluree_db_nameservice::DroppedLedger>> {
        self.inner.cas_dropped(instance, expected, new).await
    }

    async fn list_dropped(
        &self,
    ) -> Result<Vec<fluree_db_nameservice::Versioned<fluree_db_nameservice::DroppedLedger>>> {
        self.inner.list_dropped().await
    }
}

#[async_trait]
impl fluree_db_nameservice::BranchRecordStore for PausingNameService {
    async fn raw_record(&self, ledger_id: &str) -> Result<Option<NsRecord>> {
        self.inner.raw_record(ledger_id).await
    }

    async fn all_raw_records(&self) -> Result<Vec<NsRecord>> {
        self.inner.all_raw_records().await
    }

    async fn insert_record(&self, record: &NsRecord) -> Result<Option<NsRecord>> {
        self.inner.insert_record(record).await
    }

    async fn adopt_record(
        &self,
        ledger_id: &str,
        fence: fluree_db_nameservice::Fence,
    ) -> Result<fluree_db_nameservice::FenceOutcome> {
        self.inner.adopt_record(ledger_id, fence).await
    }

    async fn freeze_record(
        &self,
        ledger_id: &str,
        fence: fluree_db_nameservice::Fence,
    ) -> Result<fluree_db_nameservice::FenceOutcome> {
        self.inner.freeze_record(ledger_id, fence).await
    }

    async fn delete_record(
        &self,
        ledger_id: &str,
        fence: fluree_db_nameservice::Fence,
    ) -> Result<fluree_db_nameservice::FenceOutcome> {
        self.inner.delete_record(ledger_id, fence).await
    }

    async fn adjust_children(
        &self,
        ledger_id: &str,
        fence: fluree_db_nameservice::Fence,
        delta: i32,
    ) -> Result<fluree_db_nameservice::FenceOutcome> {
        self.inner.adjust_children(ledger_id, fence, delta).await
    }
}
