//! Writes for tests that present whatever fence the record carries now.
//!
//! A production writer presents the fence it captured when it loaded the
//! branch, so a writer that loaded it before a drop, restore or replacement
//! is refused (see [`crate::binding`]). A test that just wants to move a head
//! has no loaded branch; these look the fence up first. They create nothing:
//! a write to a missing record is refused, as every write is.

use crate::lifecycle::{self, LifecycleStore};
use crate::NsRecord;
use crate::{
    AdminPublisher, BranchLifecycle, CasResult, CommitPublisher, ConfigCasResult, ConfigPublisher,
    ConfigValue, Fence, IndexPublisher, NameServiceLookup, NsRecordSnapshot, RefKind, RefPublisher,
    RefValue, Result, StatusCasResult, StatusPublisher, StatusValue,
};
use async_trait::async_trait;
use fluree_db_core::{ContentId, LedgerId};

/// Create `ledger_id`: the ledger, when nothing holds its name, or else a
/// branch of it from its root branch.
pub async fn create<S: LifecycleStore + ?Sized>(store: &S, ledger_id: &str) -> Result<NsRecord> {
    let id = LedgerId::parse(ledger_id)?;
    match store.get_binding(id.name()).await? {
        None => lifecycle::create_ledger(store, &id).await,
        Some(binding) => {
            lifecycle::create_branch(
                store,
                &id.ledger_name(),
                id.branch(),
                &binding.value.root_branch,
                None,
            )
            .await
        }
    }
}

/// Create `ledger_id` rooted at its name, as a ledger migrated from before
/// name bindings is: its data lives at `{name}/{branch}/…`, where
/// [`StorageNamespace::parse_legacy`](fluree_db_core::StorageNamespace::parse_legacy)
/// puts it. A second branch of the name joins the first.
pub async fn create_at_name_root<S: LifecycleStore + ?Sized>(
    store: &S,
    ledger_id: &str,
) -> Result<NsRecord> {
    let id = LedgerId::parse(ledger_id)?;
    if store
        .insert_record(&NsRecord::new(id.clone()))
        .await?
        .is_some()
    {
        return Err(crate::NameServiceError::ledger_already_exists(ledger_id));
    }
    lifecycle::migrate_legacy(store).await?;
    crate::read_resolved(store, store.raw_record(ledger_id).await?)
        .await?
        .ok_or_else(|| crate::NameServiceError::not_found(ledger_id))
}

/// The fence `ledger_id`'s record carries now, if it has one.
pub async fn current_fence<S: NameServiceLookup + ?Sized>(
    store: &S,
    ledger_id: &str,
) -> Result<Option<Fence>> {
    Ok(store.lookup(ledger_id).await?.and_then(|r| r.fence))
}

/// The publisher writes, presenting the record's current fence.
#[async_trait]
pub trait CurrentFence: NameServiceLookup {
    async fn publish_commit(&self, ledger_id: &str, commit_t: i64, id: &ContentId) -> Result<()>
    where
        Self: CommitPublisher,
    {
        let fence = current_fence(self, ledger_id).await?;
        self.publish_commit_fenced(ledger_id, fence, commit_t, id)
            .await
    }

    async fn publish_index(&self, ledger_id: &str, index_t: i64, id: &ContentId) -> Result<()>
    where
        Self: IndexPublisher,
    {
        let fence = current_fence(self, ledger_id).await?;
        self.publish_index_fenced(ledger_id, fence, index_t, id)
            .await
    }

    async fn publish_index_allow_equal(
        &self,
        ledger_id: &str,
        index_t: i64,
        id: &ContentId,
    ) -> Result<()>
    where
        Self: AdminPublisher,
    {
        let fence = current_fence(self, ledger_id).await?;
        self.publish_index_allow_equal_fenced(ledger_id, fence, index_t, id)
            .await
    }

    async fn compare_and_set_ref(
        &self,
        ledger_id: &str,
        kind: RefKind,
        expected: Option<&RefValue>,
        new: &RefValue,
    ) -> Result<CasResult>
    where
        Self: RefPublisher,
    {
        let fence = current_fence(self, ledger_id).await?;
        self.compare_and_set_ref_fenced(ledger_id, fence, kind, expected, new)
            .await
    }

    async fn fast_forward_commit(
        &self,
        ledger_id: &str,
        new: &RefValue,
        max_retries: usize,
    ) -> Result<CasResult>
    where
        Self: RefPublisher,
    {
        let fence = current_fence(self, ledger_id).await?;
        self.fast_forward_commit_fenced(ledger_id, fence, new, max_retries)
            .await
    }

    async fn reset_head(&self, ledger_id: &str, snapshot: NsRecordSnapshot) -> Result<()>
    where
        Self: BranchLifecycle,
    {
        let fence = current_fence(self, ledger_id).await?;
        self.reset_head_fenced(ledger_id, fence, snapshot).await
    }

    async fn push_status(
        &self,
        ledger_id: &str,
        expected: Option<&StatusValue>,
        new: &StatusValue,
    ) -> Result<StatusCasResult>
    where
        Self: StatusPublisher,
    {
        let fence = current_fence(self, ledger_id).await?;
        self.push_status_fenced(ledger_id, fence, expected, new)
            .await
    }

    async fn push_config(
        &self,
        ledger_id: &str,
        expected: Option<&ConfigValue>,
        new: &ConfigValue,
    ) -> Result<ConfigCasResult>
    where
        Self: ConfigPublisher,
    {
        let fence = current_fence(self, ledger_id).await?;
        self.push_config_fenced(ledger_id, fence, expected, new)
            .await
    }
}

impl<T: NameServiceLookup + ?Sized> CurrentFence for T {}
