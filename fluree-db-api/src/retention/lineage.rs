//! The journal checkpoint each lineage keeps outside its ledgers.
//!
//! A ledger's journal lives in its nameservice record, which a drop or purge
//! removes and which an archive copies as of when it was made. So that a
//! copy restored from an older archive, or after its source is gone, still
//! refuses what was suppressed, every journal change is also merged into a
//! checkpoint keyed by the lineage — the genesis commit every copy of the
//! ledger shares — at `@retention/journal/<lineage>.json`, outside any
//! ledger's folder: drops, purges and the orphan sweep leave it, and ledger
//! backups do not hold it. A restore merges it in before the copy serves.
//!
//! Each write merges this ledger's whole journal into what is there, so a
//! write lost to a concurrent one from another copy is restored by that
//! copy's next change.

use fluree_db_core::{collect_first_parent_cids, ContentId, ContentStore};

use super::journal::Journal;
use super::registry::{self, Registration};
use crate::{Fluree, Result};

const CHECKPOINT_DIR: &str = "@retention/journal";

impl Fluree {
    fn checkpoint_address(&self, lineage: &ContentId) -> Option<String> {
        let storage = self.admin_storage()?;
        Some(format!(
            "fluree:{}://{CHECKPOINT_DIR}/{}.json",
            storage.storage_method(),
            lineage.digest_hex()
        ))
    }

    /// The journal checkpoint for `lineage`, if there is one.
    pub(crate) async fn journal_checkpoint(&self, lineage: &ContentId) -> Option<Journal> {
        let address = self.checkpoint_address(lineage)?;
        let bytes = self.admin_storage()?.read_bytes(&address).await.ok()?;
        serde_json::from_slice(&bytes).ok()
    }

    /// The genesis commit of `ledger_id`: its registration's record of it,
    /// or found by walking the first-parent line.
    async fn ledger_lineage(
        &self,
        ledger_id: &str,
        registration: &Registration,
    ) -> Result<Option<ContentId>> {
        if let Some(lineage) = registration.lineage.as_deref() {
            return Ok(lineage.parse().ok());
        }
        let Some(record) = self.publisher()?.lookup(ledger_id).await? else {
            return Ok(None);
        };
        let Some(head) = record.commit_head_id.clone() else {
            return Ok(None);
        };
        let store = self
            .content_store_for_record_or_id(Some(&record), &record.storage_namespace())
            .await?;
        let line = collect_first_parent_cids(store.as_ref(), &head, 0).await?;
        Ok(line.into_iter().min_by_key(|(t, _)| *t).map(|(_, id)| id))
    }

    /// Merge `ledger_id`'s journal into its lineage's checkpoint, recording
    /// the lineage on the registration the first time. A failure is logged:
    /// the ledger's own journal stays authoritative for it.
    pub(crate) async fn checkpoint_journal(&self, ledger_id: &str) {
        if let Err(e) = self.try_checkpoint_journal(ledger_id).await {
            tracing::warn!(ledger_id, error = %e, "retention: journal checkpoint not written");
        }
    }

    async fn try_checkpoint_journal(&self, ledger_id: &str) -> Result<()> {
        let ns = self.publisher()?;
        let Some(registration) = registry::read(ns, ledger_id).await?.1 else {
            return Ok(());
        };
        if registration.journal.is_empty() {
            return Ok(());
        }
        let Some(lineage) = self.ledger_lineage(ledger_id, &registration).await? else {
            return Ok(());
        };
        let Some(address) = self.checkpoint_address(&lineage) else {
            return Ok(());
        };
        let mut checkpoint = self.journal_checkpoint(&lineage).await.unwrap_or_default();
        if checkpoint.merge(&registration.journal) || checkpoint.generation == 0 {
            let bytes = serde_json::to_vec(&checkpoint)
                .map_err(|e| crate::ApiError::internal(format!("journal checkpoint: {e}")))?;
            self.admin_storage()
                .ok_or_else(|| crate::ApiError::internal("no storage for the journal checkpoint"))?
                .write_bytes(&address, &bytes)
                .await?;
        }
        if registration.lineage.is_none() {
            let lineage = lineage.to_string();
            registry::update(ns, ledger_id, |current| {
                let current = current?;
                current.lineage.is_none().then(|| Registration {
                    lineage: Some(lineage.clone()),
                    ..current.clone()
                })
            })
            .await?;
        }
        Ok(())
    }

    /// Retract, in `ledger_id`, every journaled occurrence it holds that is
    /// still current: a copy restored from an archive made before a removal
    /// holds the removed value as current state, which history suppression
    /// does not cover.
    pub(crate) async fn reapply_removals(&self, ledger_id: &str) -> Result<()> {
        let ns = self.publisher()?;
        let Some(registration) = registry::read(ns, ledger_id).await?.1 else {
            return Ok(());
        };
        if registration.journal.removals.is_empty() {
            return Ok(());
        }
        let Some(record) = ns.lookup(ledger_id).await? else {
            return Ok(());
        };
        let store = self
            .content_store_for_record_or_id(Some(&record), &record.storage_namespace())
            .await?;
        let mut occurrences = Vec::new();
        for removal in &registration.journal.removals {
            let Ok(commit) = removal.commit.parse::<ContentId>() else {
                continue;
            };
            if store.has(&commit).await? {
                occurrences.push(super::removal::OccurrenceRef {
                    commit: removal.commit.clone(),
                    slot: removal.slot,
                });
            }
        }
        if occurrences.is_empty() {
            return Ok(());
        }
        self.remove_occurrences(
            ledger_id,
            super::removal::RemovalRequest {
                occurrences,
                matching: Vec::new(),
            },
        )
        .await?;
        Ok(())
    }
}
