//! Releasing the indexes of soft-dropped ledgers that hold an erased commit.
//! A dropped ledger cannot be rebuilt in place, so its drop entry is marked
//! for a rebuild on restore, with its index heads cleared, before its index
//! files and dictionaries are deleted. A restore rebuilds the marked
//! branches from their commits, which read as their replacements, before the
//! ledger is visible.

use std::collections::BTreeSet;

use fluree_db_core::{ContentStore, LedgerId, StorageNamespace};
use fluree_db_nameservice::{DroppedLedger, DroppedState, NsRecord, RegistryCas, Versioned};

use crate::{Fluree, Result};

/// The drop entry field naming the branches a restore rebuilds.
const REBUILD: &str = "rebuildIndexes";

/// The branches of `entry` whose indexes a restore rebuilds.
pub(crate) fn released_branches(entry: &DroppedLedger) -> BTreeSet<String> {
    entry
        .extra
        .get(REBUILD)
        .and_then(|v| serde_json::from_value(v.clone()).ok())
        .unwrap_or_default()
}

/// What releasing the dropped holders did.
pub(super) enum Released {
    /// The holders released, and the files deleted.
    Done { ledgers: usize, deleted: usize },
    /// An entry is being restored, or changed meanwhile: plan again.
    Changed,
}

impl Fluree {
    /// The soft-dropped ledgers at rest, holding the original of a commit
    /// of `commits`: one being restored or purged is left alone.
    async fn dropped_at_rest(&self, commits: &[String]) -> Result<Vec<Versioned<DroppedLedger>>> {
        let commits: Vec<&str> = commits.iter().map(String::as_str).collect();
        self.dropped_holding(&commits, |state| matches!(state, DroppedState::Dropped))
            .await
    }

    /// Where `entry`'s index files and dictionaries are.
    fn index_prefixes(&self, entry: &DroppedLedger) -> Result<Vec<String>> {
        let storage = self.erasure_runtime()?.index.address_prefix();
        let mut prefixes: Vec<String> = entry
            .branches
            .iter()
            .map(|b| {
                let namespace = StorageNamespace::new(entry.root.clone(), &b.branch);
                format!("{storage}{}/index/", namespace.branch_prefix())
            })
            .collect();
        prefixes.push(format!("{storage}{}/dicts/", entry.root.shared_prefix()));
        Ok(prefixes)
    }

    /// Release the indexes of the soft-dropped ledgers holding the original
    /// of a commit of `commits`: every branch's, and the ledger's
    /// dictionaries. A ledger being purged is left alone. One being restored
    /// holds the release back: a restore that finds its name taken leaves it
    /// dropped, with an index no later pass would release.
    pub(super) async fn release_dropped(&self, commits: &[String]) -> Result<Released> {
        let runtime = self.erasure_runtime()?;
        let registry = self.publisher()?;
        let commits: Vec<&str> = commits.iter().map(String::as_str).collect();
        let holding = self
            .dropped_holding(&commits, |state| !matches!(state, DroppedState::Purging))
            .await?;
        if holding
            .iter()
            .any(|v| matches!(v.value.state, DroppedState::Restoring { .. }))
        {
            return Ok(Released::Changed);
        }
        let (mut ledgers, mut deleted) = (0, 0);
        for versioned in holding {
            let entry = versioned.value;
            let mut next = entry.clone();
            let mut rebuild = released_branches(&entry);
            let mut changed = false;
            for branch in &mut next.branches {
                if branch.index_head_id.take().is_some() {
                    if !branch.retracted {
                        rebuild.insert(branch.branch.clone());
                    }
                    branch.index_t = 0;
                    changed = true;
                }
            }
            // Marked before anything is deleted: a restore after a crash
            // here rebuilds what it cannot load.
            if changed {
                next.extra
                    .insert(REBUILD.to_string(), serde_json::json!(rebuild));
                let cas = registry
                    .cas_dropped(&entry.instance, Some(versioned.version), Some(&next))
                    .await?;
                if matches!(cas, RegistryCas::Conflict { .. }) {
                    return Ok(Released::Changed);
                }
            }
            #[cfg(any(test, feature = "testing"))]
            super::hooks::hold(super::hooks::Hold::Marked, entry.name.as_str()).await;
            for prefix in self.index_prefixes(&entry)? {
                for address in runtime.index.list_raw(&prefix).await? {
                    if runtime.index.delete_raw(&address).await? {
                        deleted += 1;
                    }
                }
            }
            ledgers += 1;
        }
        Ok(Released::Done { ledgers, deleted })
    }

    /// Whether retracted `branch` still has an index head: one a sweep keeps.
    pub(super) async fn frozen_index_head(&self, branch: &str) -> Result<bool> {
        Ok(self
            .nameservice()
            .lookup(branch)
            .await?
            .is_some_and(|r| r.index_head_id.is_some()))
    }

    /// The index files still stored for retracted `branches`, which the
    /// sweep releases once their heads are cleared.
    pub(super) async fn retracted_index_files(&self, branches: &[String]) -> Result<Vec<String>> {
        let runtime = self.erasure_runtime()?;
        let storage = runtime.index.address_prefix();
        let mut files = Vec::new();
        for branch in branches {
            if let Some(record) = self.nameservice().lookup(branch).await? {
                let prefix = format!(
                    "{storage}{}/index/",
                    record.storage_namespace().branch_prefix()
                );
                files.extend(runtime.index.list_raw(&prefix).await?);
            }
        }
        Ok(files)
    }

    /// The index files and dictionaries still stored for the soft-dropped
    /// ledgers holding the original of a commit of `commits`.
    pub(super) async fn dropped_index_files(&self, commits: &[String]) -> Result<Vec<String>> {
        let runtime = self.erasure_runtime()?;
        let mut files = Vec::new();
        for versioned in self.dropped_at_rest(commits).await? {
            for prefix in self.index_prefixes(&versioned.value)? {
                files.extend(runtime.index.list_raw(&prefix).await?);
            }
        }
        Ok(files)
    }

    /// Rebuild the index of `record`, a restored branch not yet visible
    /// among `branches`, whose index a purge released: from its commits,
    /// which read as their replacements. Then again with the full-text
    /// properties its ledger config names, if it names any: a branch with no
    /// index has no state to read them from before.
    pub(crate) async fn rebuild_released_index(
        &self,
        record: &NsRecord,
        branches: &[NsRecord],
    ) -> Result<()> {
        let store: std::sync::Arc<dyn ContentStore> =
            std::sync::Arc::new(self.store_among(branches, record));
        let ledger_id = LedgerId::parse(&record.ledger_id)?;
        let mut config = self
            .rebuild_config(&ledger_id, None, Default::default())
            .await?;
        let mut published = record.clone();
        loop {
            let built = fluree_db_indexer::rebuild_index_from_commits_with_tracker(
                store.clone(),
                fluree_db_core::Tracker::disabled(),
                record.ledger_id.as_str(),
                &published,
                config.clone(),
            )
            .await?;
            self.publisher()?
                .publish_index_allow_equal_fenced(
                    record.ledger_id.as_str(),
                    record.write_basis(),
                    built.index_t,
                    &built.root_id,
                )
                .await?;
            published.index_head_id = Some(built.root_id);
            published.index_t = built.index_t;
            if !config.fulltext_configured_properties.is_empty() {
                return Ok(());
            }
            let state = self
                .load_unlisted_state(store.clone(), published.clone())
                .await?;
            let with_state = self
                .rebuild_config(&ledger_id, Some(&state), Default::default())
                .await?;
            if with_state.fulltext_configured_properties.is_empty() {
                return Ok(());
            }
            config = with_state;
        }
    }
}
