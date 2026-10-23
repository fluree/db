//! The full-text and vector indexes over a purge's holders, which keep
//! their own copies of what they index: rebuilt from the holders as they
//! are, then every file of theirs but the head's released.

use std::collections::BTreeSet;

use fluree_db_core::{content_address, ContentId, ContentKind, StorageNamespace};
use fluree_db_nameservice::{GraphSourceRecord, GraphSourceType};

use crate::{ApiError, Fluree, Result};

impl Fluree {
    /// Of `sources`, those whose ledger cannot be loaded to rebuild them
    /// from, as a soft-dropped one.
    pub(super) async fn unrebuildable_sources(&self, sources: &[String]) -> Result<Vec<String>> {
        let mut stuck = Vec::new();
        for id in sources {
            let Some(record) = self.nameservice().lookup_graph_source(id).await? else {
                continue;
            };
            let live = match record.dependencies.first() {
                Some(ledger) => self
                    .nameservice()
                    .lookup(ledger)
                    .await?
                    .is_some_and(|r| !r.retracted),
                None => false,
            };
            let rebuildable = record.source_type == GraphSourceType::Bm25
                || (record.source_type == GraphSourceType::Vector && cfg!(feature = "vector"));
            if !live || !rebuildable {
                stuck.push(id.clone());
            }
        }
        Ok(stuck)
    }

    /// Rebuild each of `sources` from its ledger as it is now. Returns how
    /// many it rebuilt.
    pub(super) async fn rebuild_sources(&self, sources: &[String]) -> Result<usize> {
        for id in sources {
            let Some(record) = self.nameservice().lookup_graph_source(id).await? else {
                continue;
            };
            match record.source_type {
                GraphSourceType::Bm25 => {
                    self.rebuild_full_text_index(id).await?;
                }
                #[cfg(feature = "vector")]
                GraphSourceType::Vector => {
                    self.resync_vector_index_with(id, true).await?;
                }
                other => {
                    return Err(ApiError::internal(format!(
                        "{id}: no rebuild for a {other:?} index"
                    )))
                }
            }
        }
        Ok(sources.len())
    }

    /// The files of `sources` their heads do not use: earlier manifests and
    /// snapshots, which hold what the rebuild left out.
    pub(super) async fn source_leftovers(&self, sources: &[String]) -> Result<Vec<String>> {
        let runtime = self.erasure_runtime()?;
        let storage = runtime.index.address_prefix();
        let method = storage
            .strip_prefix("fluree:")
            .and_then(|m| m.strip_suffix("://"))
            .unwrap_or("file")
            .to_string();
        let mut leftovers = Vec::new();
        for id in sources {
            let Some(record) = self.nameservice().lookup_graph_source(id).await? else {
                continue;
            };
            let namespace = StorageNamespace::parse_graph_source(id)
                .map_err(|e| ApiError::internal(format!("{id}: {e}")))?;
            let keep = self.kept_by_head(&record, &method, &namespace).await?;
            for kind in [ContentKind::IndexRoot, ContentKind::GraphSourceSnapshot] {
                let any = content_address(&method, kind, &namespace, "");
                let Some((folder, _)) = any.rsplit_once('/') else {
                    continue;
                };
                for address in runtime.index.list_raw(&format!("{folder}/")).await? {
                    if !keep.contains(&address) {
                        leftovers.push(address);
                    }
                }
            }
        }
        Ok(leftovers)
    }

    /// Delete what [`Self::source_leftovers`] names. Returns how many.
    pub(super) async fn release_source_leftovers(&self, sources: &[String]) -> Result<usize> {
        let runtime = self.erasure_runtime()?;
        let mut deleted = 0;
        for address in self.source_leftovers(sources).await? {
            if runtime.index.delete_raw(&address).await? {
                deleted += 1;
            }
        }
        Ok(deleted)
    }

    /// The addresses `record`'s head uses: the head itself and, for a
    /// full-text index, each snapshot its manifest names.
    async fn kept_by_head(
        &self,
        record: &GraphSourceRecord,
        method: &str,
        namespace: &StorageNamespace,
    ) -> Result<BTreeSet<String>> {
        let mut keep = BTreeSet::new();
        let Some(head) = &record.index_id else {
            return Ok(keep);
        };
        let address = |kind: ContentKind, id: &ContentId| {
            content_address(method, kind, namespace, &id.address_digest())
        };
        keep.insert(address(ContentKind::IndexRoot, head));
        if record.source_type == GraphSourceType::Bm25 {
            let id = record.graph_source_id.to_string();
            let store = self.graph_source_store(&id)?;
            let manifest = self.bm25_manifest_of(&id, Some(record)).await?;
            for entry in &manifest.snapshots {
                let bytes = store.get(&entry.snapshot_id).await?;
                // A chunked snapshot's leaflets are files of the same kind,
                // which this cannot tell apart from an earlier one's.
                if fluree_db_query::bm25::serialize::is_chunked_format(&bytes) {
                    return Err(ApiError::internal(format!(
                        "{id}: a chunked full-text snapshot's files cannot be released"
                    )));
                }
                keep.insert(address(
                    ContentKind::GraphSourceSnapshot,
                    &entry.snapshot_id,
                ));
            }
        }
        Ok(keep)
    }
}
