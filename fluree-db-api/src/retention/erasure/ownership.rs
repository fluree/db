//! Record erasure ownership at publication, where discovering holders is
//! already necessary. Readers can then consult only their own namespaces.

use fluree_db_core::{ContentId, StorageNamespace};

use super::index::ErasedCommit;
use crate::{Fluree, Result};

impl Fluree {
    /// Called under erasure admission, before publication. Include unborn
    /// branches and dropped ledgers: a stored original can become reachable
    /// later. Subsequent writes of an erased original register their address
    /// through `record_replaced_at` before reporting success.
    pub(super) async fn locate_erasures(
        &self,
        erased: &mut [ErasedCommit],
        restoring: Option<&StorageNamespace>,
    ) -> Result<()> {
        if erased.is_empty() {
            return Ok(());
        }
        let index = &self.erasure_runtime()?.index;
        let names: std::collections::BTreeSet<_> = self
            .nameservice()
            .all_records()
            .await?
            .into_iter()
            .map(|r| r.name)
            .collect();
        let mut namespaces = Vec::new();
        for name in names {
            // A dropped parent branch may still supply a live child's
            // history. The per-ledger listing includes those records too.
            namespaces.extend(
                self.nameservice()
                    .branch_records(&name)
                    .await?
                    .into_iter()
                    .map(|r| r.storage_namespace()),
            );
        }
        for entry in self.publisher()?.list_dropped().await? {
            namespaces.extend(
                entry
                    .value
                    .branches
                    .iter()
                    .map(|b| entry.value.root.namespace(&b.branch)),
            );
        }
        namespaces.extend(restoring.cloned());
        namespaces.sort_by_key(ToString::to_string);
        namespaces.dedup();
        for chain in erased {
            let cid: ContentId = chain
                .commit
                .parse()
                .map_err(|e| crate::ApiError::internal(format!("erased commit: {e}")))?;
            for namespace in &namespaces {
                let address = index.commit_address(namespace, &cid);
                if !chain.replaced_at.contains(&address)
                    && (Some(namespace) == restoring
                        || index.holds_original(namespace, &cid).await?)
                {
                    chain.replaced_at.push(address);
                }
            }
            chain.locations_known = true;
        }
        Ok(())
    }
}
