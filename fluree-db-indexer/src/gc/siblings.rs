//! Which dictionary blobs a ledger's other branches still reach.
//!
//! Dictionary blobs are shared across a ledger's branches (see the module
//! docs on [`crate::gc`]), so before the collector releases one that a
//! manifest names it needs the union of what every *other* branch's index
//! chain references. This module computes that union and turns it into a
//! [`SharedBlobPolicy`].

use crate::error::Result;
use crate::gc::collector::{is_shared_across_branches, PrevIndexChainWalk};
use crate::gc::{BranchIndexHead, SharedBlobPolicy};
use fluree_db_core::{ContentId, StorageBackend};
use fluree_db_nameservice::NsRecord;
use std::collections::HashSet;
use std::path::Path;

/// The other branches of `ledger_id`'s ledger, as the sweep and the collector
/// need them: every record sharing the ledger name except `ledger_id` itself,
/// **retracted branches included**. A soft-dropped branch is restorable
/// until its name is purged, so its dictionaries must survive.
pub fn siblings_of(records: &[NsRecord], ledger_id: &str) -> Vec<BranchIndexHead> {
    let name = records
        .iter()
        .find(|r| r.ledger_id == ledger_id)
        .map(|r| r.name.clone())
        .or_else(|| {
            fluree_db_core::ledger_id::split_ledger_id(ledger_id)
                .ok()
                .map(|(name, _)| name)
        });
    let Some(name) = name else {
        return Vec::new();
    };
    records
        .iter()
        .filter(|r| r.name == name && r.ledger_id != ledger_id)
        .map(|r| BranchIndexHead {
            ledger_id: r.ledger_id.clone(),
            index_head_id: r.index_head_id.clone(),
        })
        .collect()
}

/// Every dictionary blob any of `branches` reaches through its index chain,
/// from its head back to the oldest root the chain still holds.
///
/// The whole chain, not the head: retained versions serve queries that
/// started against them, and a blob only an older retained root references
/// is still live for that branch.
///
/// Each branch is read through its own flat namespace, deliberately. A fork
/// starts from a copy of the source branch's head root, whose `prev_index`
/// names the source's older roots. A branch-aware store would follow that
/// link into the source's chain, and when the source is the branch being
/// collected, its own history would count as "referenced elsewhere" and
/// nothing would ever be released. The flat walk ends at the copied root, and
/// the source's chain is walked as the source's own.
///
/// Dictionary refs sit directly on the root, so no manifest expansion is
/// needed. An unreadable root fails the walk rather than shortening it: a
/// set missing a branch's refs would release blobs that branch still reads.
pub async fn shared_refs_of_branches(
    backend: &StorageBackend,
    branches: &[BranchIndexHead],
    artifact_cache_dir: Option<&Path>,
) -> Result<HashSet<ContentId>> {
    let mut refs = HashSet::new();
    for branch in branches {
        let Some(head) = branch.index_head_id.as_ref() else {
            continue;
        };
        let store = backend.content_store(&branch.ledger_id);
        let mut walk = PrevIndexChainWalk::new(store.as_ref(), head, artifact_cache_dir);
        while let Some(entry) = walk.next_entry().await? {
            refs.extend(
                entry
                    .root
                    .all_cas_ids()
                    .into_iter()
                    .filter(is_shared_across_branches),
            );
        }
    }
    Ok(refs)
}

/// The policy a collector pass on `ledger_id` should run under, given a
/// listing of the ledger's records.
///
/// No siblings means nothing is referenced elsewhere and every blob the
/// manifests name is released. Otherwise the siblings' chains are walked;
/// if any cannot be read, the pass defers every shared blob rather than
/// guess, and the next pass tries again.
pub async fn shared_blob_policy_for(
    backend: &StorageBackend,
    records: &[NsRecord],
    ledger_id: &str,
    artifact_cache_dir: Option<&Path>,
) -> SharedBlobPolicy {
    let siblings = siblings_of(records, ledger_id);
    if siblings.is_empty() {
        return SharedBlobPolicy::Release {
            referenced_elsewhere: HashSet::new(),
        };
    }
    match shared_refs_of_branches(backend, &siblings, artifact_cache_dir).await {
        Ok(referenced_elsewhere) => {
            tracing::debug!(
                ledger_id,
                siblings = siblings.len(),
                referenced_elsewhere = referenced_elsewhere.len(),
                "sibling branches' dictionary refs collected"
            );
            SharedBlobPolicy::Release {
                referenced_elsewhere,
            }
        }
        Err(e) => {
            tracing::warn!(
                ledger_id,
                error = %e,
                "could not read a sibling branch's index chain; deferring shared blobs this pass"
            );
            SharedBlobPolicy::Defer
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gc::test_support::{cid_and_addr_for, minimal_fir6_for};
    use fluree_db_binary_index::BinaryPrevIndexRef;
    use fluree_db_core::{ContentKind, DictKind, MemoryStorage, StorageWrite};
    use std::sync::Arc;

    fn record(ledger_id: &str, head: Option<ContentId>, retracted: bool) -> NsRecord {
        let (name, branch) = fluree_db_core::ledger_id::split_ledger_id(ledger_id).unwrap();
        let mut record = NsRecord::new(name, branch);
        record.index_head_id = head;
        record.retracted = retracted;
        record
    }

    fn dict_cid(data: &[u8]) -> ContentId {
        ContentId::new(
            ContentKind::DictBlob {
                dict: DictKind::Graphs,
            },
            data,
        )
    }

    /// Siblings are the other branches of the same ledger, retracted ones
    /// included; another ledger's branches are not.
    #[test]
    fn siblings_of_keeps_retracted_branches_and_ignores_other_ledgers() {
        let records = vec![
            record("db:main", None, false),
            record("db:dev", None, false),
            record("db:old", None, true),
            record("other:main", None, false),
        ];
        let mut siblings: Vec<String> = siblings_of(&records, "db:main")
            .into_iter()
            .map(|b| b.ledger_id)
            .collect();
        siblings.sort();
        assert_eq!(siblings, vec!["db:dev", "db:old"]);
    }

    /// The union covers every root on a sibling's chain, and the walk stays
    /// inside the sibling's own namespace: a fork's copied root whose
    /// `prev_index` names a root that exists only in the source namespace
    /// ends the fork's chain there.
    #[tokio::test]
    async fn shared_refs_cover_the_whole_sibling_chain_within_its_namespace() {
        let storage = MemoryStorage::new();
        let backend = StorageBackend::Managed(Arc::new(storage.clone()));

        // Source branch history: t=1 in `db:main` only.
        let (main_t1, main_t1_addr) = cid_and_addr_for("db:main", ContentKind::IndexRoot, b"m1");
        let main_only_dict = dict_cid(b"main only");
        storage
            .write_bytes(
                &main_t1_addr,
                &minimal_fir6_for("db:main", 1, None, None, main_only_dict.clone()),
            )
            .await
            .unwrap();

        // Fork `db:dev`: its copied head (t=2) links to main's t=1, then it
        // published t=3 on its own.
        let dev_t2_dict = dict_cid(b"dev t2");
        let (dev_t2, dev_t2_addr) = cid_and_addr_for("db:dev", ContentKind::IndexRoot, b"d2");
        storage
            .write_bytes(
                &dev_t2_addr,
                &minimal_fir6_for(
                    "db:dev",
                    2,
                    Some(BinaryPrevIndexRef {
                        t: 1,
                        id: main_t1.clone(),
                    }),
                    None,
                    dev_t2_dict.clone(),
                ),
            )
            .await
            .unwrap();
        let dev_t3_dict = dict_cid(b"dev t3");
        let (dev_t3, dev_t3_addr) = cid_and_addr_for("db:dev", ContentKind::IndexRoot, b"d3");
        storage
            .write_bytes(
                &dev_t3_addr,
                &minimal_fir6_for(
                    "db:dev",
                    3,
                    Some(BinaryPrevIndexRef {
                        t: 2,
                        id: dev_t2.clone(),
                    }),
                    None,
                    dev_t3_dict.clone(),
                ),
            )
            .await
            .unwrap();

        let refs = shared_refs_of_branches(
            &backend,
            &[BranchIndexHead {
                ledger_id: "db:dev".into(),
                index_head_id: Some(dev_t3),
            }],
            None,
        )
        .await
        .unwrap();

        assert!(refs.contains(&dev_t3_dict), "head refs are live");
        assert!(
            refs.contains(&dev_t2_dict),
            "older retained refs are live too"
        );
        assert!(
            !refs.contains(&main_only_dict),
            "the walk must not follow the fork link into the source namespace"
        );
    }

    /// A single-branch ledger releases everything; a sibling whose chain
    /// cannot be read defers everything.
    #[tokio::test]
    async fn policy_releases_all_without_siblings_and_defers_on_unreadable_sibling() {
        let storage = MemoryStorage::new();
        let backend = StorageBackend::Managed(Arc::new(storage.clone()));

        let alone = vec![record("db:main", None, false)];
        assert!(matches!(
            shared_blob_policy_for(&backend, &alone, "db:main", None).await,
            SharedBlobPolicy::Release { referenced_elsewhere } if referenced_elsewhere.is_empty()
        ));

        // A sibling whose head root is not in storage at all.
        let (missing, _) = cid_and_addr_for("db:dev", ContentKind::IndexRoot, b"missing");
        let with_unreadable = vec![
            record("db:main", None, false),
            record("db:dev", Some(missing), false),
        ];
        assert!(matches!(
            shared_blob_policy_for(&backend, &with_unreadable, "db:main", None).await,
            SharedBlobPolicy::Defer
        ));
    }
}
