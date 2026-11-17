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
use fluree_db_core::{ContentId, LedgerId, StorageBackend};
use fluree_db_nameservice::{NameServiceLookup, NsRecord};
use std::collections::{BTreeSet, HashSet};
use std::path::Path;

/// The other branches of `ledger_id`'s ledger, as the sweep and the collector
/// need them: every record sharing the ledger name except `ledger_id` itself,
/// **retracted branches included**. A soft-dropped branch is restorable
/// until its name is purged, so its dictionaries must survive.
pub fn siblings_of(records: &[NsRecord], ledger_id: &LedgerId) -> Vec<BranchIndexHead> {
    records
        .iter()
        .filter(|r| r.ledger_id.name() == ledger_id.name() && r.ledger_id != *ledger_id)
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

/// The heads, as of now, of the branches in `candidates` that are siblings of
/// `ledger_id`: same ledger name, not `ledger_id` itself, still recorded.
/// Retracted branches are kept, as in [`siblings_of`].
///
/// One `lookup` per sibling, which every backend answers with a consistent
/// read. `candidates` must name every sibling whose chain can reference a
/// blob `ledger_id`'s pass would release; a branch that no longer exists is
/// harmless. Any lookup failure fails the whole call: a set missing a
/// sibling would release blobs it still reads.
pub async fn current_sibling_heads(
    nameservice: &(impl NameServiceLookup + ?Sized),
    ledger_id: &LedgerId,
    candidates: &[LedgerId],
) -> fluree_db_nameservice::Result<Vec<BranchIndexHead>> {
    let siblings: BTreeSet<&LedgerId> = candidates
        .iter()
        .filter(|id| *id != ledger_id && id.name() == ledger_id.name())
        .collect();
    let mut heads = Vec::with_capacity(siblings.len());
    for id in siblings {
        if let Some(record) = nameservice.lookup(id).await? {
            heads.push(BranchIndexHead {
                ledger_id: record.ledger_id,
                index_head_id: record.index_head_id,
            });
        }
    }
    Ok(heads)
}

/// The policy a collector pass on `ledger_id` should run under, given its
/// sibling branches' heads.
///
/// No siblings means nothing is referenced elsewhere and every blob the
/// manifests name is released. Otherwise the siblings' chains are walked;
/// if any cannot be read, the pass defers every shared blob rather than
/// guess, and the next pass tries again.
pub async fn shared_blob_policy_for(
    backend: &StorageBackend,
    siblings: &[BranchIndexHead],
    ledger_id: &LedgerId,
    artifact_cache_dir: Option<&Path>,
) -> SharedBlobPolicy {
    if siblings.is_empty() {
        return SharedBlobPolicy::Release {
            referenced_elsewhere: HashSet::new(),
        };
    }
    match shared_refs_of_branches(backend, siblings, artifact_cache_dir).await {
        Ok(referenced_elsewhere) => {
            tracing::debug!(
                ledger_id = %ledger_id,
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
                ledger_id = %ledger_id,
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

    fn id(s: &str) -> LedgerId {
        LedgerId::parse(s).unwrap()
    }
    use crate::gc::test_support::{cid_and_addr_for, minimal_fir6_for};
    use fluree_db_binary_index::BinaryPrevIndexRef;
    use fluree_db_core::{ContentKind, DictKind, MemoryStorage, StorageWrite};
    use std::sync::Arc;

    fn record(ledger_id: &str, head: Option<ContentId>, retracted: bool) -> NsRecord {
        let mut record = NsRecord::new(ledger_id);
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
        let mut siblings: Vec<LedgerId> = siblings_of(&records, &id("db:main"))
            .into_iter()
            .map(|b| b.ledger_id)
            .collect();
        siblings.sort();
        assert_eq!(siblings, vec!["db:dev", "db:old"]);
    }

    /// Candidates are narrowed to the ledger's other branches, each read
    /// through the nameservice now: a head newer than the candidate list, a
    /// retracted branch kept, a branch that no longer exists dropped.
    #[tokio::test]
    async fn current_sibling_heads_reads_each_sibling_from_the_nameservice() {
        use fluree_db_nameservice::memory::MemoryNameService;
        use fluree_db_nameservice::{IndexPublisher, LedgerLifecycle};

        let ns = MemoryNameService::new();
        let head = ContentId::new(ContentKind::IndexRoot, b"dev head");
        for id in ["db:main", "db:dev", "db:old", "other:main"] {
            ns.create_ledger(id).unwrap();
        }
        ns.publish_index("db:dev", 7, &head).await.unwrap();
        ns.retract("db:old").await.unwrap();

        let candidates: Vec<LedgerId> = [
            "db:dev",
            "db:dev",
            "db:main",
            "db:old",
            "db:gone",
            "other:main",
        ]
        .map(id)
        .to_vec();
        let mut heads = current_sibling_heads(&ns, &id("db:main"), &candidates)
            .await
            .unwrap();
        heads.sort_by(|a, b| a.ledger_id.cmp(&b.ledger_id));

        let ids: Vec<&str> = heads.iter().map(|h| h.ledger_id.as_str()).collect();
        assert_eq!(ids, vec!["db:dev", "db:old"]);
        assert_eq!(heads[0].index_head_id, Some(head));
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
                ledger_id: id("db:dev"),
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
            shared_blob_policy_for(&backend, &siblings_of(&alone, &id("db:main")), &id("db:main"), None).await,
            SharedBlobPolicy::Release { referenced_elsewhere } if referenced_elsewhere.is_empty()
        ));

        // A sibling whose head root is not in storage at all.
        let (missing, _) = cid_and_addr_for("db:dev", ContentKind::IndexRoot, b"missing");
        let with_unreadable = vec![
            record("db:main", None, false),
            record("db:dev", Some(missing), false),
        ];
        assert!(matches!(
            shared_blob_policy_for(
                &backend,
                &siblings_of(&with_unreadable, &id("db:main")),
                &id("db:main"),
                None
            )
            .await,
            SharedBlobPolicy::Defer
        ));
    }
}
