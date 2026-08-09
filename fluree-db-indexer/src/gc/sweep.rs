//! Storage sweep: find index artifacts no live index chain references.
//!
//! The garbage collector reclaims artifacts by *name* — a root's garbage
//! manifest lists what the previous version replaced, and the collector
//! releases exactly those CIDs. That only reaches artifacts some manifest
//! records. Artifacts orphaned another way are invisible to it:
//!
//! - a root published with no `prev_index` link severed the chain, leaving
//!   everything older unreachable from any walk,
//! - a root published with no garbage manifest leaves the artifacts it
//!   replaced unnameable, which [`clean_garbage`](super::clean_garbage) steps
//!   over and defers here.
//!
//! This module finds those by the opposite method: enumerate what storage
//! actually holds, subtract everything reachable from a live index chain, and
//! treat the remainder as orphaned.
//!
//! # Scope
//!
//! Only index artifacts — every branch's `index/` prefix plus the ledger's
//! shared dict namespace. Commits, transactions, and config blobs are
//! deliberately excluded: they are reachable through the commit chain, not the
//! index chain, so this sweep cannot prove them unreferenced, and being wrong
//! about them destroys the ledger rather than wasting space.
//!
//! # Safety
//!
//! Planning is strict. A root that cannot be read or expanded, a prefix that
//! cannot be listed, or a CID whose codec is unrecognised aborts the plan.
//! Under-counting the live set classifies live artifacts as orphans; the cost
//! of over-counting is only that reclamation waits for the next run.

use crate::error::{IndexerError, Result};
use crate::gc::collector::walk_prev_index_chain_cs;
use fluree_db_binary_index::collect_root_cas_ids_expanded;
use fluree_db_core::address_path::{ledger_id_to_path_prefix, shared_prefix_for_path};
use fluree_db_core::storage::{candidate_addresses, content_store_for};
use fluree_db_core::{ContentId, Storage};
use std::collections::HashSet;

/// Concurrent storage deletes during a sweep. Deletes are independent
/// round trips, so a serial pass over a large backlog is almost entirely
/// latency.
const RELEASE_CONCURRENCY: usize = 32;

/// A branch's published index head, as the sweep needs it.
///
/// The caller selects which branches participate. Every branch of the ledger
/// must be included: dict blobs are shared across branches, so a live set
/// missing one branch would orphan dicts that branch still reads.
#[derive(Debug, Clone)]
pub struct BranchIndexHead {
    /// Full ledger id (`name:branch`), which selects the storage prefix.
    pub ledger_id: String,
    /// The branch's published index root, when it has one.
    pub index_head_id: Option<ContentId>,
}

/// Index artifacts a sweep would reclaim, and what it examined to decide.
#[derive(Debug, Default)]
pub struct SweepPlan {
    /// Addresses no branch's index chain references, sorted.
    pub orphans: Vec<String>,
    /// Distinct addresses found under the swept prefixes.
    pub scanned: usize,
    /// Distinct addresses reachable from a live index chain.
    pub live: usize,
}

/// Outcome of reclaiming a plan's orphans.
#[derive(Debug, Default)]
pub struct SweepResult {
    /// Number of orphaned artifacts released.
    pub reclaimed: usize,
    /// Artifacts that could not be released, each with the reason.
    pub failures: Vec<(String, String)>,
}

/// Determine which index artifacts under `ledger_name` are orphaned.
///
/// Returns a plan; nothing is deleted. Callers that intend to reclaim must
/// hold the ledger's index build excluded for the whole span of planning and
/// deleting — a build publishes its artifacts before the root that references
/// them, so a concurrent build's output is indistinguishable from an orphan.
pub async fn plan_sweep<S>(
    storage: &S,
    ledger_name: &str,
    branches: &[BranchIndexHead],
) -> Result<SweepPlan>
where
    S: Storage + Clone,
{
    let method = storage.storage_method().to_string();
    let live = live_addresses(storage, &method, branches).await?;
    let scanned = swept_addresses(storage, &method, ledger_name, branches).await?;

    let mut orphans: Vec<String> = scanned.difference(&live).cloned().collect();
    orphans.sort();

    tracing::debug!(
        ledger_name,
        branches = branches.len(),
        scanned = scanned.len(),
        live = live.len(),
        orphans = orphans.len(),
        "sweep plan complete"
    );

    Ok(SweepPlan {
        orphans,
        scanned: scanned.len(),
        live: live.len(),
    })
}

/// Release the orphans a plan identified.
///
/// The caller must still hold the ledger's index build excluded. A plan
/// describes storage as it stood when planned, and a build that published in
/// between will have made some of those addresses live.
///
/// A delete that fails is recorded and the sweep moves on. Planning is strict
/// because partial information there misclassifies live artifacts, but a blob
/// that resists deletion is merely a blob that stays put, and the next run
/// retries it. Deletes are idempotent, so re-running a plan is safe.
pub async fn execute_sweep<S>(storage: &S, plan: &SweepPlan) -> SweepResult
where
    S: Storage,
{
    use futures::stream::StreamExt;

    // A backlog is the whole reason a sweep runs, so the delete count is the
    // operation's dominant cost and every one of them is a storage round trip
    // taken while the ledger is held out of indexing.
    let outcomes: Vec<(String, Option<String>)> =
        futures::stream::iter(plan.orphans.iter().cloned())
            .map(|address| async move {
                let error = storage.delete(&address).await.err().map(|e| e.to_string());
                (address, error)
            })
            .buffer_unordered(RELEASE_CONCURRENCY)
            .collect()
            .await;

    let mut result = SweepResult::default();
    for (address, error) in outcomes {
        match error {
            None => result.reclaimed += 1,
            Some(error) => {
                tracing::warn!(
                    address,
                    error,
                    "sweep could not release an orphaned artifact"
                );
                result.failures.push((address, error));
            }
        }
    }
    result.failures.sort();

    tracing::info!(
        reclaimed = result.reclaimed,
        failed = result.failures.len(),
        "sweep complete"
    );

    result
}

/// Every address reachable from any branch's index chain.
///
/// Walks each branch's full chain rather than only its retained window: a root
/// past retention is still referenced until the collector truncates it, and
/// treating it as orphaned here would race that decision.
async fn live_addresses<S>(
    storage: &S,
    method: &str,
    branches: &[BranchIndexHead],
) -> Result<HashSet<String>>
where
    S: Storage + Clone,
{
    let mut live = HashSet::new();

    for branch in branches {
        let Some(head) = branch.index_head_id.as_ref() else {
            continue;
        };
        let store = content_store_for(storage.clone(), &branch.ledger_id);

        // Dedup at the CID level before deriving addresses. Consecutive roots
        // in a chain share nearly all of their CAS refs, so deriving per root
        // would rebuild the same handful of addresses once per root.
        let mut reachable: HashSet<ContentId> = HashSet::new();
        for entry in walk_prev_index_chain_cs(&store, head).await? {
            let expanded = collect_root_cas_ids_expanded(&store, &entry.root)
                .await
                .map_err(|e| {
                    IndexerError::StorageRead(format!(
                        "cannot expand index root at t={} for {}: {e}; refusing to sweep",
                        entry.t, branch.ledger_id
                    ))
                })?;
            reachable.insert(entry.root_id);
            reachable.extend(entry.garbage_id);
            reachable.extend(expanded);
        }

        for id in &reachable {
            // An unrecognised codec is fatal rather than skipped: the sweep
            // cannot locate the blob, so it cannot establish that any address
            // is safe to delete.
            let addresses = candidate_addresses(method, &branch.ledger_id, id);
            if addresses.is_empty() {
                return Err(IndexerError::StorageRead(format!(
                    "cannot locate CID {id} (unrecognised codec {}); refusing to sweep",
                    id.codec()
                )));
            }
            live.extend(addresses);
        }
    }

    Ok(live)
}

/// Every address under the swept prefixes.
///
/// Index artifacts are per-branch, so each branch contributes its own `index/`
/// prefix. Dict blobs are shared across a ledger's branches and live in one
/// namespace, listed once.
async fn swept_addresses<S>(
    storage: &S,
    method: &str,
    ledger_name: &str,
    branches: &[BranchIndexHead],
) -> Result<HashSet<String>>
where
    S: Storage,
{
    let mut prefixes = Vec::with_capacity(branches.len() + 1);
    for branch in branches {
        let path = ledger_id_to_path_prefix(&branch.ledger_id).map_err(|e| {
            IndexerError::StorageRead(format!("invalid ledger id {}: {e}", branch.ledger_id))
        })?;
        prefixes.push(format!("fluree:{method}://{path}/index/"));
    }
    prefixes.push(format!(
        "fluree:{method}://{}/dicts/",
        shared_prefix_for_path(ledger_name)
    ));

    let mut scanned = HashSet::new();
    for prefix in &prefixes {
        let listed = storage.list_prefix(prefix).await.map_err(|e| {
            IndexerError::StorageRead(format!("cannot list {prefix}: {e}; refusing to sweep"))
        })?;
        scanned.extend(listed);
    }

    Ok(scanned)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gc::test_support::{cid_and_addr_for, minimal_fir6_for};
    use fluree_db_binary_index::BinaryPrevIndexRef;
    use fluree_db_core::content_kind::DictKind;
    use fluree_db_core::prelude::*;
    use fluree_db_core::storage::legacy_dict_address;
    use fluree_db_core::ContentKind;

    const NAME: &str = "mydb";
    const MAIN: &str = "mydb:main";

    fn dict_cid(label: &[u8]) -> ContentId {
        ContentId::new(
            ContentKind::DictBlob {
                dict: DictKind::Graphs,
            },
            label,
        )
    }

    /// Write a linked chain of roots `t=1..=len` for one branch, each holding
    /// `dict_branch` as its only CAS reference. Returns the root CIDs.
    async fn write_chain(
        storage: &MemoryStorage,
        ledger_id: &str,
        len: i64,
        dict_branch: &ContentId,
    ) -> Vec<ContentId> {
        let mut roots: Vec<ContentId> = Vec::new();
        for t in 1..=len {
            let prev = roots.last().map(|id: &ContentId| BinaryPrevIndexRef {
                t: t - 1,
                id: id.clone(),
            });
            let (cid, addr) = cid_and_addr_for(
                ledger_id,
                ContentKind::IndexRoot,
                format!("{ledger_id}-root-{t}").as_bytes(),
            );
            let bytes = minimal_fir6_for(ledger_id, t, prev, None, dict_branch.clone());
            storage.write_bytes(&addr, &bytes).await.unwrap();
            roots.push(cid);
        }
        roots
    }

    fn heads(pairs: &[(&str, Option<&ContentId>)]) -> Vec<BranchIndexHead> {
        pairs
            .iter()
            .map(|(ledger_id, head)| BranchIndexHead {
                ledger_id: (*ledger_id).to_string(),
                index_head_id: (*head).cloned(),
            })
            .collect()
    }

    /// Everything an intact chain references stays live, so a healthy ledger
    /// sweeps to nothing.
    #[tokio::test]
    async fn an_intact_chain_yields_no_orphans() {
        let storage = MemoryStorage::new();
        let dict = dict_cid(b"live-dict");
        let (_, dict_addr) = cid_and_addr_for(
            MAIN,
            ContentKind::DictBlob {
                dict: DictKind::Graphs,
            },
            b"live-dict",
        );
        storage.write_bytes(&dict_addr, b"dict").await.unwrap();
        let roots = write_chain(&storage, MAIN, 3, &dict).await;

        let plan = plan_sweep(&storage, NAME, &heads(&[(MAIN, roots.last())]))
            .await
            .unwrap();

        assert!(
            plan.orphans.is_empty(),
            "nothing is orphaned while the chain reaches it: {:?}",
            plan.orphans
        );
        assert_eq!(plan.scanned, 4, "3 roots plus the shared dict");
    }

    /// A root published with no `prev_index` severs the chain, and everything
    /// older becomes unreachable from any walk. Those roots are exactly what
    /// the collector can no longer see and the sweep exists to reclaim.
    #[tokio::test]
    async fn a_severed_chain_leaves_the_older_roots_orphaned() {
        let storage = MemoryStorage::new();
        let dict = dict_cid(b"live-dict");
        let (_, dict_addr) = cid_and_addr_for(
            MAIN,
            ContentKind::DictBlob {
                dict: DictKind::Graphs,
            },
            b"live-dict",
        );
        storage.write_bytes(&dict_addr, b"dict").await.unwrap();
        let stranded = write_chain(&storage, MAIN, 3, &dict).await;

        // The reindex shape before the prev-index link was restored.
        let (severed, severed_addr) =
            cid_and_addr_for(MAIN, ContentKind::IndexRoot, b"severed-root");
        storage
            .write_bytes(
                &severed_addr,
                &minimal_fir6_for(MAIN, 4, None, None, dict.clone()),
            )
            .await
            .unwrap();

        let plan = plan_sweep(&storage, NAME, &heads(&[(MAIN, Some(&severed))]))
            .await
            .unwrap();

        for (t, root) in stranded.iter().enumerate() {
            let addr = candidate_addresses("memory", MAIN, root);
            assert!(
                plan.orphans.contains(&addr[0]),
                "pre-severance root t={} is unreachable and must be reclaimable",
                t + 1
            );
        }
        assert!(
            !plan.orphans.contains(&severed_addr),
            "the live head is never an orphan"
        );
        assert!(
            !plan.orphans.contains(&dict_addr),
            "the dict the live head references is never an orphan"
        );
    }

    /// Dict blobs are shared across a ledger's branches. A dict only the
    /// sibling branch references must survive, which is what makes the
    /// cross-branch union load-bearing rather than a nicety.
    #[tokio::test]
    async fn a_dict_only_a_sibling_branch_references_survives() {
        let storage = MemoryStorage::new();
        let feature = "mydb:feature";

        let main_dict = dict_cid(b"main-dict");
        let feature_dict = dict_cid(b"feature-dict");
        for label in [&b"main-dict"[..], &b"feature-dict"[..]] {
            let (_, addr) = cid_and_addr_for(
                MAIN,
                ContentKind::DictBlob {
                    dict: DictKind::Graphs,
                },
                label,
            );
            storage.write_bytes(&addr, b"dict").await.unwrap();
        }

        let main_roots = write_chain(&storage, MAIN, 1, &main_dict).await;
        let feature_roots = write_chain(&storage, feature, 1, &feature_dict).await;

        let plan = plan_sweep(
            &storage,
            NAME,
            &heads(&[(MAIN, main_roots.last()), (feature, feature_roots.last())]),
        )
        .await
        .unwrap();

        let feature_dict_addr = &candidate_addresses("memory", MAIN, &feature_dict)[0];
        assert!(
            !plan.orphans.contains(feature_dict_addr),
            "a dict the feature branch still references must not be reclaimed"
        );
        assert!(plan.orphans.is_empty(), "nothing else is orphaned either");
    }

    /// On a ledger predating the `@shared` dict migration a live dict's only
    /// copy sits at the per-branch address, which reads still fall back to.
    /// The sweep must recognise it as the same blob.
    #[tokio::test]
    async fn a_live_dict_at_its_legacy_address_survives() {
        let storage = MemoryStorage::new();
        let dict = dict_cid(b"legacy-dict");
        let legacy =
            legacy_dict_address("memory", MAIN, &dict).expect("dict CIDs carry a legacy address");
        // Written only at the pre-migration location.
        storage.write_bytes(&legacy, b"dict").await.unwrap();

        let roots = write_chain(&storage, MAIN, 1, &dict).await;

        let plan = plan_sweep(&storage, NAME, &heads(&[(MAIN, roots.last())]))
            .await
            .unwrap();

        assert!(
            plan.orphans.is_empty(),
            "a live dict at its legacy address must not be reclaimed: {:?}",
            plan.orphans
        );
    }

    /// Reclaiming a severed chain's orphans frees exactly those artifacts and
    /// leaves the live head intact, and a second plan then finds nothing —
    /// the property the whole sweep exists to provide.
    #[tokio::test]
    async fn reclaimed_orphans_leave_the_ledger_swept_clean() {
        let storage = MemoryStorage::new();
        let dict = dict_cid(b"live-dict");
        let (_, dict_addr) = cid_and_addr_for(
            MAIN,
            ContentKind::DictBlob {
                dict: DictKind::Graphs,
            },
            b"live-dict",
        );
        storage.write_bytes(&dict_addr, b"dict").await.unwrap();
        write_chain(&storage, MAIN, 3, &dict).await;

        let (severed, severed_addr) =
            cid_and_addr_for(MAIN, ContentKind::IndexRoot, b"severed-root");
        storage
            .write_bytes(
                &severed_addr,
                &minimal_fir6_for(MAIN, 4, None, None, dict.clone()),
            )
            .await
            .unwrap();

        let branches = heads(&[(MAIN, Some(&severed))]);
        let plan = plan_sweep(&storage, NAME, &branches).await.unwrap();
        assert_eq!(plan.orphans.len(), 3, "the three stranded roots");

        let result = execute_sweep(&storage, &plan).await;
        assert_eq!(result.reclaimed, 3);
        assert!(result.failures.is_empty(), "{:?}", result.failures);

        assert!(
            storage.exists(&severed_addr).await.unwrap(),
            "the live head survives"
        );
        assert!(
            storage.exists(&dict_addr).await.unwrap(),
            "the dict it references survives"
        );

        let after = plan_sweep(&storage, NAME, &branches).await.unwrap();
        assert!(
            after.orphans.is_empty(),
            "a swept ledger has nothing left to reclaim: {:?}",
            after.orphans
        );
    }

    /// Deletes are idempotent, so replaying a plan against already-reclaimed
    /// storage neither fails nor double-counts real work.
    #[tokio::test]
    async fn replaying_a_plan_is_safe() {
        let storage = MemoryStorage::new();
        let dict = dict_cid(b"live-dict");
        write_chain(&storage, MAIN, 2, &dict).await;

        let (severed, severed_addr) =
            cid_and_addr_for(MAIN, ContentKind::IndexRoot, b"severed-root");
        storage
            .write_bytes(&severed_addr, &minimal_fir6_for(MAIN, 3, None, None, dict))
            .await
            .unwrap();

        let branches = heads(&[(MAIN, Some(&severed))]);
        let plan = plan_sweep(&storage, NAME, &branches).await.unwrap();
        assert!(!plan.orphans.is_empty());

        execute_sweep(&storage, &plan).await;
        let replay = execute_sweep(&storage, &plan).await;
        assert!(
            replay.failures.is_empty(),
            "replaying must not error: {:?}",
            replay.failures
        );
    }

    /// An incomplete live set would classify live artifacts as orphans, so a
    /// root that cannot be read aborts planning rather than degrading.
    #[tokio::test]
    async fn an_unreadable_head_aborts_the_plan() {
        let storage = MemoryStorage::new();
        let (missing, _) = cid_and_addr_for(MAIN, ContentKind::IndexRoot, b"never-written");

        let result = plan_sweep(&storage, NAME, &heads(&[(MAIN, Some(&missing))])).await;

        assert!(
            result.is_err(),
            "planning must fail rather than report every artifact as orphaned"
        );
    }
}
