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
//!
//! One case is an ending rather than a failure: a root storage no longer holds
//! at all. The chain stops there, which is what the collector truncating it
//! means. See [`chain_cas_ids`].

use crate::error::{IndexerError, Result};
use crate::gc::collector::PrevIndexChainWalk;
use fluree_db_binary_index::ChainCasIds;
use fluree_db_core::address_path::{ledger_id_to_path_prefix, shared_prefix_for_path};
use fluree_db_core::storage::{candidate_addresses, content_store_for, ContentStore};
use fluree_db_core::{ContentId, Storage};
use std::collections::HashSet;
use std::path::Path;

/// Concurrent storage deletes during a sweep. Deletes are independent
/// round trips, so a serial pass over a large backlog is almost entirely
/// latency.
const RELEASE_CONCURRENCY: usize = 32;

/// Concurrent branch chain walks during planning. A chain is a sequence of
/// dependent reads — each root names the next — so it cannot be walked in
/// parallel with itself, but separate branches are independent chains and
/// overlapping them costs the slowest branch rather than their sum.
const BRANCH_WALK_CONCURRENCY: usize = 8;

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
///
/// `artifact_cache_dir` serves root reads from the local disk cache that
/// builds and the collector already populate, turning the walk's one read per
/// root into a local hit. Cached roots outlive their blobs in two cases —
/// a crash between a release and its cache eviction, or a release by another
/// process — so a cached chain can run past where storage ends it. The walk
/// stops there anyway; see [`chain_cas_ids`].
pub async fn plan_sweep<S>(
    storage: &S,
    ledger_name: &str,
    branches: &[BranchIndexHead],
    artifact_cache_dir: Option<&Path>,
) -> Result<SweepPlan>
where
    S: Storage + Clone,
{
    let method = storage.storage_method().to_string();

    // Walking the chains and listing the prefixes touch disjoint storage and
    // neither informs the other, so planning waits for the slower of the two
    // rather than their sum.
    let (live, scanned) = futures::try_join!(
        live_addresses(storage, &method, branches, artifact_cache_dir),
        swept_addresses(storage, &method, ledger_name, branches),
    )?;

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
    artifact_cache_dir: Option<&Path>,
) -> Result<HashSet<String>>
where
    S: Storage + Clone,
{
    use futures::stream::{StreamExt, TryStreamExt};

    let walks: Vec<_> = branches
        .iter()
        .map(|branch| branch_live_addresses(storage, method, branch, artifact_cache_dir))
        .collect();

    futures::stream::iter(walks)
        .buffer_unordered(BRANCH_WALK_CONCURRENCY)
        .try_fold(HashSet::new(), |mut live, addresses| async move {
            live.extend(addresses);
            Ok(live)
        })
        .await
}

/// Every address one branch's index chain reaches.
async fn branch_live_addresses<S>(
    storage: &S,
    method: &str,
    branch: &BranchIndexHead,
    artifact_cache_dir: Option<&Path>,
) -> Result<HashSet<String>>
where
    S: Storage + Clone,
{
    let Some(head) = branch.index_head_id.as_ref() else {
        return Ok(HashSet::new());
    };
    let store = content_store_for(storage.clone(), &branch.ledger_id);
    let reachable = chain_cas_ids(&store, head, &branch.ledger_id, artifact_cache_dir).await?;

    let mut addresses = HashSet::new();
    for id in &reachable {
        // An unrecognised codec is fatal rather than skipped: the sweep
        // cannot locate the blob, so it cannot establish that any address
        // is safe to delete.
        let candidates = candidate_addresses(method, &branch.ledger_id, id);
        if candidates.is_empty() {
            return Err(IndexerError::StorageRead(format!(
                "cannot locate CID {id} (unrecognised codec {}); refusing to sweep",
                id.codec()
            )));
        }
        addresses.extend(candidates);
    }

    Ok(addresses)
}

/// Every CAS id one branch's chain references, from its head back to the
/// oldest root the chain still reaches.
///
/// Accumulates the whole chain into one set rather than expanding each root on
/// its own: consecutive roots share nearly all of their branch manifests, and
/// [`ChainCasIds`] reads each one once instead of once per root. Deduping at
/// the CID level also keeps address derivation to one pass over the distinct
/// refs rather than one per root.
///
/// Reading roots through a cache can carry the walk past a root the collector
/// released, since the cached copy outlives the blob. Expanding such a root
/// fails — its manifests were released with it — and that failure is read as
/// the end of the chain rather than an error, because storage no longer holds
/// the root and an uncached walk would have stopped there too.
///
/// The set that results is a superset of the uncached walk's, not a match: a
/// root contributes its direct refs before any manifest is read, so the
/// released root's own leave the accumulator holding refs the uncached walk
/// never saw. Whichever of those blobs still exist stay counted live and wait
/// for a later run, which is the direction that costs a deferral rather than
/// a live artifact. The roots beyond the ending were already unreachable and
/// contribute nothing either way.
async fn chain_cas_ids<C>(
    store: &C,
    head: &ContentId,
    ledger_id: &str,
    artifact_cache_dir: Option<&Path>,
) -> Result<HashSet<ContentId>>
where
    C: ContentStore,
{
    let mut chain_ids = ChainCasIds::new();
    let mut walk = PrevIndexChainWalk::new(store, head, artifact_cache_dir);

    // One root at a time: only the CIDs outlive each step, so a long chain
    // costs its distinct refs rather than every decoded root at once.
    while let Some(entry) = walk.next_entry().await? {
        if let Err(e) = chain_ids.add_root(store, &entry.root).await {
            // Absence is the whole test, and it is the same one the walk uses
            // to find the end of a chain. A root that *does* exist but cannot
            // be expanded is a different thing: its refs are unreadable rather
            // than gone, and a live set short of them would classify live
            // artifacts as orphans. If existence cannot be established either,
            // treat the root as present and refuse.
            if !store.has(&entry.root_id).await.unwrap_or(true) {
                tracing::debug!(
                    root_id = %entry.root_id,
                    t = entry.t,
                    ledger_id,
                    "index root released by prior GC outlived its cache entry; chain ends here"
                );
                break;
            }

            return Err(IndexerError::StorageRead(format!(
                "cannot expand index root at t={} for {ledger_id}: {e}; refusing to sweep",
                entry.t
            )));
        }
        chain_ids.insert(entry.root_id);
        if let Some(garbage_id) = entry.garbage_id {
            chain_ids.insert(garbage_id);
        }
    }

    Ok(chain_ids.into_ids())
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

    /// Like [`write_chain`], but each root routes a named graph through a
    /// branch manifest of its own, so expanding a root reads that manifest and
    /// a build superseding one root's manifest leaves the others readable.
    ///
    /// Returns each root's CID paired with the address of the manifest it
    /// routes through.
    async fn write_named_graph_chain(
        storage: &MemoryStorage,
        ledger_id: &str,
        len: i64,
        dict_branch: &ContentId,
    ) -> Vec<(ContentId, String)> {
        let mut chain: Vec<(ContentId, String)> = Vec::new();
        for t in 1..=len {
            // Distinct `g_id` per root, so each manifest is distinct content
            // and lands at an address of its own.
            let manifest = fluree_db_binary_index::format::branch::build_branch_bytes(
                fluree_db_binary_index::RunSortOrder::Spot,
                t as u16,
                &[],
            );
            let (branch_cid, branch_addr) =
                cid_and_addr_for(ledger_id, ContentKind::IndexBranch, &manifest);
            storage.write_bytes(&branch_addr, &manifest).await.unwrap();

            let prev = chain.last().map(|(id, _)| BinaryPrevIndexRef {
                t: t - 1,
                id: id.clone(),
            });
            let (cid, addr) = cid_and_addr_for(
                ledger_id,
                ContentKind::IndexRoot,
                format!("{ledger_id}-ng-root-{t}").as_bytes(),
            );
            let bytes = crate::gc::test_support::fir6_with_named_graph_for(
                ledger_id,
                t,
                prev,
                None,
                dict_branch.clone(),
                Some(branch_cid),
            );
            storage.write_bytes(&addr, &bytes).await.unwrap();
            chain.push((cid, branch_addr));
        }
        chain
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

        let plan = plan_sweep(&storage, NAME, &heads(&[(MAIN, roots.last())]), None)
            .await
            .unwrap();

        assert!(
            plan.orphans.is_empty(),
            "nothing is orphaned while the chain reaches it: {:?}",
            plan.orphans
        );
        assert_eq!(plan.scanned, 4, "3 roots plus the shared dict");
    }

    /// Fail rather than pass vacuously when nothing was cached.
    ///
    /// With disk caching disabled every read falls through to storage, so the
    /// cached and uncached paths become the same path and a test comparing
    /// them proves nothing.
    fn assert_cache_populated(cache_dir: &std::path::Path) {
        let populated = std::fs::read_dir(cache_dir)
            .map(|entries| entries.count() > 0)
            .unwrap_or(false);
        assert!(
            populated,
            "nothing was cached at {}; disk caching is disabled \
             (FLUREE_DISK_CACHE_BUDGET_BYTES?) and this test would prove nothing",
            cache_dir.display()
        );
    }

    /// A temp cache directory of this test's own, emptied before use so a
    /// previous run's entries cannot decide the outcome.
    fn empty_cache_dir(label: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "fluree-test-sweep-cache-{}-{}-{:?}",
            label,
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    /// Reading roots through the cache does not change what a sweep plans
    /// while every root it walks is still in storage.
    #[tokio::test]
    async fn a_cached_plan_matches_an_uncached_one() {
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
        let branches = heads(&[(MAIN, roots.last())]);
        let cache_dir = empty_cache_dir("parity");

        let uncached = plan_sweep(&storage, NAME, &branches, None).await.unwrap();
        let cached = plan_sweep(&storage, NAME, &branches, Some(&cache_dir))
            .await
            .unwrap();
        assert_cache_populated(&cache_dir);

        assert_eq!(cached.orphans, uncached.orphans);
        assert_eq!(cached.live, uncached.live);
        assert_eq!(cached.scanned, uncached.scanned);
    }

    /// The case the cache introduces: a root whose blob is gone but whose
    /// cached copy remains — a crash between a release and its eviction. The
    /// chain must end there, as it would without the cache, rather than
    /// failing the plan on the manifests that were released with it.
    #[tokio::test]
    async fn a_released_root_ends_the_chain_even_when_its_cache_entry_survives() {
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

        // Roots that route a named graph, so expanding one reads its branch
        // manifest — the read that fails once the root has been released.
        let chain = write_named_graph_chain(&storage, MAIN, 3, &dict).await;
        let head = chain.last().map(|(cid, _)| cid);
        let branches = heads(&[(MAIN, head)]);
        let cache_dir = empty_cache_dir("released-root");

        // Prime the cache with the whole chain, then retire the oldest root
        // and the manifest it routed through the way the collector does —
        // but *without* evicting either from the cache, which is what a crash
        // between the two leaves behind.
        plan_sweep(&storage, NAME, &branches, Some(&cache_dir))
            .await
            .unwrap();
        assert_cache_populated(&cache_dir);

        let (oldest_root, oldest_manifest_addr) = &chain[0];
        let oldest_addr = candidate_addresses("memory", MAIN, oldest_root)[0].clone();
        storage.delete(&oldest_addr).await.unwrap();
        storage.delete(oldest_manifest_addr).await.unwrap();

        let cached = plan_sweep(&storage, NAME, &branches, Some(&cache_dir))
            .await
            .expect("a released root ends the chain rather than failing the plan");

        // The retained roots are still reachable, so nothing live is claimed.
        for (root, manifest_addr) in &chain[1..] {
            let root_addr = &candidate_addresses("memory", MAIN, root)[0];
            assert!(
                !cached.orphans.contains(root_addr),
                "a root the chain still reaches must stay live"
            );
            assert!(
                !cached.orphans.contains(manifest_addr),
                "a manifest a live root routes through must stay live"
            );
        }
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

        let plan = plan_sweep(&storage, NAME, &heads(&[(MAIN, Some(&severed))]), None)
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
            None,
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

    /// The collector defers shared dictionary blobs rather than releasing
    /// them, because it cannot see sibling branches. The sweep is the other
    /// half of that division: a dictionary no branch references is reclaimed
    /// here, so the deferral completes rather than leaking (#1548).
    #[tokio::test]
    async fn a_dictionary_no_branch_references_is_reclaimed() {
        let storage = MemoryStorage::new();
        let live = dict_cid(b"live-dict");
        let (_, live_addr) = cid_and_addr_for(
            MAIN,
            ContentKind::DictBlob {
                dict: DictKind::Graphs,
            },
            b"live-dict",
        );
        storage.write_bytes(&live_addr, b"dict").await.unwrap();

        // Superseded by an earlier build and left behind by the collector.
        let (_, stranded_addr) = cid_and_addr_for(
            MAIN,
            ContentKind::DictBlob {
                dict: DictKind::Graphs,
            },
            b"stranded-dict",
        );
        storage.write_bytes(&stranded_addr, b"dict").await.unwrap();

        let roots = write_chain(&storage, MAIN, 2, &live).await;
        let plan = plan_sweep(&storage, NAME, &heads(&[(MAIN, roots.last())]), None)
            .await
            .unwrap();

        assert_eq!(
            plan.orphans,
            vec![stranded_addr.clone()],
            "exactly the unreferenced dictionary is reclaimable"
        );

        execute_sweep(&storage, &plan).await;
        assert!(!storage.exists(&stranded_addr).await.unwrap());
        assert!(
            storage.exists(&live_addr).await.unwrap(),
            "the referenced dictionary survives"
        );
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

        let plan = plan_sweep(&storage, NAME, &heads(&[(MAIN, roots.last())]), None)
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
        let plan = plan_sweep(&storage, NAME, &branches, None).await.unwrap();
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

        let after = plan_sweep(&storage, NAME, &branches, None).await.unwrap();
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
        let plan = plan_sweep(&storage, NAME, &branches, None).await.unwrap();
        assert!(!plan.orphans.is_empty());

        execute_sweep(&storage, &plan).await;
        let replay = execute_sweep(&storage, &plan).await;
        assert!(
            replay.failures.is_empty(),
            "replaying must not error: {:?}",
            replay.failures
        );
    }

    /// Storage that reports an address as present but fails to read it — the
    /// shape of a transient backend failure, which `MemoryStorage` alone
    /// cannot produce because `exists` and `read_bytes` consult one map.
    #[derive(Debug, Clone)]
    struct FailsToReadOne {
        inner: MemoryStorage,
        address: String,
    }

    #[async_trait::async_trait]
    impl fluree_db_core::StorageRead for FailsToReadOne {
        async fn read_bytes(&self, address: &str) -> fluree_db_core::Result<Vec<u8>> {
            if address == self.address {
                return Err(fluree_db_core::error::Error::storage("transient failure"));
            }
            self.inner.read_bytes(address).await
        }

        async fn exists(&self, address: &str) -> fluree_db_core::Result<bool> {
            self.inner.exists(address).await
        }

        async fn list_prefix(&self, prefix: &str) -> fluree_db_core::Result<Vec<String>> {
            self.inner.list_prefix(prefix).await
        }
    }

    #[async_trait::async_trait]
    impl fluree_db_core::StorageWrite for FailsToReadOne {
        async fn write_bytes(&self, address: &str, bytes: &[u8]) -> fluree_db_core::Result<()> {
            self.inner.write_bytes(address, bytes).await
        }

        async fn delete(&self, address: &str) -> fluree_db_core::Result<()> {
            self.inner.delete(address).await
        }
    }

    #[async_trait::async_trait]
    impl fluree_db_core::ContentAddressedWrite for FailsToReadOne {
        async fn content_write_bytes_with_hash(
            &self,
            kind: ContentKind,
            ledger_id: &str,
            content_hash_hex: &str,
            bytes: &[u8],
        ) -> fluree_db_core::Result<fluree_db_core::ContentWriteResult> {
            self.inner
                .content_write_bytes_with_hash(kind, ledger_id, content_hash_hex, bytes)
                .await
        }
    }

    impl fluree_db_core::StorageMethod for FailsToReadOne {
        fn storage_method(&self) -> &str {
            self.inner.storage_method()
        }
    }

    /// A root that still exists but cannot be read is not a chain ending. If
    /// the walk stopped there, the live set would be short by every root
    /// beyond it, and the difference would be reported as orphans and deleted.
    #[tokio::test]
    async fn a_root_that_exists_but_fails_to_read_aborts_the_plan() {
        let inner = MemoryStorage::new();
        let dict = dict_cid(b"live-dict");
        let roots = write_chain(&inner, MAIN, 3, &dict).await;

        let storage = FailsToReadOne {
            address: candidate_addresses("memory", MAIN, &roots[1])[0].clone(),
            inner,
        };

        let result = plan_sweep(&storage, NAME, &heads(&[(MAIN, roots.last())]), None).await;

        assert!(
            result.is_err(),
            "a readable-but-failing root must abort, not truncate the live set"
        );
    }

    /// A prior GC truncates the chain from the oldest end, leaving the
    /// retained boundary's `prev_index` pointing at a root that no longer
    /// exists. That is the normal steady state of any collected ledger, so the
    /// walk must end there rather than fail — otherwise a sweep could never
    /// run on the ledgers it exists for.
    #[tokio::test]
    async fn a_chain_truncated_by_prior_gc_still_plans() {
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

        // Simulate a prior GC having released the oldest root.
        let oldest = candidate_addresses("memory", MAIN, &roots[0]);
        storage.delete(&oldest[0]).await.unwrap();

        let plan = plan_sweep(&storage, NAME, &heads(&[(MAIN, roots.last())]), None)
            .await
            .expect("a collected chain still plans");

        assert!(
            !plan
                .orphans
                .contains(&candidate_addresses("memory", MAIN, &roots[1])[0]),
            "roots still reachable above the truncation stay live"
        );
        assert!(
            !plan.orphans.contains(&dict_addr),
            "the dictionary the chain references stays live"
        );
    }

    /// A root that exists but cannot be decoded is not a chain ending. Ending
    /// there would shorten the live set, and every artifact beyond it would be
    /// reported as an orphan and deleted.
    #[tokio::test]
    async fn an_unreadable_mid_chain_root_aborts_the_plan() {
        let storage = MemoryStorage::new();
        let dict = dict_cid(b"live-dict");
        let roots = write_chain(&storage, MAIN, 3, &dict).await;

        // Corrupt the middle root in place: still present, no longer decodable.
        let middle = candidate_addresses("memory", MAIN, &roots[1]);
        storage
            .write_bytes(&middle[0], b"not a FIR6 root")
            .await
            .unwrap();

        let result = plan_sweep(&storage, NAME, &heads(&[(MAIN, roots.last())]), None).await;

        assert!(
            result.is_err(),
            "planning must fail rather than treat the roots beyond it as orphaned"
        );
    }

    /// An incomplete live set would classify live artifacts as orphans, so a
    /// root that cannot be read aborts planning rather than degrading.
    #[tokio::test]
    async fn an_unreadable_head_aborts_the_plan() {
        let storage = MemoryStorage::new();
        let (missing, _) = cid_and_addr_for(MAIN, ContentKind::IndexRoot, b"never-written");

        let result = plan_sweep(&storage, NAME, &heads(&[(MAIN, Some(&missing))]), None).await;

        assert!(
            result.is_err(),
            "planning must fail rather than report every artifact as orphaned"
        );
    }
}
