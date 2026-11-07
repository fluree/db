//! Garbage collection for reasoning indexes.
//!
//! An index lives in its own namespace, `{root}/{branch}/@reasoning/{index_id}`,
//! and shares only dictionary blobs with the rest of the ledger. Its chain is
//! its **manifest chain**: each published generation names its predecessor
//! by `prev_manifest`. One reachable set per generation serves every path:
//!
//! - the manifest;
//! - its root and everything the root expands to;
//! - its derived-only set.
//!
//! Two uses:
//! - [`index_sweep_files`]: what a sweep keeps and lists for them.
//! - [`shared_refs_of_indexes`]: the dictionary blobs a branch's indexes reach,
//!   which every pass that releases a shared blob must treat as live.
//! - [`plan_generation_release`] / [`release_generations`]: the index's own
//!   collector, which never overrides the age guard.

use crate::error::{IndexerError, Result};
use crate::gc::collector::is_shared_across_branches;
use fluree_db_binary_index::format::expanded_cas::ChainCasIds;
use fluree_db_binary_index::format::index_root::IndexRoot;
use fluree_db_core::reasoning_index::{ReasoningIndexId, ReasoningIndexes, ReasoningManifest};
use fluree_db_core::{ContentId, ContentStore, StorageBackend, StorageNamespace};
use std::collections::HashSet;

fn decode_root(bytes: &[u8]) -> std::result::Result<IndexRoot, String> {
    IndexRoot::decode(bytes).map_err(|e| e.to_string())
}

/// One published generation, as GC reads it back from storage.
#[derive(Debug, Clone)]
pub struct IndexGeneration {
    pub manifest_id: ContentId,
    pub manifest: ReasoningManifest,
}

/// The generations of an index still in storage, newest first, starting at
/// `newest`.
///
/// The walk ends at a `prev_manifest` that is absent, which is how a chain
/// looks once its oldest generations are released. A manifest that exists but
/// cannot be read fails the walk: a shorter chain would lose references a
/// retained generation still holds.
pub async fn index_generations(
    store: &dyn ContentStore,
    newest: &ContentId,
) -> Result<Vec<IndexGeneration>> {
    let mut out = Vec::new();
    let mut next = Some(newest.clone());
    while let Some(id) = next.take() {
        let bytes = match store.get(&id).await {
            Ok(bytes) => bytes,
            Err(e) if !out.is_empty() && !store.has(&id).await.unwrap_or(true) => {
                tracing::debug!(manifest = %id, error = %e, "manifest chain ends at a released manifest");
                break;
            }
            Err(e) => {
                return Err(IndexerError::StorageRead(format!(
                    "reasoning-index manifest {id}: {e}"
                )))
            }
        };
        let manifest = ReasoningManifest::decode(&bytes).map_err(|e| {
            IndexerError::StorageRead(format!("reasoning-index manifest {id}: {e}"))
        })?;
        next = manifest.prev_manifest.clone();
        out.push(IndexGeneration {
            manifest_id: id,
            manifest,
        });
    }
    Ok(out)
}

/// Everything one generation reaches: its manifest, its root and the root's
/// expansion, and its derived-only set.
pub async fn generation_reach(
    store: &dyn ContentStore,
    generation: &IndexGeneration,
) -> Result<HashSet<ContentId>> {
    let root_bytes = store.get(&generation.manifest.root).await.map_err(|e| {
        IndexerError::StorageRead(format!(
            "reasoning-index root {}: {e}",
            generation.manifest.root
        ))
    })?;
    let root = decode_root(&root_bytes).map_err(|e| {
        IndexerError::StorageRead(format!(
            "reasoning-index root {}: {e}",
            generation.manifest.root
        ))
    })?;
    let mut ids = ChainCasIds::new();
    ids.add_root(store, &root)
        .await
        .map_err(|e| IndexerError::StorageRead(format!("expand reasoning-index root: {e}")))?;
    let mut reach = ids.into_ids();
    reach.insert(generation.manifest.root.clone());
    reach.insert(generation.manifest.derived.clone());
    reach.insert(generation.manifest_id.clone());
    let derived = store.get(&generation.manifest.derived).await.map_err(|e| {
        IndexerError::StorageRead(format!(
            "derived-only set {}: {e}",
            generation.manifest.derived
        ))
    })?;
    let index = fluree_db_core::reasoning_index::DerivedIndex::decode(&derived)
        .map_err(IndexerError::StorageRead)?;
    reach.extend(index.buckets);
    Ok(reach)
}

/// The shared dictionary blobs a root references directly.
async fn root_dict_refs(store: &dyn ContentStore, root_id: &ContentId) -> Result<Vec<ContentId>> {
    let bytes = store
        .get(root_id)
        .await
        .map_err(|e| IndexerError::StorageRead(format!("reasoning-index root {root_id}: {e}")))?;
    let root = decode_root(&bytes)
        .map_err(|e| IndexerError::StorageRead(format!("reasoning-index root {root_id}: {e}")))?;
    Ok(root
        .all_cas_ids()
        .into_iter()
        .filter(is_shared_across_branches)
        .collect())
}

/// Every shared dictionary blob any generation of any index in `indexes`
/// still reaches. `branch` is the namespace of the branch that owns the
/// document. Retired indexes count until their files are cleaned up: they are
/// participants.
///
/// Any unreadable generation fails the call, so a caller defers rather than
/// releases blobs an index may still read.
pub async fn shared_refs_of_indexes(
    backend: &StorageBackend,
    branch: &StorageNamespace,
    indexes: &ReasoningIndexes,
) -> Result<HashSet<ContentId>> {
    let mut refs = HashSet::new();
    for (index_id, entry) in &indexes.indexes {
        let Some(pointer) = &entry.head else {
            continue;
        };
        let store = backend.content_store(&branch.reasoning_index(index_id.as_str()));
        for generation in index_generations(store.as_ref(), &pointer.manifest).await? {
            refs.extend(root_dict_refs(store.as_ref(), &generation.manifest.root).await?);
        }
    }
    Ok(refs)
}

/// Every shared dictionary blob the reasoning indexes of `branches` reach, read
/// from each branch's reasoning-indexes document as it stands. A branch whose document
/// cannot be read fails the call.
pub async fn shared_refs_of_branch_indexes(
    backend: &StorageBackend,
    nameservice: &(impl fluree_db_nameservice::NameServiceLookup + ?Sized),
    branches: &[crate::gc::BranchIndexHead],
) -> Result<HashSet<ContentId>> {
    let mut refs = HashSet::new();
    for branch in branches {
        let doc = nameservice
            .get_reasoning_indexes(branch.ledger_id.as_str())
            .await
            .map_err(|e| IndexerError::NameService(e.to_string()))?;
        if let Some(doc) = doc {
            refs.extend(shared_refs_of_indexes(backend, &branch.namespace, &doc).await?);
        }
    }
    Ok(refs)
}

/// What a sweep keeps and lists for the reasoning indexes of `branches`.
#[derive(Debug, Default)]
pub struct IndexSweepFiles {
    /// One participant per generation still in storage, rooted in its
    /// index's namespace, so every file a retained generation reaches is
    /// live; and one with no root for an index that has published nothing,
    /// so the files of a build that never published are listed too.
    pub participants: Vec<crate::gc::BranchIndexHead>,
    /// Each index's manifests and derived-only sets, which no root reaches:
    /// those of retained generations are live, the rest (a lost publication,
    /// a crash) are reclaimed.
    pub extra: Vec<crate::gc::ExtraSweepFiles>,
}

/// The files of every registered reasoning index of `branches`, retired ones
/// included, for a sweep.
pub async fn index_sweep_files(
    backend: &StorageBackend,
    nameservice: &(impl fluree_db_nameservice::NameServiceLookup + ?Sized),
    branches: &[crate::gc::BranchIndexHead],
) -> Result<IndexSweepFiles> {
    let mut out = IndexSweepFiles::default();
    for branch in branches {
        let doc = nameservice
            .get_reasoning_indexes(branch.ledger_id.as_str())
            .await
            .map_err(|e| IndexerError::NameService(e.to_string()))?;
        let Some(doc) = doc else {
            continue;
        };
        for (index_id, entry) in &doc.indexes {
            let namespace = branch.namespace.reasoning_index(index_id.as_str());
            let store = backend.content_store(&namespace);
            let generations = match &entry.head {
                Some(pointer) => index_generations(store.as_ref(), &pointer.manifest).await?,
                None => Vec::new(),
            };
            let mut live = Vec::new();
            for generation in &generations {
                live.push(generation.manifest_id.clone());
                live.push(generation.manifest.derived.clone());
                let bytes = store.get(&generation.manifest.derived).await.map_err(|e| {
                    IndexerError::StorageRead(format!(
                        "derived-only set {} of a retained generation: {e}; refusing to sweep",
                        generation.manifest.derived
                    ))
                })?;
                let derived = fluree_db_core::reasoning_index::DerivedIndex::decode(&bytes)
                    .map_err(|e| IndexerError::StorageRead(format!("derived-only set: {e}")))?;
                live.extend(derived.buckets);
                out.participants.push(crate::gc::BranchIndexHead {
                    ledger_id: branch.ledger_id.clone(),
                    namespace: namespace.clone(),
                    index_head_id: Some(generation.manifest.root.clone()),
                });
            }
            if generations.is_empty() {
                out.participants.push(crate::gc::BranchIndexHead {
                    ledger_id: branch.ledger_id.clone(),
                    namespace: namespace.clone(),
                    index_head_id: None,
                });
            }
            out.extra.push(crate::gc::ExtraSweepFiles {
                namespace,
                folders: vec!["manifests", "derived"],
                live,
            });
        }
    }
    Ok(out)
}

/// Which generations an index pass keeps and which it may release./// Which generations an index pass keeps and which it may release.
#[derive(Debug, Default)]
pub struct GenerationReleasePlan {
    pub retained: Vec<IndexGeneration>,
    pub released: Vec<IndexGeneration>,
}

/// Split `generations` (newest first) into what a pass keeps and releases.
///
/// The newest generation is always kept. An older one is kept while its
/// supersession time, its successor's `published_at_ms`, is younger than
/// `min_age_ms`. Supersession times are read from storage, so a restart or a
/// configuration change cannot shorten the protection of generations already
/// published. Nothing overrides the guard; publication is throttled instead.
pub fn plan_generation_release(
    generations: Vec<IndexGeneration>,
    now_ms: i64,
    min_age_ms: i64,
) -> GenerationReleasePlan {
    let mut plan = GenerationReleasePlan::default();
    let mut successor_published_at: Option<i64> = None;
    let mut guard_passed = false;
    for generation in generations {
        let keep = match successor_published_at {
            None => true,
            Some(superseded_at) => !guard_passed && now_ms - superseded_at < min_age_ms,
        };
        successor_published_at = Some(generation.manifest.published_at_ms);
        if keep {
            plan.retained.push(generation);
        } else {
            // Everything older was superseded earlier still.
            guard_passed = true;
            plan.released.push(generation);
        }
    }
    plan
}

/// Generations still inside the guard besides the newest, for the
/// publication throttle.
pub fn generations_in_guard(plan: &GenerationReleasePlan) -> usize {
    plan.retained.len().saturating_sub(1)
}

/// What an index pass released.
#[derive(Debug, Default)]
pub struct GenerationReleaseResult {
    pub generations: usize,
    pub released: usize,
    pub deferred_dicts: usize,
    pub failures: usize,
}

/// Release `plan.released`: `reach(released) − reach(retained)`. Shared
/// dictionary blobs among them are released only when absent from
/// `referenced_elsewhere`, the refs of every other participant (the owning
/// branch, its siblings, and the other indexes); `None` defers them all.
///
/// Manifests go last, oldest first, so a crash leaves a chain whose oldest
/// manifest still names files that may be gone; the next walk tolerates it.
pub async fn release_generations(
    store: &dyn ContentStore,
    plan: &GenerationReleasePlan,
    referenced_elsewhere: Option<&HashSet<ContentId>>,
) -> Result<GenerationReleaseResult> {
    let mut result = GenerationReleaseResult {
        generations: plan.released.len(),
        ..Default::default()
    };
    if plan.released.is_empty() {
        return Ok(result);
    }
    let mut retained = HashSet::new();
    for generation in &plan.retained {
        retained.extend(generation_reach(store, generation).await?);
    }
    let mut nodes = Vec::new();
    let mut manifests = Vec::new();
    for generation in plan.released.iter().rev() {
        let reach = match generation_reach(store, generation).await {
            Ok(reach) => reach,
            Err(e) => {
                // A generation an earlier pass half-released: its manifest is
                // all that is left to name it.
                tracing::debug!(manifest = %generation.manifest_id, error = %e, "partial index generation");
                HashSet::from([generation.manifest_id.clone()])
            }
        };
        for id in reach {
            if retained.contains(&id) || id == generation.manifest_id {
                continue;
            }
            if is_shared_across_branches(&id) {
                match referenced_elsewhere {
                    Some(elsewhere) if !elsewhere.contains(&id) => nodes.push(id),
                    _ => result.deferred_dicts += 1,
                }
            } else {
                nodes.push(id);
            }
        }
        manifests.push(generation.manifest_id.clone());
    }
    nodes.sort();
    nodes.dedup();
    let failures = store.release_many(&nodes).await;
    result.failures += failures.len();
    result.released += nodes.len() - failures.len();
    for manifest in manifests {
        match store.release(&manifest).await {
            Ok(()) => result.released += 1,
            Err(_) => result.failures += 1,
        }
    }
    Ok(result)
}

/// The namespaces of every index a branch document names, for listings that
/// must cover them (drop, sweep).
pub fn index_namespaces(
    branch: &StorageNamespace,
    indexes: &ReasoningIndexes,
) -> Vec<(ReasoningIndexId, StorageNamespace)> {
    indexes
        .indexes
        .keys()
        .map(|id| (id.clone(), branch.reasoning_index(id.as_str())))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::reasoning_index::{
        Basis, ClockPolicy, ErasureEpoch, GenerationKind, GenerationStats, SemanticKey,
    };
    use fluree_db_core::ContentKind;

    fn cid(tag: &str) -> ContentId {
        ContentId::new(ContentKind::Commit, tag.as_bytes())
    }

    fn generation(n: u64, published_at_ms: i64) -> IndexGeneration {
        IndexGeneration {
            manifest_id: cid(&format!("m{n}")),
            manifest: ReasoningManifest {
                index_id: ReasoningIndexId::parse("01JC0000000000000000000000").unwrap(),
                ledger_id: "db:main".into(),
                generation: n,
                prev_manifest: None,
                published_at_ms,
                kind: GenerationKind::Logical,
                revision: 1,
                semantics: SemanticKey {
                    program: cid("p"),
                    reasoned_graph: None,
                    clock: ClockPolicy::SnapshotCommitTime,
                },
                settled: cid("settled"),
                engine: 1,
                basis: Basis {
                    commit: cid("c"),
                    t: n as i64,
                    epoch: ErasureEpoch(0),
                },
                root: cid(&format!("r{n}")),
                derived: cid(&format!("d{n}")),
                stats: GenerationStats::default(),
            },
        }
    }

    fn numbers(gens: &[IndexGeneration]) -> Vec<u64> {
        gens.iter().map(|g| g.manifest.generation).collect()
    }

    #[test]
    fn the_newest_generation_is_always_kept() {
        let plan = plan_generation_release(vec![generation(3, 0)], 1_000_000, 10);
        assert_eq!(numbers(&plan.retained), [3]);
        assert!(plan.released.is_empty());
    }

    #[test]
    fn a_generation_is_kept_until_the_guard_has_passed_since_its_successor_published() {
        // Generation 3 published at 950 superseded 2; 2 published at 500
        // superseded 1; 1 published at 100 superseded nothing older.
        let gens = vec![generation(3, 950), generation(2, 500), generation(1, 100)];
        let plan = plan_generation_release(gens, 1000, 100);
        assert_eq!(numbers(&plan.retained), [3, 2]);
        assert_eq!(numbers(&plan.released), [1]);
    }

    #[test]
    fn nothing_overrides_the_guard() {
        // Many generations, all superseded within the guard: all kept.
        let gens: Vec<_> = (1..=50).rev().map(|n| generation(n, 990)).collect();
        let plan = plan_generation_release(gens, 1000, 100);
        assert_eq!(plan.retained.len(), 50);
        assert_eq!(generations_in_guard(&plan), 49);
    }
}
