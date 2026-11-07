//! Garbage collection for reasoned heads.
//!
//! A head lives in its own namespace, `{root}/{branch}/@reasoned/{head_id}`,
//! and shares only dictionary blobs with the rest of the ledger. Its chain is
//! its **manifest chain**: each published generation names its predecessor
//! by `prev_manifest`. One reachable set per generation serves every path:
//!
//! - the manifest;
//! - its root and everything the root expands to;
//! - its derived-only set.
//!
//! Two uses:
//! - [`shared_refs_of_heads`]: the dictionary blobs a branch's heads reach,
//!   which every pass that releases a shared blob must treat as live.
//! - [`plan_head_release`] / [`release_head_generations`]: the head's own
//!   collector, which never overrides the age guard.

use crate::error::{IndexerError, Result};
use crate::gc::collector::is_shared_across_branches;
use fluree_db_binary_index::format::expanded_cas::ChainCasIds;
use fluree_db_binary_index::format::index_root::IndexRoot;
use fluree_db_core::reasoned_head::{HeadId, ReasonedHeads, ReasonedManifest};
use fluree_db_core::{ContentId, ContentStore, StorageBackend, StorageNamespace};
use std::collections::HashSet;

fn decode_root(bytes: &[u8]) -> std::result::Result<IndexRoot, String> {
    IndexRoot::decode(bytes).map_err(|e| e.to_string())
}

/// One published generation, as GC reads it back from storage.
#[derive(Debug, Clone)]
pub struct HeadGeneration {
    pub manifest_id: ContentId,
    pub manifest: ReasonedManifest,
}

/// The generations of a head still in storage, newest first, starting at
/// `newest`.
///
/// The walk ends at a `prev_manifest` that is absent, which is how a chain
/// looks once its oldest generations are released. A manifest that exists but
/// cannot be read fails the walk: a shorter chain would lose references a
/// retained generation still holds.
pub async fn head_generations(
    store: &dyn ContentStore,
    newest: &ContentId,
) -> Result<Vec<HeadGeneration>> {
    let mut out = Vec::new();
    let mut next = Some(newest.clone());
    while let Some(id) = next.take() {
        let bytes = match store.get(&id).await {
            Ok(bytes) => bytes,
            Err(e) if !out.is_empty() && !store.has(&id).await.unwrap_or(true) => {
                tracing::debug!(manifest = %id, error = %e, "head chain ends at a released manifest");
                break;
            }
            Err(e) => {
                return Err(IndexerError::StorageRead(format!(
                    "reasoned-head manifest {id}: {e}"
                )))
            }
        };
        let manifest = ReasonedManifest::decode(&bytes)
            .map_err(|e| IndexerError::StorageRead(format!("reasoned-head manifest {id}: {e}")))?;
        next = manifest.prev_manifest.clone();
        out.push(HeadGeneration {
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
    generation: &HeadGeneration,
) -> Result<HashSet<ContentId>> {
    let root_bytes = store.get(&generation.manifest.root).await.map_err(|e| {
        IndexerError::StorageRead(format!(
            "reasoned-head root {}: {e}",
            generation.manifest.root
        ))
    })?;
    let root = decode_root(&root_bytes).map_err(|e| {
        IndexerError::StorageRead(format!(
            "reasoned-head root {}: {e}",
            generation.manifest.root
        ))
    })?;
    let mut ids = ChainCasIds::new();
    ids.add_root(store, &root)
        .await
        .map_err(|e| IndexerError::StorageRead(format!("expand reasoned-head root: {e}")))?;
    let mut reach = ids.into_ids();
    reach.insert(generation.manifest.root.clone());
    reach.insert(generation.manifest.derived.clone());
    reach.insert(generation.manifest_id.clone());
    Ok(reach)
}

/// The shared dictionary blobs a root references directly.
async fn root_dict_refs(store: &dyn ContentStore, root_id: &ContentId) -> Result<Vec<ContentId>> {
    let bytes = store
        .get(root_id)
        .await
        .map_err(|e| IndexerError::StorageRead(format!("reasoned-head root {root_id}: {e}")))?;
    let root = decode_root(&bytes)
        .map_err(|e| IndexerError::StorageRead(format!("reasoned-head root {root_id}: {e}")))?;
    Ok(root
        .all_cas_ids()
        .into_iter()
        .filter(is_shared_across_branches)
        .collect())
}

/// Every shared dictionary blob any generation of any head in `heads`
/// still reaches. `branch` is the namespace of the branch that owns the
/// document. Retired heads count until their files are cleaned up: they are
/// participants.
///
/// Any unreadable generation fails the call, so a caller defers rather than
/// releases blobs a head may still read.
pub async fn shared_refs_of_heads(
    backend: &StorageBackend,
    branch: &StorageNamespace,
    heads: &ReasonedHeads,
) -> Result<HashSet<ContentId>> {
    let mut refs = HashSet::new();
    for (head_id, entry) in &heads.heads {
        let Some(pointer) = &entry.published else {
            continue;
        };
        let store = backend.content_store(&branch.reasoned_head(head_id.as_str()));
        for generation in head_generations(store.as_ref(), &pointer.manifest).await? {
            refs.extend(root_dict_refs(store.as_ref(), &generation.manifest.root).await?);
        }
    }
    Ok(refs)
}

/// Which generations a head pass keeps and which it may release.
#[derive(Debug, Default)]
pub struct HeadReleasePlan {
    pub retained: Vec<HeadGeneration>,
    pub released: Vec<HeadGeneration>,
}

/// Split `generations` (newest first) into what a pass keeps and releases.
///
/// The newest generation is always kept. An older one is kept while its
/// supersession time, its successor's `published_at_ms`, is younger than
/// `min_age_ms`. Supersession times are read from storage, so a restart or a
/// configuration change cannot shorten the protection of generations already
/// published. Nothing overrides the guard; publication is throttled instead.
pub fn plan_head_release(
    generations: Vec<HeadGeneration>,
    now_ms: i64,
    min_age_ms: i64,
) -> HeadReleasePlan {
    let mut plan = HeadReleasePlan::default();
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
pub fn generations_in_guard(plan: &HeadReleasePlan) -> usize {
    plan.retained.len().saturating_sub(1)
}

/// What a head pass released.
#[derive(Debug, Default)]
pub struct HeadReleaseResult {
    pub generations: usize,
    pub released: usize,
    pub deferred_dicts: usize,
    pub failures: usize,
}

/// Release `plan.released`: `reach(released) − reach(retained)`. Shared
/// dictionary blobs among them are released only when absent from
/// `referenced_elsewhere`, the refs of every other participant (the owning
/// branch, its siblings, and the other heads); `None` defers them all.
///
/// Manifests go last, oldest first, so a crash leaves a chain whose oldest
/// manifest still names files that may be gone; the next walk tolerates it.
pub async fn release_head_generations(
    store: &dyn ContentStore,
    plan: &HeadReleasePlan,
    referenced_elsewhere: Option<&HashSet<ContentId>>,
) -> Result<HeadReleaseResult> {
    let mut result = HeadReleaseResult {
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
                tracing::debug!(manifest = %generation.manifest_id, error = %e, "partial head generation");
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

/// The namespaces of every head a branch document names, for listings that
/// must cover them (drop, sweep).
pub fn head_namespaces(
    branch: &StorageNamespace,
    heads: &ReasonedHeads,
) -> Vec<(HeadId, StorageNamespace)> {
    heads
        .heads
        .keys()
        .map(|id| (id.clone(), branch.reasoned_head(id.as_str())))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::reasoned_head::{
        Basis, ClockPolicy, ErasureEpoch, GenerationKind, GenerationStats, SemanticKey,
    };
    use fluree_db_core::ContentKind;

    fn cid(tag: &str) -> ContentId {
        ContentId::new(ContentKind::Commit, tag.as_bytes())
    }

    fn generation(n: u64, published_at_ms: i64) -> HeadGeneration {
        HeadGeneration {
            manifest_id: cid(&format!("m{n}")),
            manifest: ReasonedManifest {
                head_id: HeadId::parse("01JC0000000000000000000000").unwrap(),
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

    fn numbers(gens: &[HeadGeneration]) -> Vec<u64> {
        gens.iter().map(|g| g.manifest.generation).collect()
    }

    #[test]
    fn the_newest_generation_is_always_kept() {
        let plan = plan_head_release(vec![generation(3, 0)], 1_000_000, 10);
        assert_eq!(numbers(&plan.retained), [3]);
        assert!(plan.released.is_empty());
    }

    #[test]
    fn a_generation_is_kept_until_the_guard_has_passed_since_its_successor_published() {
        // Generation 3 published at 950 superseded 2; 2 published at 500
        // superseded 1; 1 published at 100 superseded nothing older.
        let gens = vec![generation(3, 950), generation(2, 500), generation(1, 100)];
        let plan = plan_head_release(gens, 1000, 100);
        assert_eq!(numbers(&plan.retained), [3, 2]);
        assert_eq!(numbers(&plan.released), [1]);
    }

    #[test]
    fn nothing_overrides_the_guard() {
        // Many generations, all superseded within the guard: all kept.
        let gens: Vec<_> = (1..=50).rev().map(|n| generation(n, 990)).collect();
        let plan = plan_head_release(gens, 1000, 100);
        assert_eq!(plan.retained.len(), 50);
        assert_eq!(generations_in_guard(&plan), 49);
    }
}
