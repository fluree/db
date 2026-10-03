//! Expanded CAS reachability for an `IndexRoot`.
//!
//! `IndexRoot::all_cas_ids()` returns only the CIDs the root references
//! directly. Several use cases — drop / unpin, pack / branch transfer,
//! garbage-record diff production — also need the CIDs sitting behind
//! the root's branch manifests:
//!
//! - **Named-graph branches** (`FBR3`) routing to leaf + sidecar CIDs.
//! - **Legacy annotation-arena branches**, which old roots still name and
//!   whose leaves stay reachable until a build releases them.
//!
//! This module owns the single async expansion path so callers stay in
//! lockstep when new branch-shaped artifacts are added to the root.
//!
//! ## Strict vs tolerant
//!
//! Two entry points with different correctness contracts:
//!
//! - [`collect_root_cas_ids_expanded`] — **strict.** Returns
//!   `Err` on the first branch read or decode failure. Use when an
//!   incomplete reachability set would corrupt the caller's invariant
//!   (pack / branch-copy: missing leaves yield a non-self-contained
//!   index snapshot; garbage-record diff: missing leaves on the *new*
//!   root would misclassify still-reachable blobs as garbage).
//!
//! - [`collect_root_cas_ids_expanded_tolerant`] — **best-effort.**
//!   Logs and skips per-branch failures, returning whatever it could
//!   collect. Use only when partial coverage is strictly safer than
//!   bailing out — e.g. `drop_ledger`'s CID-walk fallback, where
//!   skipping a leaf only means a stray pin survives, never data
//!   corruption.
//!
//! ## Use sites (must all stay in sync)
//!
//! - `fluree-db-indexer::drop::collect_index_chain_cids` — drop / unpin
//!   (tolerant).
//! - `fluree-db-indexer::build::root_assembly::superseded_cids` —
//!   garbage-record diff (strict).
//! - `fluree-db-indexer::gc::sweep::live_addresses` — storage-sweep live
//!   set (strict, chain-wide via [`ChainCasIds`]).
//! - `fluree-db-api::pack::compute_missing_index_artifacts` — pack
//!   transfer (strict).
//! - `fluree-db-api::ledger::loading::copy_index_to_branch` — branch
//!   fork (strict).

use std::collections::HashSet;

use fluree_db_core::content_id::ContentId;
use fluree_db_core::storage::ContentStore;
use fluree_db_core::{Error, Result};

use crate::format::branch::read_branch_from_bytes;
use crate::format::index_root::{IndexRoot, LegacyAnnotationArena};

/// Strict expansion: returns the complete reachable CAS set or an error.
///
/// Starts from `root.all_cas_ids()` and additionally fetches every
/// named-graph branch + legacy arena branch from `store`, decoding
/// each manifest to discover the leaf (and named-graph sidecar) CIDs
/// they route to. The first read or decode failure short-circuits and
/// returns `Err` — partial sets are never returned.
///
/// Does NOT include the root's own CID, the garbage manifest CID, the
/// `prev_index` link, or anything older in the chain. Callers composing
/// a set over several roots of one chain should use [`ChainCasIds`]
/// rather than unioning per-root results, which re-reads every manifest
/// the roots share.
pub async fn collect_root_cas_ids_expanded(
    store: &dyn ContentStore,
    root: &IndexRoot,
) -> Result<HashSet<ContentId>> {
    let mut chain_ids = ChainCasIds::new();
    chain_ids.add_root(store, root).await?;
    Ok(chain_ids.into_ids())
}

/// The CAS ids reachable from a run of roots in one index chain, expanded
/// root by root.
///
/// Consecutive roots share nearly all of their branch manifests: an
/// incremental build rewrites only the branches whose leaves changed, and
/// carries the rest over by CID. Expanding each root on its own therefore
/// fetches and decodes the shared manifests once per root, paying
/// `O(roots × manifests)` reads for `O(distinct manifests)` of routing
/// information. This remembers which manifests it has already expanded and
/// reads each one once.
///
/// Skipping a repeat manifest is sound only because its leaves are already
/// in the same set. An instance therefore covers exactly one accumulation —
/// callers keeping per-branch sets need one instance per branch.
///
/// Callers that *subtract* one root's set from another's — the garbage-record
/// diff in `fluree-db-indexer::build::root_assembly::superseded_cids` —
/// must expand each root through an instance of its own. Sharing one yields
/// an empty difference, and the dedup means the second root's shared
/// manifests are never even read.
///
/// Expansion is strict, matching [`collect_root_cas_ids_expanded`]: the
/// first read or decode failure returns `Err`.
#[derive(Debug, Default)]
pub struct ChainCasIds {
    ids: HashSet<ContentId>,
    /// Manifests whose leaves are already in `ids`. Separate from `ids`
    /// because a manifest's own CID lands there via `all_cas_ids()` before
    /// anything routes through it, so `ids` cannot say whether it was read.
    expanded_manifests: HashSet<ContentId>,
}

impl ChainCasIds {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add every CAS id `root` reaches, expanding the branch manifests this
    /// chain has not expanded already.
    pub async fn add_root(&mut self, store: &dyn ContentStore, root: &IndexRoot) -> Result<()> {
        self.ids.extend(root.all_cas_ids());

        for named_graph in &root.named_graphs {
            for (_, branch_cid) in &named_graph.orders {
                self.expand_named_graph_branch(store, branch_cid).await?;
            }
        }

        if let Some(ref arena) = root.legacy_annotation_arena {
            for branch_cid in arena.branches() {
                self.expand_legacy_arena_branch(store, branch_cid).await?;
            }
        }

        Ok(())
    }

    /// Add an id reachable from the chain but not from any root's contents,
    /// such as a root's own CID or its garbage manifest.
    pub fn insert(&mut self, id: ContentId) {
        self.ids.insert(id);
    }

    pub fn into_ids(self) -> HashSet<ContentId> {
        self.ids
    }

    /// Named-graph branch (`FBR3`) → leaf + sidecar CIDs.
    async fn expand_named_graph_branch(
        &mut self,
        store: &dyn ContentStore,
        branch_cid: &ContentId,
    ) -> Result<()> {
        let Some(bytes) = self
            .read_unexpanded_manifest(store, branch_cid, "named-graph branch")
            .await?
        else {
            return Ok(());
        };
        let manifest = read_branch_from_bytes(&bytes).map_err(|e| {
            Error::invalid_index(format!(
                "failed to decode named-graph branch {branch_cid} during CID expansion: {e}"
            ))
        })?;

        for leaf in &manifest.leaves {
            self.ids.insert(leaf.leaf_cid.clone());
            if let Some(ref sidecar_cid) = leaf.sidecar_cid {
                self.ids.insert(sidecar_cid.clone());
            }
        }
        self.expanded_manifests.insert(branch_cid.clone());

        Ok(())
    }

    /// Legacy annotation-arena branch → leaf CIDs.
    async fn expand_legacy_arena_branch(
        &mut self,
        store: &dyn ContentStore,
        branch_cid: &ContentId,
    ) -> Result<()> {
        let Some(bytes) = self
            .read_unexpanded_manifest(store, branch_cid, "legacy annotation arena branch")
            .await?
        else {
            return Ok(());
        };
        self.ids
            .extend(decode_legacy_arena_branch(branch_cid, &bytes)?);
        self.expanded_manifests.insert(branch_cid.clone());
        Ok(())
    }

    /// Fetch a manifest's bytes, or `None` when its leaves are already
    /// accounted for. `kind` names the artifact in read failures.
    async fn read_unexpanded_manifest(
        &self,
        store: &dyn ContentStore,
        manifest_cid: &ContentId,
        kind: &str,
    ) -> Result<Option<Vec<u8>>> {
        if self.expanded_manifests.contains(manifest_cid) {
            return Ok(None);
        }
        let bytes = store.get(manifest_cid).await.map_err(|e| {
            Error::invalid_index(format!(
                "failed to read {kind} {manifest_cid} during CID expansion: {e}"
            ))
        })?;

        Ok(Some(bytes))
    }
}

/// Tolerant expansion: logs and skips per-branch failures.
///
/// Returns whatever could be collected, including the root's direct
/// CAS refs even if every branch fails to expand. Suitable only for
/// best-effort cleanup paths (drop / unpin) where leaving an extra
/// blob behind is strictly safer than bailing out.
///
/// Pack / branch-copy / garbage-diff callers must use the strict
/// [`collect_root_cas_ids_expanded`] instead — silently dropping
/// reachable leaves there yields incomplete snapshots or misclassified
/// garbage.
pub async fn collect_root_cas_ids_expanded_tolerant(
    store: &dyn ContentStore,
    root: &IndexRoot,
) -> HashSet<ContentId> {
    let mut ids: HashSet<ContentId> = root.all_cas_ids().into_iter().collect();

    for ng in &root.named_graphs {
        for (_, branch_cid) in &ng.orders {
            match store.get(branch_cid).await {
                Ok(bytes) => match read_branch_from_bytes(&bytes) {
                    Ok(manifest) => {
                        for leaf in &manifest.leaves {
                            ids.insert(leaf.leaf_cid.clone());
                            if let Some(ref sc) = leaf.sidecar_cid {
                                ids.insert(sc.clone());
                            }
                        }
                    }
                    Err(e) => tracing::warn!(
                        branch_cid = %branch_cid,
                        error = %e,
                        "failed to decode named-graph branch during CID expansion, skipping"
                    ),
                },
                Err(e) => tracing::warn!(
                    branch_cid = %branch_cid,
                    error = %e,
                    "failed to read named-graph branch during CID expansion, skipping"
                ),
            }
        }
    }

    if let Some(ref arena) = root.legacy_annotation_arena {
        for branch_cid in arena.branches() {
            let leaves = match store.get(branch_cid).await {
                Ok(bytes) => decode_legacy_arena_branch(branch_cid, &bytes),
                Err(e) => Err(e),
            };
            match leaves {
                Ok(leaves) => ids.extend(leaves),
                Err(e) => tracing::warn!(
                    branch_cid = %branch_cid,
                    error = %e,
                    "failed to expand legacy annotation arena branch, skipping"
                ),
            }
        }
    }

    ids
}

/// Every blob of a legacy annotation arena: both branches and their leaves.
/// A build over a root that names one releases these, since the root it
/// writes does not.
pub async fn legacy_annotation_arena_cids(
    store: &dyn ContentStore,
    arena: &LegacyAnnotationArena,
) -> Result<Vec<ContentId>> {
    let mut ids = Vec::new();
    for branch_cid in arena.branches() {
        let bytes = store.get(branch_cid).await.map_err(|e| {
            Error::invalid_index(format!(
                "failed to read legacy annotation arena branch {branch_cid}: {e}"
            ))
        })?;
        ids.extend(decode_legacy_arena_branch(branch_cid, &bytes)?);
        ids.push(branch_cid.clone());
    }
    Ok(ids)
}

/// The leaf CIDs a legacy arena branch routes to. Both branch kinds frame a
/// CBOR body behind the same 12-byte header (length at bytes 8..12) and name
/// each leaf `leaf_cid`.
fn decode_legacy_arena_branch(branch_cid: &ContentId, bytes: &[u8]) -> Result<Vec<ContentId>> {
    #[derive(serde::Deserialize)]
    struct Branch {
        leaves: Vec<Leaf>,
    }
    #[derive(serde::Deserialize)]
    struct Leaf {
        leaf_cid: ContentId,
    }
    let invalid = |e: &dyn std::fmt::Display| {
        Error::invalid_index(format!(
            "failed to decode legacy annotation arena branch {branch_cid}: {e}"
        ))
    };
    let body = bytes
        .get(8..12)
        .map(|len| u32::from_le_bytes(len.try_into().unwrap()) as usize)
        .and_then(|len| bytes.get(12..12 + len))
        .ok_or_else(|| invalid(&"truncated"))?;
    let branch: Branch = ciborium::de::from_reader(body).map_err(|e| invalid(&e))?;
    Ok(branch
        .leaves
        .into_iter()
        .map(|leaf| leaf.leaf_cid)
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::branch::{build_branch_bytes, LeafEntry};
    use crate::format::index_root::NamedGraphRouting;
    use crate::format::run_record::{RunSortOrder, LIST_INDEX_NONE};
    use crate::format::run_record_v2::RunRecordV2;
    use crate::format::wire_helpers::{DictPackRefs, DictRefs, DictTreeRefs};
    use fluree_db_core::storage::MemoryContentStore;
    use fluree_db_core::subject_id::SubjectId;
    use fluree_db_core::ContentKind;
    use std::collections::{BTreeMap, HashMap};
    use std::sync::Mutex;

    /// A store that records how many times each CID was fetched.
    #[derive(Debug, Default)]
    struct GetCountingStore {
        inner: MemoryContentStore,
        gets: Mutex<HashMap<ContentId, usize>>,
    }

    impl GetCountingStore {
        fn new() -> Self {
            Self::default()
        }

        fn get_count(&self, id: &ContentId) -> usize {
            self.gets.lock().unwrap().get(id).copied().unwrap_or(0)
        }
    }

    #[async_trait::async_trait]
    impl ContentStore for GetCountingStore {
        fn permits_plaintext_cache(&self) -> bool {
            self.inner.permits_plaintext_cache()
        }

        async fn has(&self, id: &ContentId) -> Result<bool> {
            self.inner.has(id).await
        }

        async fn get(&self, id: &ContentId) -> Result<Vec<u8>> {
            *self.gets.lock().unwrap().entry(id.clone()).or_insert(0) += 1;
            self.inner.get(id).await
        }

        async fn put(&self, kind: ContentKind, bytes: &[u8]) -> Result<ContentId> {
            self.inner.put(kind, bytes).await
        }

        async fn put_with_id(&self, id: &ContentId, bytes: &[u8]) -> Result<()> {
            self.inner.put_with_id(id, bytes).await
        }

        async fn release(&self, id: &ContentId) -> Result<()> {
            self.inner.release(id).await
        }
    }

    fn cid(kind: ContentKind, seed: &[u8]) -> ContentId {
        ContentId::new(kind, seed)
    }

    fn minimal_root() -> IndexRoot {
        let dummy_cid = ContentId::new(ContentKind::IndexLeaf, b"dummy");
        let dummy_tree = DictTreeRefs {
            branch: dummy_cid.clone(),
            leaves: Vec::new(),
        };
        IndexRoot {
            ledger_id: "test:main".to_string(),
            index_t: 1,
            base_t: 0,
            subject_id_encoding: fluree_db_core::SubjectIdEncoding::Narrow,
            namespace_codes: BTreeMap::new(),
            predicate_sids: Vec::new(),
            graph_iris: Vec::new(),
            datatype_iris: Vec::new(),
            language_tags: Vec::new(),
            dict_refs: DictRefs {
                forward_packs: DictPackRefs {
                    string_fwd_packs: Vec::new(),
                    subject_fwd_ns_packs: Vec::new(),
                },
                subject_reverse: dummy_tree.clone(),
                string_reverse: dummy_tree,
            },
            subject_watermarks: Vec::new(),
            string_watermark: 0,
            lex_sorted_string_ids: false,
            total_commit_size: 0,
            total_asserts: 0,
            total_retracts: 0,
            graph_arenas: Vec::new(),
            default_graph_orders: Vec::new(),
            named_graphs: Vec::new(),
            stats: None,
            schema: None,
            prev_index: None,
            garbage: None,
            sketch_ref: None,
            has_annotations: false,
            legacy_annotation_arena: None,
            term_dict: None,
            has_list_meta: None,
            o_type_table: IndexRoot::build_o_type_table(&[], &[]),
            ns_split_mode: fluree_db_core::ns_encoding::NsSplitMode::default(),
        }
    }

    /// A legacy arena branch as its encoder wrote it: a 12-byte header, then
    /// CBOR whose entries carry fields beyond `leaf_cid`.
    fn legacy_branch_bytes(magic: &[u8; 4], leaf_cid: &ContentId) -> Vec<u8> {
        #[derive(serde::Serialize)]
        struct Entry<'a> {
            first_ann: &'a str,
            row_count: u64,
            leaf_cid: &'a ContentId,
        }
        #[derive(serde::Serialize)]
        struct Branch<'a> {
            leaves: Vec<Entry<'a>>,
        }
        let mut body = Vec::new();
        ciborium::ser::into_writer(
            &Branch {
                leaves: vec![Entry {
                    first_ann: "a",
                    row_count: 1,
                    leaf_cid,
                }],
            },
            &mut body,
        )
        .unwrap();
        let mut bytes = magic.to_vec();
        bytes.extend_from_slice(&[1, 0, 0, 0]);
        bytes.extend_from_slice(&(body.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&body);
        bytes
    }

    /// A root that still names a legacy arena, with both branches and their
    /// leaves in `store`. Returns the root and every arena blob.
    async fn root_with_legacy_arena(store: &dyn ContentStore) -> (IndexRoot, Vec<ContentId>) {
        let fwd_leaf = store
            .put(ContentKind::AnnotationForwardLeaf, b"fwd-leaf")
            .await
            .unwrap();
        let rev_leaf = store
            .put(ContentKind::AnnotationReverseLeaf, b"rev-leaf")
            .await
            .unwrap();
        let fwd_branch = store
            .put(
                ContentKind::AnnotationForwardBranch,
                &legacy_branch_bytes(b"EAFB", &fwd_leaf),
            )
            .await
            .unwrap();
        let rev_branch = store
            .put(
                ContentKind::AnnotationReverseBranch,
                &legacy_branch_bytes(b"EARB", &rev_leaf),
            )
            .await
            .unwrap();
        let mut root = minimal_root();
        root.has_annotations = true;
        root.legacy_annotation_arena = Some(LegacyAnnotationArena {
            forward_branch_cid: fwd_branch.clone(),
            reverse_branch_cid: rev_branch.clone(),
        });
        (root, vec![fwd_leaf, rev_leaf, fwd_branch, rev_branch])
    }

    #[tokio::test]
    async fn expands_legacy_arena_branches_to_leaves() {
        let store = MemoryContentStore::new();
        let (root, arena_blobs) = root_with_legacy_arena(&store).await;

        let strict = collect_root_cas_ids_expanded(&store, &root).await.unwrap();
        let tolerant = collect_root_cas_ids_expanded_tolerant(&store, &root).await;
        let mut released =
            legacy_annotation_arena_cids(&store, root.legacy_annotation_arena.as_ref().unwrap())
                .await
                .unwrap();
        released.sort();
        let mut expected = arena_blobs.clone();
        expected.sort();
        assert_eq!(released, expected, "a build releases every arena blob");
        for blob in &arena_blobs {
            assert!(strict.contains(blob), "strict expansion missed {blob}");
            assert!(tolerant.contains(blob), "tolerant expansion missed {blob}");
        }
    }

    /// A build over a root with a legacy arena writes a root without it, so
    /// the garbage diff between the two releases the whole arena.
    #[tokio::test]
    async fn the_garbage_diff_releases_a_legacy_arena() {
        let store = MemoryContentStore::new();
        let (prev_root, arena_blobs) = root_with_legacy_arena(&store).await;
        let mut new_root = prev_root.clone();
        new_root.legacy_annotation_arena = None;

        let prev_ids = collect_root_cas_ids_expanded(&store, &prev_root)
            .await
            .unwrap();
        let new_ids = collect_root_cas_ids_expanded(&store, &new_root)
            .await
            .unwrap();
        let replaced: HashSet<_> = prev_ids.difference(&new_ids).cloned().collect();
        assert_eq!(replaced, arena_blobs.into_iter().collect::<HashSet<_>>());
    }

    #[tokio::test]
    async fn strict_errors_on_missing_legacy_arena_branch() {
        let store = MemoryContentStore::new();
        let mut root = minimal_root();
        root.legacy_annotation_arena = Some(LegacyAnnotationArena {
            forward_branch_cid: cid(ContentKind::AnnotationForwardBranch, b"missing-fwd"),
            reverse_branch_cid: cid(ContentKind::AnnotationReverseBranch, b"missing-rev"),
        });

        // Strict: must surface the read failure rather than return a
        // partial set that pack / GC-diff would treat as authoritative.
        let err = collect_root_cas_ids_expanded(&store, &root)
            .await
            .expect_err("strict mode should error on missing branch");
        assert!(
            err.to_string().contains("legacy annotation arena branch"),
            "error should identify the missing branch: {err}"
        );
    }

    #[tokio::test]
    async fn tolerant_expansion_swallows_missing_legacy_arena_branch() {
        let store = MemoryContentStore::new();
        let mut root = minimal_root();
        let arena = LegacyAnnotationArena {
            forward_branch_cid: cid(ContentKind::AnnotationForwardBranch, b"missing-fwd"),
            reverse_branch_cid: cid(ContentKind::AnnotationReverseBranch, b"missing-rev"),
        };
        root.legacy_annotation_arena = Some(arena.clone());

        let ids = collect_root_cas_ids_expanded_tolerant(&store, &root).await;
        // Still contains the direct branch CIDs from all_cas_ids().
        assert!(arena.branches().all(|branch| ids.contains(branch)));
    }

    /// Roots that carry a manifest over unchanged must not make the chain
    /// re-read it. This is what keeps a sweep's planning cost proportional to
    /// the distinct manifests rather than to the length of the chain.
    #[tokio::test]
    async fn a_chain_reads_a_carried_over_manifest_once() {
        let store = GetCountingStore::new();
        let leaf_cid = store.put(ContentKind::IndexLeaf, b"leaf").await.unwrap();
        let key = RunRecordV2 {
            s_id: SubjectId(1),
            o_key: 0,
            p_id: 1,
            t: 1,
            o_i: LIST_INDEX_NONE,
            o_type: 0,
            g_id: 2,
        };
        let branch = build_branch_bytes(
            RunSortOrder::Spot,
            2,
            &[LeafEntry {
                first_key: key,
                last_key: key,
                row_count: 1,
                leaf_cid: leaf_cid.clone(),
                sidecar_cid: None,
            }],
        );
        let branch_cid = store.put(ContentKind::IndexBranch, &branch).await.unwrap();
        let mut root = minimal_root();
        root.named_graphs = vec![NamedGraphRouting {
            g_id: 2,
            orders: vec![(RunSortOrder::Spot, branch_cid.clone())],
        }];

        // What an incremental build that touched nothing in the graph
        // publishes: a new root pointing at the previous branch.
        let mut later_root = root.clone();
        later_root.index_t = root.index_t + 1;

        let mut chain_ids = ChainCasIds::new();
        chain_ids.add_root(&store, &root).await.unwrap();
        chain_ids.add_root(&store, &later_root).await.unwrap();
        let ids = chain_ids.into_ids();

        assert_eq!(
            store.get_count(&branch_cid),
            1,
            "branch re-read for a root that carried it over"
        );
        // The leaf behind the skipped read is still live: it entered the set
        // when the first root expanded that manifest.
        assert!(
            ids.contains(&leaf_cid),
            "leaf missing after the second root skipped its manifest"
        );
    }
}
