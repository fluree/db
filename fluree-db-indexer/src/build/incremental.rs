//! Incremental index pipeline (FIR6).
//!
//! Resolves only new commits since the last indexed `t`, merges novelty into
//! affected FLI3 leaves via the V3 incremental modules, updates dictionaries,
//! and publishes a new FIR6 root.
//!
//! ## Phases
//!
//! 1. **Resolve**: Load FIR6 root, walk new commits, produce `RunRecordV2` + ops
//! 2. **Branch updates**: For each (graph, order) — including named graphs —
//!    sort novelty by order, fetch existing branch, call `update_branch`,
//!    upload new blobs
//! 3. **Arena updates**: NumBig, Vector, Spatial, Fulltext — patch affected
//!    (g_id, p_id) arenas, carry forward unchanged ones by CID.
//!    Stats / HLL refresh. Schema refresh (rdfs:subClassOf / rdfs:subPropertyOf).
//! 4. **Dict updates**: Update reverse trees + forward packs for new subjects/strings
//! 5. **Root assembly**: `IncrementalRootBuilder` → encode → CAS write → publish

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use fluree_db_binary_index::format::branch::LeafEntry;
use fluree_db_binary_index::format::run_record::RunSortOrder;
use fluree_db_binary_index::format::run_record_v2::{cmp_v2_for_order, RunRecordV2};
use fluree_db_binary_index::{BinaryGarbageRef, BinaryPrevIndexRef};
use fluree_db_core::{ContentId, ContentKind, ContentStore};
use futures::stream::{self, StreamExt};

use crate::error::{IndexerError, Result};
use crate::gc;
use crate::run_index::build::incremental_branch::{
    update_branch, BranchUpdateConfig, BranchUpdateResult,
};
use crate::run_index::build::incremental_resolve::{
    resolve_incremental_commits_v6, IncrementalResolveConfig,
};
use crate::run_index::build::incremental_root::IncrementalRootBuilder;
use crate::{IndexResult, IndexStats, IndexerConfig};

fn artifact_cache_dir(config: &IndexerConfig) -> std::path::PathBuf {
    config
        .data_dir
        .as_ref()
        .map(|d| d.join("binary_artifact_cache"))
        .unwrap_or_else(|| std::env::temp_dir().join("fluree_binary_cache"))
}

/// Run `update_branch` on a blocking thread.
///
/// Uses `spawn_blocking` instead of `block_in_place` so this works on both
/// multi-threaded and current-thread tokio runtimes (the latter is used by
/// `#[tokio::test]`).
async fn run_update_branch(
    branch_bytes: Vec<u8>,
    sorted_records: Vec<RunRecordV2>,
    sorted_ops: Vec<u8>,
    branch_config: BranchUpdateConfig,
    content_store: Arc<dyn fluree_db_core::storage::ContentStore>,
    cache_dir: std::path::PathBuf,
) -> std::result::Result<BranchUpdateResult, IndexerError> {
    let handle = tokio::runtime::Handle::current();
    let parent_span = tracing::Span::current();
    tokio::task::spawn_blocking(move || {
        let _guard = parent_span.enter();
        let cs = content_store.clone();
        let cs2 = content_store;
        let cache_dir2 = cache_dir.clone();
        update_branch(
            &branch_bytes,
            &sorted_records,
            &sorted_ops,
            &branch_config,
            &|cid| {
                handle.block_on(async {
                    fluree_db_binary_index::read::artifact_cache::fetch_cached_bytes_cid(
                        cs.as_ref(),
                        cid,
                        &cache_dir,
                    )
                    .await
                })
            },
            &|cid| {
                handle
                    .block_on(async {
                        fluree_db_binary_index::read::artifact_cache::fetch_cached_bytes_cid(
                            cs2.as_ref(),
                            cid,
                            &cache_dir2,
                        )
                        .await
                    })
                    .map(Some)
                    .or_else(|e| match e.kind() {
                        std::io::ErrorKind::NotFound => {
                            tracing::debug!("sidecar not found (treating as absent): {e}");
                            Ok(None)
                        }
                        _ => Err(e),
                    })
            },
        )
    })
    .await
    .map_err(|e| IndexerError::StorageWrite(format!("branch update task panicked: {e}")))?
    .map_err(|e| IndexerError::StorageWrite(e.to_string()))
}

enum Phase2TaskKind {
    DefaultExisting { leaves: Vec<LeafEntry> },
    DefaultFresh,
    NamedExisting { branch_cid: ContentId },
    NamedFresh,
}

struct Phase2Task {
    seq: usize,
    g_id: u16,
    order: RunSortOrder,
    sorted_records: Vec<RunRecordV2>,
    sorted_ops: Vec<u8>,
    kind: Phase2TaskKind,
}

enum Phase2TaskUpdate {
    Default { leaf_entries: Vec<LeafEntry> },
    Named { branch_cid: ContentId },
}

struct Phase2TaskOutput {
    seq: usize,
    g_id: u16,
    order: RunSortOrder,
    update: Phase2TaskUpdate,
    replaced_leaf_cids: Vec<ContentId>,
    replaced_sidecar_cids: Vec<ContentId>,
    new_leaf_count: usize,
}

async fn execute_phase2_task(
    task: Phase2Task,
    config: &IndexerConfig,
    content_store: Arc<dyn fluree_db_core::storage::ContentStore>,
    cache_dir: std::path::PathBuf,
) -> Result<Phase2TaskOutput> {
    let Phase2Task {
        seq,
        g_id,
        order,
        sorted_records,
        sorted_ops,
        kind,
    } = task;

    let branch_config = BranchUpdateConfig {
        order,
        g_id,
        zstd_level: 1,
        leaflet_target_rows: config.leaflet_rows.max(1),
        leaf_target_rows: config
            .leaflet_rows
            .max(1)
            .saturating_mul(config.leaflets_per_leaf.max(1)),
    };

    let started = Instant::now();
    let output = match kind {
        Phase2TaskKind::DefaultExisting { leaves } => {
            let branch_bytes =
                fluree_db_binary_index::format::branch::build_branch_bytes(order, g_id, &leaves);
            let result = run_update_branch(
                branch_bytes,
                sorted_records,
                sorted_ops,
                branch_config,
                Arc::clone(&content_store),
                cache_dir,
            )
            .await?;
            upload_leaf_blobs(content_store.as_ref(), &result).await?;
            Phase2TaskOutput {
                seq,
                g_id,
                order,
                update: Phase2TaskUpdate::Default {
                    leaf_entries: result.leaf_entries,
                },
                replaced_leaf_cids: result.replaced_leaf_cids,
                replaced_sidecar_cids: result.replaced_sidecar_cids,
                new_leaf_count: result.new_leaf_blobs.len(),
            }
        }
        Phase2TaskKind::DefaultFresh => {
            let result =
                build_fresh_default_graph_v3(&sorted_records, &sorted_ops, order, g_id, config)?;
            upload_leaf_blobs(content_store.as_ref(), &result).await?;
            Phase2TaskOutput {
                seq,
                g_id,
                order,
                update: Phase2TaskUpdate::Default {
                    leaf_entries: result.leaf_entries,
                },
                replaced_leaf_cids: Vec::new(),
                replaced_sidecar_cids: Vec::new(),
                new_leaf_count: result.new_leaf_blobs.len(),
            }
        }
        Phase2TaskKind::NamedExisting { branch_cid } => {
            let branch_bytes =
                fluree_db_binary_index::read::artifact_cache::fetch_cached_bytes_cid(
                    content_store.as_ref(),
                    &branch_cid,
                    &cache_dir,
                )
                .await
                .map_err(|e| {
                    IndexerError::StorageRead(format!("fetch V3 branch g_id={g_id} {order:?}: {e}"))
                })?;
            let result = run_update_branch(
                branch_bytes,
                sorted_records,
                sorted_ops,
                branch_config,
                Arc::clone(&content_store),
                cache_dir,
            )
            .await?;
            upload_leaf_blobs(content_store.as_ref(), &result).await?;
            content_store
                .put(ContentKind::IndexBranch, &result.branch_bytes)
                .await
                .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
            Phase2TaskOutput {
                seq,
                g_id,
                order,
                update: Phase2TaskUpdate::Named {
                    branch_cid: result.branch_cid,
                },
                replaced_leaf_cids: result.replaced_leaf_cids,
                replaced_sidecar_cids: result.replaced_sidecar_cids,
                new_leaf_count: result.new_leaf_blobs.len(),
            }
        }
        Phase2TaskKind::NamedFresh => {
            let result = build_fresh_named_graph_v3(&sorted_records, order, g_id, config)?;
            upload_leaf_blobs(content_store.as_ref(), &result).await?;
            content_store
                .put(ContentKind::IndexBranch, &result.branch_bytes)
                .await
                .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
            Phase2TaskOutput {
                seq,
                g_id,
                order,
                update: Phase2TaskUpdate::Named {
                    branch_cid: result.branch_cid,
                },
                replaced_leaf_cids: Vec::new(),
                replaced_sidecar_cids: Vec::new(),
                new_leaf_count: result.new_leaf_blobs.len(),
            }
        }
    };

    tracing::debug!(
        seq,
        g_id,
        ?order,
        new_leaf_count = output.new_leaf_count,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "V6 Phase 2 task complete"
    );

    Ok(output)
}

/// Entry point for incremental indexing.
///
/// Called from `build_index_for_ledger` when an index exists and
/// incremental conditions are met.
pub async fn incremental_index(
    content_store: Arc<dyn ContentStore>,
    ledger_id: &str,
    record: &fluree_db_nameservice::NsRecord,
    config: IndexerConfig,
) -> Result<IndexResult> {
    let base_root_id = record.index_head_id.clone().ok_or(IndexerError::NoIndex)?;
    let head_commit_id = record
        .commit_head_id
        .clone()
        .ok_or(IndexerError::NoCommits)?;
    let from_t = record.index_t;
    tracing::debug!(
        ledger_id = ledger_id,
        from_t,
        to_t = record.commit_t,
        base_root = %base_root_id,
        head_commit = %head_commit_id,
        "starting incremental index build"
    );

    let cache_dir = artifact_cache_dir(&config);
    let _ = std::fs::create_dir_all(&cache_dir);

    // ---- Phase 1: Resolve incremental commits ----
    let resolve_config = IncrementalResolveConfig {
        base_root_id: base_root_id.clone(),
        head_commit_id,
        from_t,
        artifact_cache_dir: Some(cache_dir.clone()),
        max_commit_bytes: config.incremental_max_commit_bytes,
        fulltext_configured_properties: config.fulltext_configured_properties.clone(),
    };
    let mut novelty = resolve_incremental_commits_v6(content_store.clone(), resolve_config)
        .await
        .map_err(|e| IndexerError::StorageWrite(format!("V6 incremental resolve: {e}")))?;

    if novelty.records.is_empty() {
        tracing::debug!("no new records resolved; returning existing V6 root");
        return Ok(IndexResult {
            root_id: base_root_id,
            index_t: novelty.max_t,
            ledger_id: ledger_id.to_string(),
            stats: IndexStats::default(),
        });
    }

    tracing::debug!(
        records = novelty.records.len(),
        new_subjects = novelty.new_subjects.len(),
        new_strings = novelty.new_strings.len(),
        max_t = novelty.max_t,
        "V6 Phase 1 complete: incremental resolve"
    );

    // ---- Phase 2: Per-(graph, order) branch updates ----
    let all_orders = [
        RunSortOrder::Spot,
        RunSortOrder::Psot,
        RunSortOrder::Post,
        RunSortOrder::Opst,
    ];

    // Group records + ops by g_id.
    let mut by_graph: std::collections::BTreeMap<u16, Vec<usize>> =
        std::collections::BTreeMap::new();
    for (idx, rec) in novelty.records.iter().enumerate() {
        by_graph.entry(rec.g_id).or_default().push(idx);
    }

    let base_root = &novelty.base_root;
    let mut root_builder = IncrementalRootBuilder::from_old_root(novelty.base_root.clone());
    root_builder.set_index_t(novelty.max_t);
    root_builder.add_commit_stats(
        novelty.delta_commit_size,
        novelty.delta_asserts,
        novelty.delta_retracts,
    );

    let concurrency = config.incremental_max_concurrency.max(1);
    let mut graph_order: Vec<u16> = by_graph.keys().copied().collect();
    graph_order.sort_unstable_by_key(|&g_id| (u8::from(g_id == 0), g_id));

    let mut phase2_tasks = Vec::with_capacity(by_graph.len().saturating_mul(all_orders.len()));
    for g_id in graph_order.iter().copied() {
        let indices = &by_graph[&g_id];
        let graph_records: Vec<RunRecordV2> = indices.iter().map(|&i| novelty.records[i]).collect();
        let graph_ops: Vec<u8> = indices.iter().map(|&i| novelty.ops[i]).collect();

        for &order in &all_orders {
            let cmp = cmp_v2_for_order(order);
            let mut sorted_indices: Vec<usize> = (0..graph_records.len()).collect();
            sorted_indices.sort_unstable_by(|&a, &b| cmp(&graph_records[a], &graph_records[b]));
            let sorted_records: Vec<RunRecordV2> =
                sorted_indices.iter().map(|&i| graph_records[i]).collect();
            let sorted_ops: Vec<u8> = sorted_indices.iter().map(|&i| graph_ops[i]).collect();

            let kind = if g_id == 0 {
                match base_root
                    .default_graph_orders
                    .iter()
                    .find(|o| o.order == order)
                    .map(|o| o.leaves.clone())
                {
                    Some(leaves) => Phase2TaskKind::DefaultExisting { leaves },
                    None => Phase2TaskKind::DefaultFresh,
                }
            } else {
                match base_root
                    .named_graphs
                    .iter()
                    .find(|ng| ng.g_id == g_id)
                    .and_then(|ng| ng.orders.iter().find(|(o, _)| *o == order))
                    .map(|(_, cid)| cid.clone())
                {
                    Some(branch_cid) => Phase2TaskKind::NamedExisting { branch_cid },
                    None => Phase2TaskKind::NamedFresh,
                }
            };

            phase2_tasks.push(Phase2Task {
                seq: phase2_tasks.len(),
                g_id,
                order,
                sorted_records,
                sorted_ops,
                kind,
            });
        }
    }

    tracing::debug!(
        tasks = phase2_tasks.len(),
        concurrency,
        graphs = by_graph.len(),
        graph_order = ?graph_order,
        "V6 Phase 2: scheduling branch updates"
    );

    let config_ref = &config;
    let phase2_results = stream::iter(phase2_tasks)
        .map(|task| {
            let content_store = content_store.clone();
            let cache_dir = cache_dir.clone();
            async move { execute_phase2_task(task, config_ref, content_store, cache_dir).await }
        })
        .buffer_unordered(concurrency)
        .collect::<Vec<_>>()
        .await;

    let mut total_new_leaves = 0usize;
    let mut phase2_outputs: Vec<Phase2TaskOutput> =
        phase2_results.into_iter().collect::<Result<Vec<_>>>()?;
    phase2_outputs.sort_unstable_by_key(|output| output.seq);

    for output in phase2_outputs {
        total_new_leaves += output.new_leaf_count;
        match output.update {
            Phase2TaskUpdate::Default { leaf_entries } => {
                root_builder.set_default_graph_order(output.order, leaf_entries);
            }
            Phase2TaskUpdate::Named { branch_cid } => {
                root_builder.set_named_graph_branch(output.g_id, output.order, branch_cid);
            }
        }
        root_builder.add_replaced_cids(output.replaced_leaf_cids);
        root_builder.add_replaced_cids(output.replaced_sidecar_cids);
    }

    tracing::debug!(
        new_leaves = total_new_leaves,
        graphs = by_graph.len(),
        concurrency,
        "V6 Phase 2 complete: branch updates"
    );

    // ---- Phase 3: Dict updates ----
    // Incremental dict tree updates.
    use super::dicts::{
        upload_incremental_reverse_tree_async, upload_incremental_reverse_tree_async_strings,
    };
    let mut new_dict_refs = base_root.dict_refs.clone();

    if !novelty.new_subjects.is_empty() {
        tracing::debug!(
            count = novelty.new_subjects.len(),
            "V6 Phase 3: updating subject reverse tree"
        );
        let updated = upload_incremental_reverse_tree_async(
            content_store.as_ref(),
            fluree_db_core::DictKind::SubjectReverse,
            &base_root.dict_refs.subject_reverse,
            novelty.new_subjects.clone(),
        )
        .await?;
        root_builder.add_replaced_cids(updated.replaced_cids);
        new_dict_refs.subject_reverse = updated.tree_refs;
    }

    if !novelty.new_strings.is_empty() {
        tracing::debug!(
            count = novelty.new_strings.len(),
            "V6 Phase 3: updating string reverse tree"
        );
        let updated = upload_incremental_reverse_tree_async_strings(
            content_store.as_ref(),
            fluree_db_core::DictKind::StringReverse,
            &base_root.dict_refs.string_reverse,
            novelty.new_strings.clone(),
        )
        .await?;
        root_builder.add_replaced_cids(updated.replaced_cids);
        new_dict_refs.string_reverse = updated.tree_refs;
    }

    // Forward pack updates (FPK1): append new pack artifacts for new subjects/strings.
    // Forward packs are append-only: existing pack refs are preserved, new entries get
    // their own pack artifacts appended to the routing list.
    {
        use fluree_db_binary_index::dict::incremental::{
            build_incremental_string_packs, build_incremental_subject_packs_for_ns,
        };
        use fluree_db_binary_index::PackBranchEntry;

        // String forward packs.
        // Invariant: new_strings is sorted by string_id ascending (enforced by resolver).
        if !novelty.new_strings.is_empty() {
            debug_assert!(
                novelty.new_strings.windows(2).all(|w| w[0].0 < w[1].0),
                "new_strings must be sorted by string_id ascending"
            );

            let existing_refs = &base_root.dict_refs.forward_packs.string_fwd_packs;
            let entries: Vec<(u32, &[u8])> = novelty
                .new_strings
                .iter()
                .map(|(id, val)| (*id, val.as_slice()))
                .collect();

            let pack_result = build_incremental_string_packs(existing_refs, &entries)
                .map_err(|e| IndexerError::StorageWrite(format!("string fwd pack build: {e}")))?;

            // Upload new pack artifacts and build updated refs.
            let mut updated_refs = existing_refs.clone();
            let kind = ContentKind::DictBlob {
                dict: fluree_db_core::DictKind::StringForward,
            };
            for pack in &pack_result.new_packs {
                let pack_cid = content_store
                    .put(kind, &pack.bytes)
                    .await
                    .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
                updated_refs.push(PackBranchEntry {
                    first_id: pack.first_id,
                    last_id: pack.last_id,
                    pack_cid,
                });
            }
            new_dict_refs.forward_packs.string_fwd_packs = updated_refs;

            tracing::debug!(
                new_packs = pack_result.new_packs.len(),
                new_strings = novelty.new_strings.len(),
                "V6 Phase 3: string forward packs updated"
            );
        }

        // Subject forward packs (per namespace).
        // Invariant: new_subjects is sorted by (ns_code, local_id) ascending (enforced by resolver).
        if !novelty.new_subjects.is_empty() {
            debug_assert!(
                novelty
                    .new_subjects
                    .windows(2)
                    .all(|w| (w[0].0, w[0].1) <= (w[1].0, w[1].1)),
                "new_subjects must be sorted by (ns_code, local_id) ascending"
            );

            // Group by namespace. BTreeMap preserves ns_code order;
            // within each namespace, local_ids are already sorted.
            let mut by_ns: std::collections::BTreeMap<u16, Vec<(u64, Vec<u8>)>> =
                std::collections::BTreeMap::new();
            for (ns_code, local_id, suffix) in &novelty.new_subjects {
                by_ns
                    .entry(*ns_code)
                    .or_default()
                    .push((*local_id, suffix.clone()));
            }

            let kind = ContentKind::DictBlob {
                dict: fluree_db_core::DictKind::SubjectForward,
            };

            for (ns_code, entries) in &by_ns {
                // Find existing refs for this namespace.
                let existing_ns_refs: Vec<PackBranchEntry> = new_dict_refs
                    .forward_packs
                    .subject_fwd_ns_packs
                    .iter()
                    .find(|(ns, _)| ns == ns_code)
                    .map(|(_, refs)| refs.clone())
                    .unwrap_or_default();

                let entry_refs: Vec<(u64, &[u8])> = entries
                    .iter()
                    .map(|(id, val)| (*id, val.as_slice()))
                    .collect();

                let pack_result = build_incremental_subject_packs_for_ns(
                    *ns_code,
                    &existing_ns_refs,
                    &entry_refs,
                )
                .map_err(|e| {
                    IndexerError::StorageWrite(format!("subject fwd pack build ns={ns_code}: {e}"))
                })?;

                // Upload new packs and build updated refs.
                let mut updated_refs = existing_ns_refs;
                for pack in &pack_result.new_packs {
                    let pack_cid = content_store
                        .put(kind, &pack.bytes)
                        .await
                        .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
                    updated_refs.push(PackBranchEntry {
                        first_id: pack.first_id,
                        last_id: pack.last_id,
                        pack_cid,
                    });
                }

                // Update or insert namespace entry.
                if let Some(entry) = new_dict_refs
                    .forward_packs
                    .subject_fwd_ns_packs
                    .iter_mut()
                    .find(|(ns, _)| ns == ns_code)
                {
                    entry.1 = updated_refs;
                } else {
                    new_dict_refs
                        .forward_packs
                        .subject_fwd_ns_packs
                        .push((*ns_code, updated_refs));
                }

                tracing::debug!(
                    ns_code,
                    new_packs = pack_result.new_packs.len(),
                    new_subjects = entries.len(),
                    "V6 Phase 3: subject forward packs updated"
                );
            }
        }
    }

    root_builder.set_dict_refs(new_dict_refs);

    // Update metadata from resolver state.
    let new_ns_codes: std::collections::BTreeMap<u16, String> = novelty
        .shared
        .ns_prefixes
        .iter()
        .map(|(&code, prefix)| (code, prefix.clone()))
        .collect();
    root_builder.set_namespace_codes(new_ns_codes.clone());

    let new_pred_sids = build_predicate_sids(&novelty.shared, &new_ns_codes);
    root_builder.set_predicate_sids(new_pred_sids);

    let new_graph_iris: Vec<String> = (0..novelty.shared.graphs.len())
        .filter_map(|id| {
            novelty
                .shared
                .graphs
                .resolve(id)
                .map(std::string::ToString::to_string)
        })
        .collect();
    root_builder.set_graph_iris(new_graph_iris);

    let new_datatype_iris: Vec<String> = (0..novelty.shared.datatypes.len())
        .filter_map(|id| {
            novelty
                .shared
                .datatypes
                .resolve(id)
                .map(std::string::ToString::to_string)
        })
        .collect();
    root_builder.set_datatype_iris(new_datatype_iris);

    // NOTE: `set_language_tags` is deferred to AFTER Phase 3a so that any
    // synthetic `lang_id`s the fulltext builder must allocate (e.g., the
    // dict-assigned id for `"en"` used as the bucket for `@fulltext`-datatype
    // values) are present in the persisted `language_tags` list. Otherwise
    // the root can end up referencing `lang_id`s that never made it into the
    // dict blob, breaking query-time arena lookup.

    root_builder.set_watermarks(
        novelty.updated_watermarks.clone(),
        novelty.updated_string_watermark,
    );

    // ---- Phase 3a: Arena updates (numbig + vectors + spatial + fulltext) ----
    //
    // Build updated graph_arenas by starting from the base root's arenas
    // and patching any (g_id, p_id) that have new/extended data.
    // The resolver already populated shared.numbigs/vectors/spatial_hook/fulltext_hook.
    {
        use fluree_db_binary_index::{
            FulltextArenaRef, GraphArenaRefs, SpatialArenaRef, VectorDictRef,
        };
        use std::collections::BTreeMap;

        let mut arenas_by_gid: BTreeMap<u16, GraphArenaRefs> = BTreeMap::new();
        for ga in &base_root.graph_arenas {
            arenas_by_gid.insert(ga.g_id, ga.clone());
        }

        let has_new_numbigs = !novelty.shared.numbigs.is_empty();
        let has_new_vectors = !novelty.shared.vectors.is_empty();
        let has_new_spatial = novelty
            .shared
            .spatial_hook
            .as_ref()
            .is_some_and(|h| !h.is_empty());
        let has_new_fulltext = novelty
            .shared
            .fulltext_hook
            .as_ref()
            .is_some_and(|h| !h.is_empty());

        if has_new_numbigs || has_new_vectors || has_new_spatial || has_new_fulltext {
            // ---- NumBig arena upload ----
            for (&g_id, per_pred) in &novelty.shared.numbigs {
                let ga = arenas_by_gid.entry(g_id).or_insert_with(|| GraphArenaRefs {
                    g_id,
                    numbig: Vec::new(),
                    vectors: Vec::new(),
                    spatial: Vec::new(),
                    fulltext: vec![],
                });

                for (&p_id, arena) in per_pred {
                    if arena.is_empty() {
                        continue;
                    }
                    let base_nb_count = novelty
                        .base_numbig_counts
                        .get(&(g_id, p_id))
                        .copied()
                        .unwrap_or(0);
                    if arena.len() == base_nb_count {
                        continue;
                    }

                    let bytes =
                        fluree_db_binary_index::arena::numbig::write_numbig_arena_to_bytes(arena)
                            .map_err(|e| {
                            IndexerError::StorageWrite(format!("numbig arena serialize: {e}"))
                        })?;
                    let dict_kind = fluree_db_core::DictKind::NumBig { p_id };
                    let cid = super::upload::upload_dict_blob(
                        content_store.as_ref(),
                        dict_kind,
                        &bytes,
                        "incremental V6 numbig arena uploaded",
                    )
                    .await?;

                    if let Some(pos) = ga.numbig.iter().position(|(pid, _)| *pid == p_id) {
                        let old_cid = ga.numbig[pos].1.clone();
                        root_builder.add_replaced_cids(vec![old_cid]);
                        ga.numbig[pos].1 = cid;
                    } else {
                        ga.numbig.push((p_id, cid));
                        ga.numbig.sort_by_key(|(pid, _)| *pid);
                    }
                }
            }

            // ---- Vector arena upload ----
            for (&g_id, per_pred) in &novelty.shared.vectors {
                let ga = arenas_by_gid.entry(g_id).or_insert_with(|| GraphArenaRefs {
                    g_id,
                    numbig: Vec::new(),
                    vectors: Vec::new(),
                    spatial: Vec::new(),
                    fulltext: vec![],
                });

                for (&p_id, arena) in per_pred {
                    if arena.is_empty() {
                        continue;
                    }

                    let base_count = novelty
                        .base_vector_counts
                        .get(&(g_id, p_id))
                        .copied()
                        .unwrap_or(0);
                    // `arena` here is the unified base+chunk arena (see
                    // `incremental_resolve` step 4b). `arena.len()` is the
                    // total count after this round; it inherently fits in
                    // u32 because the arena's append API caps at u32::MAX.
                    if arena.len() < base_count {
                        return Err(IndexerError::IncrementalAbort(format!(
                            "vector arena shrunk for g_id={g_id}, p_id={p_id}: \
                             base_count={base_count}, arena.len()={}",
                            arena.len()
                        )));
                    }

                    if let Some(pos) = ga.vectors.iter().position(|v| v.p_id == p_id) {
                        // Extending an existing vector arena.
                        let existing = &ga.vectors[pos];

                        let old_manifest_bytes =
                            content_store.get(&existing.manifest).await.map_err(|e| {
                                IndexerError::StorageRead(format!(
                                    "read existing vector manifest: {e}"
                                ))
                            })?;
                        let old_manifest =
                            fluree_db_binary_index::arena::vector::read_vector_manifest(
                                &old_manifest_bytes,
                            )
                            .map_err(|e| {
                                IndexerError::StorageRead(format!(
                                    "decode existing vector manifest: {e}"
                                ))
                            })?;

                        if old_manifest.dims != arena.dims() {
                            return Err(IndexerError::IncrementalAbort(format!(
                                "vector dims mismatch for g_id={g_id}, p_id={p_id}: \
                                 existing={}, new={}",
                                old_manifest.dims,
                                arena.dims()
                            )));
                        }
                        if old_manifest.shard_capacity
                            != fluree_db_binary_index::arena::vector::SHARD_CAPACITY
                        {
                            return Err(IndexerError::IncrementalAbort(format!(
                                "vector shard_capacity mismatch for g_id={g_id}, p_id={p_id}: \
                                 existing={}, expected={}",
                                old_manifest.shard_capacity,
                                fluree_db_binary_index::arena::vector::SHARD_CAPACITY
                            )));
                        }
                        if old_manifest.normalized != arena.is_normalized() {
                            return Err(IndexerError::IncrementalAbort(format!(
                                "vector normalization mismatch for g_id={g_id}, p_id={p_id}: \
                                 existing={}, new={}",
                                old_manifest.normalized,
                                arena.is_normalized()
                            )));
                        }

                        let shard_cap = old_manifest.shard_capacity;
                        let dims_usize = arena.dims() as usize;
                        // `arena.raw_values()` holds base + chunk-new values
                        // because step 4b pre-loaded the base arena before
                        // the resolver appended chunk inserts. Everything
                        // below operates on the chunk-new portion only;
                        // base entries are already persisted in existing
                        // shards and must not be re-read or re-written.
                        let new_count = arena.len() - base_count;
                        // Retraction-only chunk for this predicate: the base
                        // arena is unchanged. Re-uploading the manifest (a)
                        // re-CASes identical bytes, and (b) queues the still-
                        // live `existing.manifest` for GC release — which
                        // would delete the manifest the current root still
                        // references, since `release` deletes by CID with no
                        // live-reference check. Leave `ga.vectors[pos]`
                        // pointing at the existing manifest and shards.
                        if new_count == 0 {
                            continue;
                        }
                        let new_raw_offset = base_count as usize * dims_usize;
                        let new_raw = &arena.raw_values()[new_raw_offset..];
                        let mut combined_shards = existing.shards.clone();
                        let mut combined_shard_infos = old_manifest.shards.clone();

                        let mut consumed_new: u32 = 0;
                        if let Some(last_info) = combined_shard_infos.last().cloned() {
                            if last_info.count < shard_cap {
                                let remaining = shard_cap - last_info.count;
                                let take = remaining.min(new_count);
                                if take > 0 {
                                    let last_idx = combined_shard_infos.len() - 1;
                                    let old_last_cid = combined_shards[last_idx].clone();
                                    let old_last_bytes =
                                        content_store.get(&old_last_cid).await.map_err(|e| {
                                            IndexerError::StorageRead(format!(
                                                "read vector last shard: {e}"
                                            ))
                                        })?;
                                    let old_last_shard =
                                        fluree_db_binary_index::arena::vector::read_vector_shard_from_bytes(
                                            &old_last_bytes,
                                        )
                                        .map_err(|e| {
                                            IndexerError::StorageRead(format!(
                                                "decode vector last shard: {e}"
                                            ))
                                        })?;

                                    let take_f32 = take as usize * dims_usize;
                                    let mut merged: Vec<f32> =
                                        Vec::with_capacity(old_last_shard.values.len() + take_f32);
                                    merged.extend_from_slice(&old_last_shard.values);
                                    merged.extend_from_slice(&new_raw[0..take_f32]);

                                    let shard_bytes =
                                        fluree_db_binary_index::arena::vector::write_vector_shard_to_bytes(
                                            old_manifest.dims,
                                            &merged,
                                        )
                                        .map_err(|e| {
                                            IndexerError::StorageWrite(format!(
                                                "vector last shard serialize: {e}"
                                            ))
                                        })?;

                                    let dict_kind = fluree_db_core::DictKind::VectorShard { p_id };
                                    let new_last_cid = super::upload::upload_dict_blob(
                                        content_store.as_ref(),
                                        dict_kind,
                                        &shard_bytes,
                                        "incremental V6 vector last shard replaced",
                                    )
                                    .await?;

                                    combined_shard_infos[last_idx].cas = new_last_cid.to_string();
                                    combined_shards[last_idx] = new_last_cid;
                                    combined_shard_infos[last_idx].count = last_info.count + take;
                                    root_builder.add_replaced_cids(vec![old_last_cid]);
                                    consumed_new = take;
                                }
                            }
                        }

                        let start_f32 = consumed_new as usize * dims_usize;
                        let remaining_raw = &new_raw[start_f32..];
                        let shard_results =
                            fluree_db_binary_index::arena::vector::write_vector_shards_from_raw(
                                arena.dims(),
                                remaining_raw,
                            )
                            .map_err(|e| {
                                IndexerError::StorageWrite(format!("vector shard serialize: {e}"))
                            })?;

                        for (shard_bytes, mut shard_info) in shard_results {
                            let dict_kind = fluree_db_core::DictKind::VectorShard { p_id };
                            let shard_cid = super::upload::upload_dict_blob(
                                content_store.as_ref(),
                                dict_kind,
                                &shard_bytes,
                                "incremental V6 vector shard uploaded",
                            )
                            .await?;
                            shard_info.cas = shard_cid.to_string();
                            combined_shards.push(shard_cid);
                            combined_shard_infos.push(shard_info);
                        }

                        let combined_manifest =
                            fluree_db_binary_index::arena::vector::VectorManifest {
                                version: 1,
                                dims: old_manifest.dims,
                                dtype: "f32".to_string(),
                                normalized: old_manifest.normalized,
                                shard_capacity: old_manifest.shard_capacity,
                                // `arena.len()` already includes base + new
                                // (step 4b pre-loaded base into the arena);
                                // adding base_count here would double-count.
                                total_count: arena.len(),
                                shards: combined_shard_infos,
                            };

                        let manifest_json =
                            serde_json::to_vec_pretty(&combined_manifest).map_err(|e| {
                                IndexerError::StorageWrite(format!(
                                    "serialize vector manifest: {e}"
                                ))
                            })?;

                        let dict_kind = fluree_db_core::DictKind::VectorManifest { p_id };
                        let manifest_cid = super::upload::upload_dict_blob(
                            content_store.as_ref(),
                            dict_kind,
                            &manifest_json,
                            "incremental V6 vector manifest uploaded",
                        )
                        .await?;

                        root_builder.add_replaced_cids(vec![existing.manifest.clone()]);
                        ga.vectors[pos] = VectorDictRef {
                            p_id,
                            manifest: manifest_cid,
                            shards: combined_shards,
                        };
                    } else {
                        // Brand new vector arena.
                        let shard_results =
                            fluree_db_binary_index::arena::vector::write_vector_shards_to_bytes(
                                arena,
                            )
                            .map_err(|e| {
                                IndexerError::StorageWrite(format!("vector shard serialize: {e}"))
                            })?;

                        let mut new_shard_cids = Vec::with_capacity(shard_results.len());
                        let mut new_shard_infos = Vec::with_capacity(shard_results.len());

                        for (shard_bytes, mut shard_info) in shard_results {
                            let dict_kind = fluree_db_core::DictKind::VectorShard { p_id };
                            let shard_cid = super::upload::upload_dict_blob(
                                content_store.as_ref(),
                                dict_kind,
                                &shard_bytes,
                                "incremental V6 vector shard uploaded",
                            )
                            .await?;
                            shard_info.cas = shard_cid.to_string();
                            new_shard_cids.push(shard_cid);
                            new_shard_infos.push(shard_info);
                        }

                        let manifest = fluree_db_binary_index::arena::vector::VectorManifest {
                            version: 1,
                            dims: arena.dims(),
                            dtype: "f32".to_string(),
                            normalized: arena.is_normalized(),
                            shard_capacity: fluree_db_binary_index::arena::vector::SHARD_CAPACITY,
                            total_count: arena.len(),
                            shards: new_shard_infos,
                        };

                        let manifest_json = serde_json::to_vec_pretty(&manifest).map_err(|e| {
                            IndexerError::StorageWrite(format!("serialize vector manifest: {e}"))
                        })?;

                        let dict_kind = fluree_db_core::DictKind::VectorManifest { p_id };
                        let manifest_cid = super::upload::upload_dict_blob(
                            content_store.as_ref(),
                            dict_kind,
                            &manifest_json,
                            "incremental V6 vector manifest uploaded",
                        )
                        .await?;

                        ga.vectors.push(VectorDictRef {
                            p_id,
                            manifest: manifest_cid,
                            shards: new_shard_cids,
                        });
                        ga.vectors.sort_by_key(|v| v.p_id);
                    }
                }
            }

            // ---- Fulltext arena incremental update ----
            if has_new_fulltext {
                use fluree_db_binary_index::analyzer::{Analyzer, Language};

                let fulltext_entries = novelty
                    .shared
                    .fulltext_hook
                    .as_ref()
                    .map(super::super::fulltext_hook::FulltextHook::entries)
                    .unwrap_or(&[]);

                // Resolve the dict-assigned lang_id for `"en"` once — used as
                // the bucket for every `DatatypeFulltext` entry and as the
                // fallback for `Configured` entries whose row is untagged.
                //
                // TODO(fulltext-config): once `FullTextDefaults.default_language`
                // is plumbed into the indexing pipeline, prefer that tag over
                // the plain `"en"` fallback for `Configured && lang_id == 0`.
                let en_lang_id = novelty.shared.languages.get_or_insert(Some("en"));

                // Group entries by (g_id, p_id, bucket_lang_id).
                let mut ft_grouped: BTreeMap<
                    (u16, u32, u16),
                    Vec<&crate::fulltext_hook::FulltextEntry>,
                > = BTreeMap::new();
                for entry in fulltext_entries {
                    let bucket_lang_id = match entry.source {
                        crate::fulltext_hook::FulltextSource::DatatypeFulltext => en_lang_id,
                        crate::fulltext_hook::FulltextSource::Configured => {
                            if entry.lang_id != 0 {
                                entry.lang_id
                            } else {
                                en_lang_id
                            }
                        }
                    };
                    ft_grouped
                        .entry((entry.g_id, entry.p_id, bucket_lang_id))
                        .or_default()
                        .push(entry);
                }

                for ((g_id, p_id, lang_id), group_entries) in ft_grouped {
                    // Resolve the BCP-47 tag for this bucket so we can pick an
                    // analyzer. Missing dict entry falls back to English.
                    let lang_tag = novelty
                        .shared
                        .languages
                        .resolve(lang_id)
                        .map(std::string::ToString::to_string)
                        .unwrap_or_else(|| "en".to_string());
                    let language = Language::from_bcp47(&lang_tag);
                    let analyzer = Analyzer::for_language(language);

                    let ga_ref = arenas_by_gid.get(&g_id);
                    let existing_ref = ga_ref.and_then(|a| {
                        a.fulltext
                            .iter()
                            .find(|f| f.p_id == p_id && f.lang_id == lang_id)
                    });

                    // Fetch + decode the prior arena for this bucket. Any
                    // failure here MUST propagate — silently "starting fresh"
                    // would drop every previously-indexed doc in the bucket,
                    // leaving the on-disk arena reflecting only the novelty
                    // window. The incremental pipeline's error-handling
                    // contract covers this: `build_index_for_ledger` falls
                    // back to a full rebuild on incremental failure, which is
                    // the correct recovery for a bad prior-arena blob.
                    let prior_arena = if let Some(ft_ref) = existing_ref {
                        let blob = content_store.get(&ft_ref.arena_cid).await.map_err(|e| {
                            IndexerError::StorageRead(format!(
                                "fulltext incremental: prior arena fetch for (g_id={g_id}, p_id={p_id}, lang_id={lang_id}) cid={}: {e}",
                                ft_ref.arena_cid
                            ))
                        })?;
                        fluree_db_binary_index::arena::fulltext::FulltextArena::decode(&blob)
                            .map_err(|e| {
                                IndexerError::Other(format!(
                                    "fulltext incremental: prior arena decode for (g_id={g_id}, p_id={p_id}, lang_id={lang_id}) cid={}: {e}",
                                    ft_ref.arena_cid
                                ))
                            })?
                    } else {
                        fluree_db_binary_index::arena::fulltext::FulltextArena::new()
                    };

                    let arena = super::fulltext::build_incremental_fulltext_arena(
                        &prior_arena,
                        &group_entries,
                        &novelty.fulltext_string_bytes,
                        &analyzer,
                    );

                    if arena.is_empty() {
                        if let Some(ga) = arenas_by_gid.get_mut(&g_id) {
                            if let Some(pos) = ga
                                .fulltext
                                .iter()
                                .position(|f| f.p_id == p_id && f.lang_id == lang_id)
                            {
                                let old = &ga.fulltext[pos];
                                root_builder.add_replaced_cids(vec![old.arena_cid.clone()]);
                                ga.fulltext.remove(pos);
                            }
                        }
                        continue;
                    }

                    let blob = arena.encode();
                    let arena_cid = content_store
                        .put(ContentKind::IndexLeaf, &blob)
                        .await
                        .map_err(|e| {
                            IndexerError::StorageWrite(format!("fulltext CAS write: {e}"))
                        })?;

                    let new_ref = FulltextArenaRef {
                        p_id,
                        lang_id,
                        arena_cid,
                    };

                    let ga = arenas_by_gid.entry(g_id).or_insert_with(|| GraphArenaRefs {
                        g_id,
                        numbig: Vec::new(),
                        vectors: Vec::new(),
                        spatial: Vec::new(),
                        fulltext: vec![],
                    });

                    if let Some(pos) = ga
                        .fulltext
                        .iter()
                        .position(|f| f.p_id == p_id && f.lang_id == lang_id)
                    {
                        let old = &ga.fulltext[pos];
                        root_builder.add_replaced_cids(vec![old.arena_cid.clone()]);
                        ga.fulltext[pos] = new_ref;
                    } else {
                        ga.fulltext.push(new_ref);
                        ga.fulltext.sort_by_key(|f| (f.p_id, f.lang_id));
                    }

                    tracing::debug!(
                        g_id,
                        p_id,
                        lang_id,
                        language = ?language,
                        docs = arena.doc_count(),
                        terms = arena.terms().len(),
                        "incremental V6: fulltext arena rebuilt"
                    );
                }
            }

            // ---- Spatial arena rebuild (per affected predicate) ----
            //
            // For each (g_id, p_id) with novelty spatial entries, rebuild the
            // spatial index from scratch: load all prior entries from the existing
            // snapshot, combine with novelty, build + upload a new index.
            // Unchanged spatial arenas carry forward by CID.
            if has_new_spatial {
                let spatial_entries = novelty
                    .shared
                    .spatial_hook
                    .as_ref()
                    .map(super::super::spatial_hook::SpatialHook::entries)
                    .unwrap_or(&[]);

                // Group novelty entries by (g_id, p_id).
                let mut grouped: BTreeMap<(u16, u32), Vec<&crate::spatial_hook::SpatialEntry>> =
                    BTreeMap::new();
                for entry in spatial_entries {
                    grouped
                        .entry((entry.g_id, entry.p_id))
                        .or_default()
                        .push(entry);
                }

                for ((g_id, p_id), new_entries) in grouped {
                    let pred_iri = novelty
                        .shared
                        .predicates
                        .resolve(p_id)
                        .unwrap_or("unknown")
                        .to_string();

                    let config = fluree_db_spatial::SpatialCreateConfig::new(
                        format!("spatial:g{g_id}p{p_id}"),
                        ledger_id.to_string(),
                        pred_iri.clone(),
                    );
                    let mut builder = fluree_db_spatial::SpatialIndexBuilder::new(config);

                    // Load prior entries from the existing spatial snapshot (if any).
                    let ga = arenas_by_gid.get(&g_id);
                    let existing_ref = ga.and_then(|a| a.spatial.iter().find(|s| s.p_id == p_id));
                    let mut prior_count = 0u64;

                    if let Some(sp_ref) = existing_ref {
                        // Load the SpatialIndexRoot + snapshot from CAS.
                        let root_bytes =
                            content_store.get(&sp_ref.root_cid).await.map_err(|e| {
                                IndexerError::StorageRead(format!(
                                    "spatial root load for g{g_id}:p{p_id}: {e}"
                                ))
                            })?;
                        let spatial_root: fluree_db_spatial::SpatialIndexRoot =
                            serde_json::from_slice(&root_bytes).map_err(|e| {
                                IndexerError::StorageRead(format!(
                                    "spatial root decode for g{g_id}:p{p_id}: {e}"
                                ))
                            })?;

                        // Pre-fetch all blobs for the sync load_from_cas closure.
                        let mut blob_cache: std::collections::HashMap<String, Vec<u8>> =
                            std::collections::HashMap::new();
                        for cid in [&sp_ref.manifest, &sp_ref.arena] {
                            let bytes = content_store.get(cid).await.map_err(|e| {
                                IndexerError::StorageRead(format!("spatial blob fetch: {e}"))
                            })?;
                            blob_cache.insert(cid.digest_hex(), bytes);
                        }
                        for leaflet_cid in &sp_ref.leaflets {
                            let bytes = content_store.get(leaflet_cid).await.map_err(|e| {
                                IndexerError::StorageRead(format!("spatial leaflet fetch: {e}"))
                            })?;
                            blob_cache.insert(leaflet_cid.digest_hex(), bytes);
                        }

                        let cache_arc = std::sync::Arc::new(blob_cache);
                        match fluree_db_spatial::SpatialIndexSnapshot::load_from_cas(
                            spatial_root,
                            move |hash| {
                                cache_arc.get(hash).cloned().ok_or_else(|| {
                                    fluree_db_spatial::error::SpatialError::ChunkNotFound(
                                        hash.to_string(),
                                    )
                                })
                            },
                        ) {
                            Ok(snapshot) => {
                                let all_entries = snapshot
                                    .cell_index()
                                    .scan_range(0, u64::MAX)
                                    .unwrap_or_default();
                                for ce in &all_entries {
                                    if let Some(arena_entry) = snapshot.arena().get(ce.geo_handle) {
                                        if let Ok(wkt_str) = std::str::from_utf8(&arena_entry.wkt) {
                                            let _ = builder.add_geometry(
                                                ce.subject_id,
                                                wkt_str,
                                                ce.t,
                                                ce.is_assert(),
                                            );
                                            prior_count += 1;
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                return Err(IndexerError::IncrementalAbort(format!(
                                    "spatial snapshot load failed for g_id={g_id}, \
                                     p_id={p_id}: {e}; falling back to full rebuild \
                                     for correctness"
                                )));
                            }
                        }
                    }

                    // Feed novelty entries.
                    for entry in &new_entries {
                        let _ = builder.add_geometry(
                            entry.subject_id,
                            &entry.wkt,
                            entry.t,
                            entry.is_assert,
                        );
                    }

                    let build_result = builder.build().map_err(|e| {
                        IndexerError::Other(format!("spatial build g{g_id}:p{p_id}: {e}"))
                    })?;

                    if build_result.entries.is_empty() {
                        continue;
                    }

                    // Upload via the same two-phase pattern as the full build.
                    let mut pending_blobs: Vec<(String, Vec<u8>)> = Vec::new();
                    let write_result = build_result
                        .write_to_cas(|bytes| {
                            use sha2::{Digest, Sha256};
                            let hash_hex = hex::encode(Sha256::digest(bytes));
                            pending_blobs.push((hash_hex.clone(), bytes.to_vec()));
                            Ok(hash_hex)
                        })
                        .map_err(|e| IndexerError::Other(format!("spatial CAS build: {e}")))?;

                    for (_hash, blob_bytes) in &pending_blobs {
                        content_store
                            .put(ContentKind::SpatialIndex, blob_bytes)
                            .await
                            .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
                    }

                    // Build CIDs.
                    let spatial_codec = ContentKind::SpatialIndex.to_codec();
                    let root_json = serde_json::to_vec(&write_result.root)
                        .map_err(|e| IndexerError::Other(format!("spatial root serialize: {e}")))?;
                    let root_cid = content_store
                        .put(ContentKind::SpatialIndex, &root_json)
                        .await
                        .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
                    let manifest_cid =
                        ContentId::from_hex_digest(spatial_codec, &write_result.manifest_address)
                            .ok_or_else(|| {
                            IndexerError::StorageWrite(format!(
                                "invalid spatial manifest hash for g_id={g_id}, p_id={p_id}: {}",
                                write_result.manifest_address
                            ))
                        })?;
                    let arena_cid =
                        ContentId::from_hex_digest(spatial_codec, &write_result.arena_address)
                            .ok_or_else(|| {
                                IndexerError::StorageWrite(format!(
                                    "invalid spatial arena hash for g_id={g_id}, p_id={p_id}: {}",
                                    write_result.arena_address
                                ))
                            })?;
                    let leaflet_cids: Vec<ContentId> = write_result
                        .leaflet_addresses
                        .iter()
                        .enumerate()
                        .map(|(i, h)| {
                            ContentId::from_hex_digest(spatial_codec, h).ok_or_else(|| {
                                IndexerError::StorageWrite(format!(
                                    "invalid spatial leaflet hash [{i}] for \
                                     g_id={g_id}, p_id={p_id}: {h}"
                                ))
                            })
                        })
                        .collect::<Result<Vec<_>>>()?;

                    let new_ref = SpatialArenaRef {
                        p_id,
                        root_cid,
                        manifest: manifest_cid,
                        arena: arena_cid,
                        leaflets: leaflet_cids,
                    };

                    // Replace or insert in graph arenas.
                    let ga = arenas_by_gid.entry(g_id).or_insert_with(|| GraphArenaRefs {
                        g_id,
                        numbig: Vec::new(),
                        vectors: Vec::new(),
                        spatial: Vec::new(),
                        fulltext: vec![],
                    });

                    if let Some(pos) = ga.spatial.iter().position(|s| s.p_id == p_id) {
                        // GC old spatial CIDs.
                        let old = &ga.spatial[pos];
                        root_builder.add_replaced_cids(vec![
                            old.root_cid.clone(),
                            old.manifest.clone(),
                            old.arena.clone(),
                        ]);
                        root_builder.add_replaced_cids(old.leaflets.clone());
                        ga.spatial[pos] = new_ref;
                    } else {
                        ga.spatial.push(new_ref);
                        ga.spatial.sort_by_key(|s| s.p_id);
                    }

                    tracing::debug!(
                        g_id,
                        p_id,
                        predicate = %pred_iri,
                        prior_entries = prior_count,
                        novelty_entries = new_entries.len(),
                        "incremental V6: spatial index rebuilt for (graph, predicate)"
                    );
                }
            }

            let updated_arenas: Vec<GraphArenaRefs> = arenas_by_gid.into_values().collect();
            root_builder.set_graph_arenas(updated_arenas);

            tracing::debug!(
                "Phase 3a complete: arena updates (numbig + vectors + fulltext + spatial)"
            );
        }
    }

    // ---- Snapshot language dict AFTER arena builds ----
    //
    // Arena building can extend the language dict (notably the fulltext
    // builder resolves or inserts `"en"` to bucket `@fulltext`-datatype
    // values). Snapshotting here guarantees the persisted `language_tags`
    // includes every `lang_id` referenced by arena refs in the root.
    let new_language_tags: Vec<String> = novelty
        .shared
        .languages
        .iter()
        .map(|(_, tag)| tag.to_string())
        .collect();
    root_builder.set_language_tags(new_language_tags);

    // ---- Phase 3b: Stats / HLL refresh ----
    // Load prior sketches, feed novelty records, upload updated sketch.
    {
        use crate::stats;

        // Load prior sketches from the base root's sketch_ref (if present).
        let prior_properties = if let Some(ref cid) = base_root.sketch_ref {
            match stats::load_sketch_blob(content_store.as_ref(), cid).await {
                Ok(Some(blob)) => match blob.into_properties() {
                    Ok(props) => {
                        tracing::debug!(
                            entries = props.len(),
                            "loaded prior HLL sketches for incremental refresh"
                        );
                        props
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "failed to decode prior sketches, starting fresh");
                        std::collections::HashMap::new()
                    }
                },
                Ok(None) => {
                    tracing::debug!(
                        "sketch blob CID present but content not found, starting fresh"
                    );
                    std::collections::HashMap::new()
                }
                Err(e) => {
                    tracing::warn!(error = %e, "failed to load sketch blob, starting fresh");
                    std::collections::HashMap::new()
                }
            }
        } else {
            std::collections::HashMap::new()
        };

        // Seed hook with prior properties.
        let rdf_type_p_id = novelty
            .shared
            .predicates
            .get(fluree_vocab::rdf::TYPE)
            .unwrap_or(u32::MAX);
        let mut stats_hook = stats::IdStatsHook::with_prior_properties(prior_properties);
        stats_hook.set_rdf_type_p_id(rdf_type_p_id);
        stats_hook.set_track_ref_targets(true);

        // Seed per-graph flake totals from base root stats.
        if let Some(ref base_stats) = base_root.stats {
            if let Some(ref graphs) = base_stats.graphs {
                for g in graphs {
                    *stats_hook.graph_flakes_mut().entry(g.g_id).or_insert(0) += g.flakes as i64;
                }
            }
        }

        // Feed all novelty records.
        for (i, rec) in novelty.records.iter().enumerate() {
            let op = novelty.ops[i];
            let sr = stats::stats_record_from_v2(rec, op);
            stats_hook.on_record(&sr);
        }

        // Upload HLL sketches (before finalize consumes the hook).
        let sketch_ref = {
            let sketch_blob =
                stats::HllSketchBlob::from_properties(novelty.max_t, stats_hook.properties());
            if !sketch_blob.entries.is_empty() {
                let sketch_bytes = sketch_blob
                    .to_json_bytes()
                    .map_err(|e| IndexerError::StorageWrite(format!("sketch serialize: {e}")))?;
                let cid = content_store
                    .put(ContentKind::StatsSketch, &sketch_bytes)
                    .await
                    .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
                tracing::debug!(
                    %cid,
                    bytes = sketch_bytes.len(),
                    entries = sketch_blob.entries.len(),
                    "incremental V6: HLL sketch uploaded"
                );
                Some(cid)
            } else {
                None
            }
        };

        // Move per-subject maps out of the hook (avoids cloning).
        let (
            id_stats_result,
            class_count_deltas,
            novelty_subject_class_deltas,
            novelty_subject_props,
            novelty_subject_prop_dts,
            novelty_subject_prop_langs,
            subject_ref_history,
        ) = stats_hook.finalize_into_parts();

        // Build IndexStats with per-graph and aggregate properties.
        let trie = fluree_db_core::PrefixTrie::from_namespace_codes(&novelty.shared.ns_prefixes);
        let db_stats = {
            use fluree_db_core::index_stats as is;

            let properties = crate::stats::aggregate_property_entries_from_graphs(
                &id_stats_result.graphs,
                &trie,
                |p_id| {
                    novelty
                        .shared
                        .predicates
                        .resolve(p_id)
                        .map(ToString::to_string)
                },
            );

            // Class-property attribution: build full ClassStatEntry with property usage.
            //
            // Strategy:
            // 1. Load store for PSOT lookup + SID resolution
            // 2. Per-graph batched PSOT lookup for base class memberships
            // 3. Merge base + novelty class deltas → subject→classes
            // 4. Cross-reference subject→properties with subject→classes
            // 5. Build ClassStatEntry with datatypes, langs, ref-class edges
            // 6. Apply count deltas to base entries, add new entries
            let mut final_graphs = id_stats_result.graphs;

            let has_class_changes =
                !class_count_deltas.is_empty() || !novelty_subject_class_deltas.is_empty();

            if has_class_changes || !novelty_subject_props.is_empty() {
                let phase3b_started = Instant::now();
                tracing::debug!(
                    has_class_changes,
                    novelty_subject_props = novelty_subject_props.len(),
                    novelty_subject_class_deltas = novelty_subject_class_deltas.len(),
                    novelty_records = novelty.records.len(),
                    "Phase 3b: class-property attribution starting"
                );
                // subject_classes: (g_id, s_id) → HashSet<class_sid64>
                let mut subject_classes: std::collections::HashMap<
                    (u16, u64),
                    std::collections::HashSet<u64>,
                > = std::collections::HashMap::new();

                // SID resolver closure (populated when store loads successfully).
                let resolve_class_sid_calls = AtomicUsize::new(0usize);
                let resolve_class_sid_novelty_hits = AtomicUsize::new(0usize);
                let resolve_class_sid_store_hits = AtomicUsize::new(0usize);
                let resolve_class_sid_fallbacks = AtomicUsize::new(0usize);
                let resolve_class_sid =
                    |sid64: u64,
                     store: Option<
                        &fluree_db_binary_index::read::binary_index_store::BinaryIndexStore,
                    >,
                     new_subs: &std::collections::HashMap<(u16, u64), String>|
                     -> fluree_db_core::Sid {
                        resolve_class_sid_calls.fetch_add(1, Ordering::Relaxed);
                        let sid = fluree_db_core::subject_id::SubjectId::from_u64(sid64);
                        // Try novelty subjects first.
                        if let Some(suffix) = new_subs.get(&(sid.ns_code(), sid.local_id())) {
                            resolve_class_sid_novelty_hits.fetch_add(1, Ordering::Relaxed);
                            return fluree_db_core::Sid::new(sid.ns_code(), suffix.as_str());
                        }
                        // Try store resolution.
                        if let Some(s) = store {
                            if let Ok(iri) = s.resolve_subject_iri(sid64) {
                                resolve_class_sid_store_hits.fetch_add(1, Ordering::Relaxed);
                                return s.encode_iri(&iri);
                            }
                        }
                        // Fallback: ns_code + local_id (will be opaque but stable).
                        resolve_class_sid_fallbacks.fetch_add(1, Ordering::Relaxed);
                        fluree_db_core::Sid::new(sid.ns_code(), sid.local_id().to_string())
                    };

                let new_subject_suffix: std::collections::HashMap<(u16, u64), String> = novelty
                    .new_subjects
                    .iter()
                    .filter_map(|(ns_code, local_id, suffix)| {
                        let s = std::str::from_utf8(suffix).ok()?.to_string();
                        Some(((*ns_code, *local_id), s))
                    })
                    .collect();

                // Load store for PSOT lookup + SID resolution.
                let store_load_started = Instant::now();
                let store_opt = if rdf_type_p_id != u32::MAX {
                    match fluree_db_binary_index::read::binary_index_store::BinaryIndexStore::load_from_root_v6(
                        content_store.clone(),
                        base_root,
                        &cache_dir,
                        None,
                    )
                    .await
                    {
                        Ok(s) => Some(Arc::new(s)),
                        Err(e) => {
                            tracing::warn!(error = %e, "V6 store load for class attribution failed");
                            None
                        }
                    }
                } else {
                    None
                };
                tracing::debug!(
                    loaded = store_opt.is_some(),
                    elapsed_ms = store_load_started.elapsed().as_millis() as u64,
                    "Phase 3b: class attribution store load complete"
                );

                // Partition novelty subjects by g_id for per-graph PSOT scans.
                // Also include ref-object s_ids so we can resolve ref-class edges.
                if let Some(ref store) = store_opt {
                    let iri_ref_otype = fluree_db_core::o_type::OType::IRI_REF.as_u16();
                    let mut subjects_by_graph: std::collections::HashMap<u16, Vec<u64>> =
                        std::collections::HashMap::new();
                    for &(g_id, s_id) in novelty_subject_class_deltas
                        .keys()
                        .chain(novelty_subject_props.keys())
                    {
                        subjects_by_graph.entry(g_id).or_default().push(s_id);
                    }
                    // Add ref-object targets so we can look up their class memberships.
                    for (i, rec) in novelty.records.iter().enumerate() {
                        if rec.o_type == iri_ref_otype
                            && rec.p_id != rdf_type_p_id
                            && novelty.ops[i] != 0
                        {
                            subjects_by_graph
                                .entry(rec.g_id)
                                .or_default()
                                .push(rec.o_key);
                        }
                    }
                    // Dedup within each graph.
                    for sids in subjects_by_graph.values_mut() {
                        sids.sort_unstable();
                        sids.dedup();
                    }

                    let total_subjects: usize =
                        subjects_by_graph.values().map(std::vec::Vec::len).sum();
                    tracing::debug!(
                        graphs = subjects_by_graph.len(),
                        total_subjects,
                        rdf_type_p_id,
                        "Phase 3b: prepared base class lookup subject sets"
                    );
                    for (&g, sids) in &subjects_by_graph {
                        if sids.is_empty() {
                            continue;
                        }
                        let min_s = sids[0];
                        let max_s = *sids.last().unwrap_or(&min_s);
                        tracing::debug!(
                            g_id = g,
                            subjects = sids.len(),
                            min_s_id = min_s,
                            max_s_id = max_s,
                            span = max_s.saturating_sub(min_s),
                            "Phase 3b: graph subject range for rdf:type scan"
                        );
                    }

                    // Batched PSOT lookup per graph.
                    for (&scan_g_id, scan_sids) in &subjects_by_graph {
                        if scan_sids.is_empty() {
                            continue;
                        }
                        let min_s = scan_sids[0];
                        let max_s = *scan_sids.last().unwrap_or(&min_s);
                        tracing::debug!(
                            g_id = scan_g_id,
                            subjects = scan_sids.len(),
                            min_s_id = min_s,
                            max_s_id = max_s,
                            span = max_s.saturating_sub(min_s),
                            "Phase 3b: starting batched PSOT rdf:type lookup"
                        );
                        let started = Instant::now();
                        match fluree_db_binary_index::batched_lookup_predicate_refs(
                            store,
                            scan_g_id,
                            rdf_type_p_id,
                            scan_sids,
                            base_root.index_t,
                        ) {
                            Ok(base_map) => {
                                tracing::debug!(
                                    g_id = scan_g_id,
                                    subjects_with_hits = base_map.len(),
                                    elapsed_ms = started.elapsed().as_millis() as u64,
                                    "Phase 3b: completed batched PSOT rdf:type lookup"
                                );
                                for (s_id, classes) in base_map {
                                    subject_classes
                                        .entry((scan_g_id, s_id))
                                        .or_default()
                                        .extend(classes);
                                }
                            }
                            Err(e) => {
                                tracing::warn!(
                                    g_id = scan_g_id, error = %e,
                                    "batched PSOT class lookup failed for graph"
                                );
                            }
                        }
                    }
                }

                // Apply novelty rdf:type deltas on top of base memberships.
                let subject_classes_started = Instant::now();
                for (&(g_id, s_id), class_map) in &novelty_subject_class_deltas {
                    let set = subject_classes.entry((g_id, s_id)).or_default();
                    for (&class_sid64, &delta) in class_map {
                        if delta > 0 {
                            set.insert(class_sid64);
                        } else {
                            set.remove(&class_sid64);
                        }
                    }
                }
                let total_subject_class_links: usize = subject_classes
                    .values()
                    .map(std::collections::HashSet::len)
                    .sum();
                tracing::debug!(
                    subjects = subject_classes.len(),
                    class_links = total_subject_class_links,
                    elapsed_ms = subject_classes_started.elapsed().as_millis() as u64,
                    phase_elapsed_ms = phase3b_started.elapsed().as_millis() as u64,
                    "Phase 3b: subject class membership ready"
                );

                // Build class→properties, class→prop→dts, class→prop→langs, ref_edges.
                let attribution_maps_started = Instant::now();
                let mut class_properties: std::collections::HashMap<
                    (u16, u64),
                    std::collections::HashSet<u32>,
                > = std::collections::HashMap::new();
                let mut class_prop_dts: std::collections::HashMap<
                    (u16, u64),
                    std::collections::HashMap<u32, std::collections::HashMap<u8, i64>>,
                > = std::collections::HashMap::new();
                let mut class_prop_lang_deltas: std::collections::HashMap<
                    (u16, u64),
                    std::collections::HashMap<u32, std::collections::HashMap<u16, i64>>,
                > = std::collections::HashMap::new();
                let mut ref_edges: std::collections::HashMap<
                    (u16, u64),
                    std::collections::HashMap<u32, std::collections::HashMap<u64, i64>>,
                > = std::collections::HashMap::new();

                for (&(g_id, s_id), props) in &novelty_subject_props {
                    if let Some(classes) = subject_classes.get(&(g_id, s_id)) {
                        for &class_sid64 in classes {
                            class_properties
                                .entry((g_id, class_sid64))
                                .or_default()
                                .extend(props);
                            if let Some(s_dts) = novelty_subject_prop_dts.get(&(g_id, s_id)) {
                                for (&pid, dt_map) in s_dts {
                                    let cp = class_prop_dts
                                        .entry((g_id, class_sid64))
                                        .or_default()
                                        .entry(pid)
                                        .or_default();
                                    for (&dt, &cnt) in dt_map {
                                        *cp.entry(dt).or_insert(0) += cnt;
                                    }
                                }
                            }
                            if let Some(s_langs) = novelty_subject_prop_langs.get(&(g_id, s_id)) {
                                for (&pid, lang_map) in s_langs {
                                    let cl = class_prop_lang_deltas
                                        .entry((g_id, class_sid64))
                                        .or_default()
                                        .entry(pid)
                                        .or_default();
                                    for (&lid, &cnt) in lang_map {
                                        *cl.entry(lid).or_insert(0) += cnt;
                                    }
                                }
                            }
                        }
                    }
                }
                for (&(g_id, subj), per_prop) in &subject_ref_history {
                    let Some(subj_classes) = subject_classes.get(&(g_id, subj)) else {
                        continue;
                    };
                    for (&pid, objs) in per_prop {
                        for (&obj, &delta) in objs {
                            if delta == 0 {
                                continue;
                            }
                            let Some(obj_classes) = subject_classes.get(&(g_id, obj)) else {
                                continue;
                            };
                            for &sc in subj_classes {
                                for &oc in obj_classes {
                                    *ref_edges
                                        .entry((g_id, sc))
                                        .or_default()
                                        .entry(pid)
                                        .or_default()
                                        .entry(oc)
                                        .or_insert(0) += delta;
                                }
                            }
                        }
                    }
                }
                let class_property_count: usize = class_properties
                    .values()
                    .map(std::collections::HashSet::len)
                    .sum();
                let class_ref_edge_count: usize = ref_edges
                    .values()
                    .map(|per_prop| {
                        per_prop
                            .values()
                            .map(std::collections::HashMap::len)
                            .sum::<usize>()
                    })
                    .sum();
                tracing::debug!(
                    classes_with_properties = class_properties.len(),
                    class_property_count,
                    classes_with_ref_edges = ref_edges.len(),
                    class_ref_edge_count,
                    elapsed_ms = attribution_maps_started.elapsed().as_millis() as u64,
                    phase_elapsed_ms = phase3b_started.elapsed().as_millis() as u64,
                    "Phase 3b: class attribution maps built"
                );

                // Build lang_id → tag string map from the language dict (keyed by id, not Vec index).
                let lang_id_to_tag: std::collections::HashMap<u16, String> = novelty
                    .shared
                    .languages
                    .iter()
                    .map(|(lang_id, tag)| (lang_id, tag.to_string()))
                    .collect();

                // Build entries_by_key from base + apply deltas.
                let seed_entries_started = Instant::now();
                let base_class_sid_lookups = AtomicUsize::new(0usize);
                let base_class_sid_hits = AtomicUsize::new(0usize);
                let mut entries_by_key: std::collections::HashMap<(u16, u64), is::ClassStatEntry> =
                    std::collections::HashMap::new();

                // Seed from base root per-graph class entries.
                if let Some(ref base_stats) = base_root.stats {
                    if let Some(ref graphs) = base_stats.graphs {
                        for g in graphs {
                            if let Some(ref classes) = g.classes {
                                for entry in classes {
                                    // Try to resolve class Sid → sid64 via store.
                                    base_class_sid_lookups.fetch_add(1, Ordering::Relaxed);
                                    let sid64 = store_opt
                                        .as_ref()
                                        .and_then(|s| {
                                            s.find_subject_id_by_parts(
                                                entry.class_sid.namespace_code,
                                                &entry.class_sid.name,
                                            )
                                            .ok()
                                            .flatten()
                                        })
                                        .unwrap_or(0);
                                    if sid64 != 0 {
                                        base_class_sid_hits.fetch_add(1, Ordering::Relaxed);
                                        entries_by_key.insert((g.g_id, sid64), entry.clone());
                                    }
                                }
                            }
                        }
                    }
                }
                tracing::debug!(
                    seeded_entries = entries_by_key.len(),
                    base_class_sid_lookups = base_class_sid_lookups.load(Ordering::Relaxed),
                    base_class_sid_hits = base_class_sid_hits.load(Ordering::Relaxed),
                    elapsed_ms = seed_entries_started.elapsed().as_millis() as u64,
                    phase_elapsed_ms = phase3b_started.elapsed().as_millis() as u64,
                    "Phase 3b: seeded class entries from base stats"
                );

                // Apply class count deltas (both positive and negative).
                let class_count_delta_started = Instant::now();
                for (&(g_id, class_sid64), &delta) in &class_count_deltas {
                    let entry = entries_by_key
                        .entry((g_id, class_sid64))
                        .or_insert_with(|| {
                            let class_sid = resolve_class_sid(
                                class_sid64,
                                store_opt.as_deref(),
                                &new_subject_suffix,
                            );
                            is::ClassStatEntry {
                                class_sid,
                                count: 0,
                                properties: Vec::new(),
                            }
                        });
                    entry.count = (entry.count as i64 + delta).max(0) as u64;
                }
                tracing::debug!(
                    class_count_deltas = class_count_deltas.len(),
                    entries = entries_by_key.len(),
                    elapsed_ms = class_count_delta_started.elapsed().as_millis() as u64,
                    phase_elapsed_ms = phase3b_started.elapsed().as_millis() as u64,
                    "Phase 3b: applied class count deltas"
                );

                // Build property attribution for each class entry.
                let property_merge_started = Instant::now();
                let ref_class_sid_lookups = AtomicUsize::new(0usize);
                let ref_class_sid_hits = AtomicUsize::new(0usize);
                let mut merged_class_entries = 0usize;
                let total_class_entries = entries_by_key.len();
                for (&(g_id, class_sid64), entry) in &mut entries_by_key {
                    if let Some(props) = class_properties.get(&(g_id, class_sid64)) {
                        let class_dts = class_prop_dts.get(&(g_id, class_sid64));
                        let class_refs = ref_edges.get(&(g_id, class_sid64));
                        let class_langs = class_prop_lang_deltas.get(&(g_id, class_sid64));

                        // Merge novelty properties with existing properties.
                        let mut prop_set: std::collections::HashSet<u32> = entry
                            .properties
                            .iter()
                            .filter_map(|pu| {
                                novelty.shared.predicates.get(&format!(
                                    "{}{}",
                                    novelty
                                        .shared
                                        .ns_prefixes
                                        .get(&pu.property_sid.namespace_code)
                                        .map(std::string::String::as_str)
                                        .unwrap_or(""),
                                    pu.property_sid.name
                                ))
                            })
                            .collect();
                        prop_set.extend(props);

                        // Index base property usage by p_id for merging.
                        let base_prop_by_pid: std::collections::HashMap<
                            u32,
                            &is::ClassPropertyUsage,
                        > = entry
                            .properties
                            .iter()
                            .filter_map(|pu| {
                                let iri = format!(
                                    "{}{}",
                                    novelty
                                        .shared
                                        .ns_prefixes
                                        .get(&pu.property_sid.namespace_code)
                                        .map(std::string::String::as_str)
                                        .unwrap_or(""),
                                    pu.property_sid.name
                                );
                                novelty.shared.predicates.get(&iri).map(|pid| (pid, pu))
                            })
                            .collect();

                        entry.properties = prop_set
                            .iter()
                            .map(|&cp_id| {
                                let iri = novelty.shared.predicates.resolve(cp_id).unwrap_or("");
                                let p_sid = match trie.longest_match(iri) {
                                    Some((code, plen)) => {
                                        fluree_db_core::Sid::new(code, &iri[plen..])
                                    }
                                    None => fluree_db_core::Sid::new(0, iri),
                                };
                                let base_pu = base_prop_by_pid.get(&cp_id).copied();

                                // Merge base + novelty datatypes.
                                let mut merged_dts: std::collections::HashMap<u8, i64> =
                                    std::collections::HashMap::new();
                                if let Some(bpu) = base_pu {
                                    for &(dt, cnt) in &bpu.datatypes {
                                        *merged_dts.entry(dt).or_insert(0) += cnt as i64;
                                    }
                                }
                                if let Some(dt_delta) = class_dts.and_then(|m| m.get(&cp_id)) {
                                    for (&dt, &cnt) in dt_delta {
                                        *merged_dts.entry(dt).or_insert(0) += cnt;
                                    }
                                }
                                let datatypes: Vec<(u8, u64)> = merged_dts
                                    .into_iter()
                                    .filter(|(_, v)| *v > 0)
                                    .map(|(dt, v)| (dt, v as u64))
                                    .collect();

                                // Merge base + novelty langs.
                                let mut merged_langs: std::collections::HashMap<String, i64> =
                                    std::collections::HashMap::new();
                                if let Some(bpu) = base_pu {
                                    for (tag, cnt) in &bpu.langs {
                                        *merged_langs.entry(tag.clone()).or_insert(0) +=
                                            *cnt as i64;
                                    }
                                }
                                if let Some(lang_delta) = class_langs.and_then(|m| m.get(&cp_id)) {
                                    for (&lid, &cnt) in lang_delta {
                                        let tag = lang_id_to_tag
                                            .get(&lid)
                                            .cloned()
                                            .unwrap_or_else(|| format!("lang:{lid}"));
                                        *merged_langs.entry(tag).or_insert(0) += cnt;
                                    }
                                }
                                let langs: Vec<(String, u64)> = merged_langs
                                    .into_iter()
                                    .filter(|(_, v)| *v > 0)
                                    .map(|(tag, v)| (tag, v as u64))
                                    .collect();

                                // Merge base + novelty ref_classes.
                                let mut merged_refs: std::collections::HashMap<u64, i64> =
                                    std::collections::HashMap::new();
                                if let Some(bpu) = base_pu {
                                    for rc in &bpu.ref_classes {
                                        ref_class_sid_lookups.fetch_add(1, Ordering::Relaxed);
                                        let rc_sid64 = store_opt
                                            .as_ref()
                                            .and_then(|s| {
                                                s.find_subject_id_by_parts(
                                                    rc.class_sid.namespace_code,
                                                    &rc.class_sid.name,
                                                )
                                                .ok()
                                                .flatten()
                                            })
                                            .unwrap_or(0);
                                        if rc_sid64 != 0 {
                                            ref_class_sid_hits.fetch_add(1, Ordering::Relaxed);
                                            *merged_refs.entry(rc_sid64).or_insert(0) +=
                                                rc.count as i64;
                                        }
                                    }
                                }
                                if let Some(ref_delta) = class_refs.and_then(|m| m.get(&cp_id)) {
                                    for (&target, &cnt) in ref_delta {
                                        *merged_refs.entry(target).or_insert(0) += cnt;
                                    }
                                }
                                let ref_classes: Vec<is::ClassRefCount> = merged_refs
                                    .into_iter()
                                    .filter(|(_, v)| *v > 0)
                                    .map(|(target_sid64, cnt)| {
                                        let cs = resolve_class_sid(
                                            target_sid64,
                                            store_opt.as_deref(),
                                            &new_subject_suffix,
                                        );
                                        is::ClassRefCount {
                                            class_sid: cs,
                                            count: cnt as u64,
                                        }
                                    })
                                    .collect();

                                is::ClassPropertyUsage {
                                    property_sid: p_sid,
                                    datatypes,
                                    langs,
                                    ref_classes,
                                }
                            })
                            .collect();
                        merged_class_entries += 1;
                    }
                    // Resolve class SID if still placeholder.
                    if entry.class_sid.name.is_empty() && entry.class_sid.namespace_code == 0 {
                        entry.class_sid = resolve_class_sid(
                            class_sid64,
                            store_opt.as_deref(),
                            &new_subject_suffix,
                        );
                    }
                    if merged_class_entries > 0 && merged_class_entries.is_multiple_of(500) {
                        tracing::debug!(
                            merged_class_entries,
                            total_entries = total_class_entries,
                            ref_class_sid_lookups = ref_class_sid_lookups.load(Ordering::Relaxed),
                            ref_class_sid_hits = ref_class_sid_hits.load(Ordering::Relaxed),
                            resolve_class_sid_calls =
                                resolve_class_sid_calls.load(Ordering::Relaxed),
                            phase_elapsed_ms = phase3b_started.elapsed().as_millis() as u64,
                            elapsed_ms = property_merge_started.elapsed().as_millis() as u64,
                            "Phase 3b: property attribution merge progress"
                        );
                    }
                }
                tracing::debug!(
                    merged_class_entries,
                    total_entries = total_class_entries,
                    ref_class_sid_lookups = ref_class_sid_lookups.load(Ordering::Relaxed),
                    ref_class_sid_hits = ref_class_sid_hits.load(Ordering::Relaxed),
                    resolve_class_sid_calls = resolve_class_sid_calls.load(Ordering::Relaxed),
                    resolve_class_sid_novelty_hits =
                        resolve_class_sid_novelty_hits.load(Ordering::Relaxed),
                    resolve_class_sid_store_hits =
                        resolve_class_sid_store_hits.load(Ordering::Relaxed),
                    resolve_class_sid_fallbacks =
                        resolve_class_sid_fallbacks.load(Ordering::Relaxed),
                    elapsed_ms = property_merge_started.elapsed().as_millis() as u64,
                    phase_elapsed_ms = phase3b_started.elapsed().as_millis() as u64,
                    "Phase 3b: property attribution merge complete"
                );

                // Remove entries with count=0 (fully retracted classes).
                entries_by_key.retain(|_, e| e.count > 0);

                // Group into per-graph class lists.
                let mut by_graph: std::collections::HashMap<u16, Vec<is::ClassStatEntry>> =
                    std::collections::HashMap::new();
                for ((g_id, _), entry) in entries_by_key {
                    by_graph.entry(g_id).or_default().push(entry);
                }

                for g in &mut final_graphs {
                    if let Some(classes) = by_graph.remove(&g.g_id) {
                        g.classes = Some(classes);
                    }
                }
                tracing::debug!(
                    final_graphs = final_graphs.len(),
                    phase_elapsed_ms = phase3b_started.elapsed().as_millis() as u64,
                    "Phase 3b: class-property attribution complete"
                );
            }

            let root_classes = fluree_db_core::index_stats::union_per_graph_classes(&final_graphs);

            is::IndexStats {
                flakes: id_stats_result.total_flakes,
                size: 0,
                properties: Some(properties),
                classes: root_classes,
                graphs: Some(final_graphs),
            }
        };

        tracing::debug!(
            total_flakes = db_stats.flakes,
            property_count = db_stats.properties.as_ref().map_or(0, std::vec::Vec::len),
            "incremental V6: stats refreshed"
        );

        root_builder.set_stats(Some(db_stats));
        root_builder.set_sketch_ref(sketch_ref);
    }

    // ---- Phase 3c: Schema refresh (rdfs:subClassOf / rdfs:subPropertyOf) ----
    {
        use crate::stats::SchemaExtractor;
        use fluree_db_core::o_type::OType;
        use fluree_db_core::{Flake, FlakeValue, Sid};

        let rdfs_subclass_iri = format!("{}subClassOf", fluree_vocab::rdfs::NS);
        let rdfs_subprop_iri = format!("{}subPropertyOf", fluree_vocab::rdfs::NS);

        let subclass_p_id = novelty.shared.predicates.get(&rdfs_subclass_iri);
        let subprop_p_id = novelty.shared.predicates.get(&rdfs_subprop_iri);

        // Only run schema extraction if schema predicates exist and novelty has matching records.
        let has_schema_records = novelty.records.iter().any(|r| {
            (subclass_p_id == Some(r.p_id) || subprop_p_id == Some(r.p_id))
                && r.o_type == OType::IRI_REF.as_u16()
        });

        if has_schema_records {
            match fluree_db_binary_index::read::binary_index_store::BinaryIndexStore::load_from_root_v6(
                content_store.clone(),
                base_root,
                &cache_dir,
                None,
            )
            .await
            {
                Ok(store) => {
                    // Map new subjects for local resolution (not yet in base dicts).
                    let new_subject_suffix: std::collections::HashMap<(u16, u64), String> = novelty
                        .new_subjects
                        .iter()
                        .filter_map(|(ns_code, local_id, suffix)| {
                            let s = std::str::from_utf8(suffix).ok()?.to_string();
                            Some(((*ns_code, *local_id), s))
                        })
                        .collect();

                    let resolve_sid64 = |sid64: u64| -> Option<Sid> {
                        let sid = fluree_db_core::subject_id::SubjectId::from_u64(sid64);
                        let ns_code = sid.ns_code();
                        let local_id = sid.local_id();
                        if let Some(suffix) = new_subject_suffix.get(&(ns_code, local_id)) {
                            return Some(Sid::new(ns_code, suffix));
                        }
                        store
                            .resolve_subject_iri(sid64)
                            .ok()
                            .map(|iri| store.encode_iri(&iri))
                    };

                    let mut extractor = SchemaExtractor::from_prior(base_root.schema.as_ref());

                    for (i, record) in novelty.records.iter().enumerate() {
                        let is_subclass = subclass_p_id == Some(record.p_id);
                        let is_subprop = subprop_p_id == Some(record.p_id);
                        if !is_subclass && !is_subprop {
                            continue;
                        }
                        if record.o_type != OType::IRI_REF.as_u16() {
                            continue;
                        }

                        let Some(s_sid) = resolve_sid64(record.s_id.as_u64()) else {
                            continue;
                        };
                        let Some(o_sid) = resolve_sid64(record.o_key) else {
                            continue;
                        };

                        let p_sid = if is_subclass {
                            Sid::new(fluree_vocab::namespaces::RDFS, "subClassOf")
                        } else {
                            Sid::new(fluree_vocab::namespaces::RDFS, "subPropertyOf")
                        };

                        let flake = Flake::new(
                            s_sid,
                            p_sid,
                            FlakeValue::Ref(o_sid),
                            Sid::new(0, ""),
                            record.t as i64,
                            novelty.ops[i] != 0,
                            None,
                        );
                        extractor.on_flake(&flake);
                    }

                    let updated_schema = extractor.finalize(novelty.max_t);
                    root_builder.set_schema(updated_schema);
                    tracing::debug!("Phase 3c: incremental V6 schema refreshed");
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        "Phase 3c: V6 store load for schema extraction failed; \
                         carrying forward base schema"
                    );
                }
            }
        }
    }

    // ---- Phase 4: Root assembly ----
    root_builder.set_prev_index(Some(BinaryPrevIndexRef {
        t: base_root.index_t,
        id: base_root_id.clone(),
    }));

    let (new_root, replaced_cids) = root_builder.build();

    // Write garbage manifest.
    if !replaced_cids.is_empty() {
        let garbage_strings: Vec<String> = replaced_cids
            .iter()
            .map(std::string::ToString::to_string)
            .collect();
        let garbage_cid = gc::write_garbage_record(
            content_store.as_ref(),
            ledger_id,
            new_root.index_t,
            garbage_strings,
        )
        .await
        .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;

        // Set garbage on the root before encoding.
        let mut final_root = new_root;
        final_root.garbage = Some(BinaryGarbageRef { id: garbage_cid });
        if let Some(stats) = final_root.stats.as_mut() {
            stats.distribute_total_size_by_flakes(final_root.total_commit_size);
        }

        super::root_assembly::reconcile_ns_at_publish(
            &final_root.namespace_codes,
            &novelty.shared.ns_prefixes,
            final_root.index_t,
        )?;

        let root_bytes = final_root.encode();
        let root_id = content_store
            .put(ContentKind::IndexRoot, &root_bytes)
            .await
            .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;

        tracing::debug!(
            %root_id,
            index_t = final_root.index_t,
            replaced = replaced_cids.len(),
            new_leaves = total_new_leaves,
            "V6 incremental index root published"
        );

        Ok(IndexResult {
            root_id,
            index_t: final_root.index_t,
            ledger_id: ledger_id.to_string(),
            stats: IndexStats {
                flake_count: novelty.records.len(),
                leaf_count: total_new_leaves,
                branch_count: by_graph.len(),
                total_bytes: root_bytes.len(),
            },
        })
    } else {
        let mut final_root = new_root;
        if let Some(stats) = final_root.stats.as_mut() {
            stats.distribute_total_size_by_flakes(final_root.total_commit_size);
        }

        super::root_assembly::reconcile_ns_at_publish(
            &final_root.namespace_codes,
            &novelty.shared.ns_prefixes,
            final_root.index_t,
        )?;
        let root_bytes = final_root.encode();
        let root_id = content_store
            .put(ContentKind::IndexRoot, &root_bytes)
            .await
            .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;

        tracing::debug!(
            %root_id,
            index_t = final_root.index_t,
            new_leaves = total_new_leaves,
            "V6 incremental index root published (no garbage)"
        );

        Ok(IndexResult {
            root_id,
            index_t: final_root.index_t,
            ledger_id: ledger_id.to_string(),
            stats: IndexStats {
                flake_count: novelty.records.len(),
                leaf_count: total_new_leaves,
                branch_count: by_graph.len(),
                total_bytes: root_bytes.len(),
            },
        })
    }
}

// ============================================================================
// Helpers
// ============================================================================

/// Upload leaf and sidecar blobs from a branch update result.
///
/// Shared by all four code paths: default-graph existing/fresh, named-graph existing/fresh.
async fn upload_leaf_blobs(
    content_store: &dyn ContentStore,
    result: &BranchUpdateResult,
) -> Result<()> {
    for blob in &result.new_leaf_blobs {
        content_store
            .put(ContentKind::IndexLeaf, &blob.info.leaf_bytes)
            .await
            .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;

        if let Some(ref sc_bytes) = blob.info.sidecar_bytes {
            content_store
                .put(ContentKind::HistorySidecar, sc_bytes)
                .await
                .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
        }
    }
    Ok(())
}

/// Build a fresh V3 branch from pure novelty for the default graph.
fn build_fresh_default_graph_v3(
    sorted_records: &[RunRecordV2],
    // Kept for: filtering retractions in fresh branches once the rebuild
    // pipeline supports retraction-aware builds. Currently only assertions
    // appear in fresh branches so ops are not consulted.
    sorted_ops: &[u8],
    order: RunSortOrder,
    g_id: u16,
    config: &IndexerConfig,
) -> Result<BranchUpdateResult> {
    let _ = sorted_ops;
    use crate::run_index::build::incremental_leaf::NewLeafBlob;
    use fluree_db_binary_index::format::branch::{build_branch_bytes, LeafEntry};
    use fluree_db_binary_index::format::leaf::LeafWriter;
    use fluree_db_binary_index::format::run_record_v2::read_ordered_key_v2;

    let leaflet_target = config.leaflet_rows.max(1);
    let leaf_target = leaflet_target.saturating_mul(config.leaflets_per_leaf.max(1));

    let mut writer = LeafWriter::new(order, leaflet_target, leaf_target, 1);
    for rec in sorted_records {
        writer
            .push_record(*rec)
            .map_err(|e| IndexerError::StorageWrite(format!("V3 fresh branch writer: {e}")))?;
    }

    let leaf_infos = writer
        .finish()
        .map_err(|e| IndexerError::StorageWrite(format!("V3 fresh branch finish: {e}")))?;

    let mut leaf_entries = Vec::with_capacity(leaf_infos.len());
    let mut new_blobs = Vec::with_capacity(leaf_infos.len());

    for info in leaf_infos {
        let header = fluree_db_binary_index::format::leaf::decode_leaf_header_v3(&info.leaf_bytes)
            .map_err(|e| IndexerError::StorageWrite(format!("decode fresh leaf header: {e}")))?;
        let first_key = read_ordered_key_v2(order, &header.first_key);
        let last_key = read_ordered_key_v2(order, &header.last_key);

        leaf_entries.push(LeafEntry {
            first_key,
            last_key,
            row_count: info.total_rows,
            leaf_cid: info.leaf_cid.clone(),
            sidecar_cid: info.sidecar_cid.clone(),
        });

        new_blobs.push(NewLeafBlob { info });
    }

    let branch_bytes = build_branch_bytes(order, g_id, &leaf_entries);
    let branch_hash = fluree_db_core::sha256_hex(&branch_bytes);
    let branch_cid = ContentId::from_hex_digest(
        fluree_db_core::content_kind::CODEC_FLUREE_INDEX_BRANCH,
        &branch_hash,
    )
    .ok_or_else(|| IndexerError::StorageWrite(format!("invalid V3 branch hash: {branch_hash}")))?;

    Ok(BranchUpdateResult {
        leaf_entries,
        new_leaf_blobs: new_blobs,
        replaced_leaf_cids: Vec::new(),
        replaced_sidecar_cids: Vec::new(),
        branch_bytes,
        branch_cid,
    })
}

/// Build a fresh V3 branch from pure novelty for a named graph.
///
/// Same as `build_fresh_default_graph_v3` — the build pipeline is graph-agnostic.
fn build_fresh_named_graph_v3(
    sorted_records: &[RunRecordV2],
    order: RunSortOrder,
    g_id: u16,
    config: &IndexerConfig,
) -> Result<BranchUpdateResult> {
    build_fresh_default_graph_v3(sorted_records, &[], order, g_id, config)
}

/// Build predicate SIDs from resolver state for the index root.
fn build_predicate_sids(
    shared: &crate::run_index::resolve::resolver::SharedResolverState,
    namespace_codes: &std::collections::BTreeMap<u16, String>,
) -> Vec<(u16, String)> {
    use fluree_db_core::PrefixTrie;

    let ns_map: std::collections::HashMap<u16, String> = namespace_codes
        .iter()
        .map(|(&k, v)| (k, v.clone()))
        .collect();
    let trie = PrefixTrie::from_namespace_codes(&ns_map);

    (0..shared.predicates.len())
        .map(|p_id| {
            let iri = shared.predicates.resolve(p_id).unwrap_or("");
            if let Some((ns_code, prefix_len)) = trie.longest_match(iri) {
                let suffix = &iri[prefix_len..];
                (ns_code, suffix.to_string())
            } else {
                (0, iri.to_string())
            }
        })
        .collect()
}
