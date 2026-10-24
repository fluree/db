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

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use fluree_db_binary_index::format::branch::LeafEntry;
use fluree_db_binary_index::format::run_record::RunSortOrder;
use fluree_db_binary_index::format::run_record_v2::{cmp_v2_for_order, RunRecordV2};
use fluree_db_binary_index::{BinaryPrevIndexRef, LeafletCache};
use fluree_db_core::{ContentBytes, ContentId, ContentKind, ContentStore};
use futures::stream::{self, StreamExt, TryStreamExt};

use crate::error::{IndexerError, Result};
use crate::run_index::build::graph_scope::{GraphSize, PlacementPolicy};
use crate::run_index::build::incremental_branch::{
    touched_leaf_refs, update_branch_streaming, BranchUpdateConfig, BranchUpdateMeta,
    BranchUpdateResult, PoolUpdate,
};
use crate::run_index::build::incremental_leaf::{GraphDelta, GraphDeltas, NewLeafBlob};
use crate::run_index::build::incremental_resolve::{
    resolve_incremental_commits_v6, IncrementalResolveConfig, IncrementalResolveError,
};
use crate::run_index::build::incremental_root::IncrementalRootBuilder;
use crate::run_index::build::novelty_merge::OP_PURGE;
use crate::{IndexResult, IndexStats, IndexerConfig};
use tokio::sync::Semaphore;

#[derive(Default)]
struct Phase2FetchStats {
    leaf_fetches: AtomicU64,
    leaf_cache_hits: AtomicU64,
    leaf_cache_misses: AtomicU64,
    leaf_bytes: AtomicU64,
    leaf_fetch_ms: AtomicU64,
    sidecar_fetches: AtomicU64,
    sidecar_cache_hits: AtomicU64,
    sidecar_cache_misses: AtomicU64,
    sidecar_absent: AtomicU64,
    sidecar_bytes: AtomicU64,
    sidecar_fetch_ms: AtomicU64,
}

#[derive(Debug, Default, Clone)]
struct Phase2FetchStatsSnapshot {
    leaf_fetches: u64,
    leaf_cache_hits: u64,
    leaf_cache_misses: u64,
    leaf_bytes: u64,
    leaf_fetch_ms: u64,
    sidecar_fetches: u64,
    sidecar_cache_hits: u64,
    sidecar_cache_misses: u64,
    sidecar_absent: u64,
    sidecar_bytes: u64,
    sidecar_fetch_ms: u64,
}

impl Phase2FetchStats {
    fn snapshot(&self) -> Phase2FetchStatsSnapshot {
        Phase2FetchStatsSnapshot {
            leaf_fetches: self.leaf_fetches.load(Ordering::Relaxed),
            leaf_cache_hits: self.leaf_cache_hits.load(Ordering::Relaxed),
            leaf_cache_misses: self.leaf_cache_misses.load(Ordering::Relaxed),
            leaf_bytes: self.leaf_bytes.load(Ordering::Relaxed),
            leaf_fetch_ms: self.leaf_fetch_ms.load(Ordering::Relaxed),
            sidecar_fetches: self.sidecar_fetches.load(Ordering::Relaxed),
            sidecar_cache_hits: self.sidecar_cache_hits.load(Ordering::Relaxed),
            sidecar_cache_misses: self.sidecar_cache_misses.load(Ordering::Relaxed),
            sidecar_absent: self.sidecar_absent.load(Ordering::Relaxed),
            sidecar_bytes: self.sidecar_bytes.load(Ordering::Relaxed),
            sidecar_fetch_ms: self.sidecar_fetch_ms.load(Ordering::Relaxed),
        }
    }
}

/// Offer bytes this build just uploaded to the store's local tier, so the
/// first reader does not fetch them back ([`ContentStore::keep_local`]; a
/// store with no tier to keep them in ignores it).
fn cache_artifact_bytes(
    content_store: &dyn ContentStore,
    cid: &ContentId,
    bytes: &[u8],
    artifact_kind: &'static str,
) {
    content_store.keep_local(cid, bytes);
    tracing::trace!(
        %cid,
        artifact_kind,
        bytes = bytes.len(),
        "V6 incremental: offered artifact to the local tier"
    );
}

async fn fetch_cached_index_bytes(
    content_store: &dyn ContentStore,
    cid: &ContentId,
    context: impl Into<String>,
) -> Result<Vec<u8>> {
    let context = context.into();
    content_store
        .get(cid)
        .await
        .map(ContentBytes::into_vec)
        .map_err(|e| IndexerError::StorageRead(format!("{context}: {e}")))
}

/// `id`'s bytes, and whether the store already had them locally.
async fn fetch_noting_local(
    content_store: &dyn ContentStore,
    id: &ContentId,
) -> fluree_db_core::Result<(ContentBytes, bool)> {
    if let Some(bytes) = content_store.get_local(id)? {
        return Ok((bytes, true));
    }
    Ok((content_store.get(id).await?, false))
}

async fn upload_dict_blob_cached(
    content_store: &dyn ContentStore,
    dict: fluree_db_core::DictKind,
    bytes: &[u8],
    msg: &'static str,
) -> Result<ContentId> {
    let cid = super::upload::upload_dict_blob(content_store, dict, bytes, msg).await?;
    cache_artifact_bytes(content_store, &cid, bytes, "dict_blob");
    Ok(cid)
}

/// Run a branch update: async-prefetch the touched leaves/sidecars, then do
/// the CPU-bound copy-on-write update on a blocking thread while STREAMING each
/// produced leaf blob to an async uploader.
///
/// All CAS I/O happens here, on the async runtime — NOT inside the
/// `spawn_blocking` closure. The previous version fetched leaves on demand via
/// `handle.block_on(S3)` from inside the blocking closure; on a blocking-pool
/// thread that `block_on` has no tokio worker to drive its reactor, so under
/// slow remote storage (S3) + concurrent builds it can stall — a wedge that
/// never reproduces on a local filesystem (cache hits resolve instantly). We
/// prefetch the touched set (mirroring `build/dicts.rs`) so the blocking
/// closure only reads in-memory maps.
///
/// Streaming: the `spawn_blocking` CoW emits each finished [`NewLeafBlob`] over
/// a bounded channel (`blocking_send` for backpressure) to a concurrent async
/// uploader that puts each blob under the shared global `upload_budget` and
/// frees its bytes immediately. The CoW returns only [`BranchUpdateMeta`], so
/// the leaf/sidecar byte set is never resident as a whole — peak upload memory
/// is ~`upload_budget × leaf_size` plus the bounded channel.
#[allow(clippy::too_many_arguments)]
async fn run_update_branch(
    branch_bytes: Vec<u8>,
    sorted_records: Vec<RunRecordV2>,
    sorted_ops: Vec<u8>,
    sorted_superseded: Vec<RunRecordV2>,
    sorted_superseded_ops: Vec<u8>,
    branch_config: BranchUpdateConfig,
    content_store: Arc<dyn fluree_db_core::storage::ContentStore>,
    tracker: fluree_db_core::tracking::Tracker,
    upload_budget: Arc<Semaphore>,
    upload_buffer: usize,
    warm_cache: Option<Arc<LeafletCache>>,
) -> std::result::Result<(BranchUpdateMeta, Phase2FetchStatsSnapshot), IndexerError> {
    let parent_span = tracing::Span::current();
    let stats = Phase2FetchStats::default();
    let (order, g_id) = (branch_config.order, branch_config.g_id);

    // The leaves (+ sidecars) update_branch will request: exactly the leaves
    // with a non-empty novelty slice. Computed with the same slicing logic, so
    // the prefetched set matches its requests exactly.
    let touched = touched_leaf_refs(
        &branch_bytes,
        &sorted_records,
        &sorted_ops,
        branch_config.order,
    )
    .map_err(|e| IndexerError::StorageRead(format!("compute touched leaves: {e}")))?;

    // Prefetch (async). Dedup distinct CIDs, then fetch them concurrently under
    // the SAME shared global budget as the uploads (one permit per fetch, never
    // held across the stream), so prefetch reads + leaf uploads across all
    // order-tasks share one in-flight cap. Maps are keyed by CID, so fetch order
    // is irrelevant to the downstream CoW.
    let mut leaf_cids: Vec<ContentId> = Vec::new();
    let mut sidecar_cids: Vec<ContentId> = Vec::new();
    {
        let mut seen_leaf = std::collections::HashSet::new();
        let mut seen_sc = std::collections::HashSet::new();
        for (leaf_cid, sidecar_cid) in &touched {
            if seen_leaf.insert(leaf_cid.clone()) {
                leaf_cids.push(leaf_cid.clone());
            }
            if let Some(sc) = sidecar_cid {
                if seen_sc.insert(sc.clone()) {
                    sidecar_cids.push(sc.clone());
                }
            }
        }
    }
    let fetch_width = upload_budget.available_permits().max(1);

    // Required leaves: any fetch error aborts the fold (preserved).
    let leaf_pairs: Vec<(ContentId, ContentBytes)> = stream::iter(leaf_cids)
        .map(|cid| async {
            let _permit = upload_budget.acquire().await.expect("upload budget open");
            let started = Instant::now();
            let (bytes, cached_before) = fetch_noting_local(content_store.as_ref(), &cid)
                .await
                .map_err(|e| IndexerError::StorageRead(format!("prefetch leaf {cid}: {e}")))?;
            stats.leaf_fetches.fetch_add(1, Ordering::Relaxed);
            stats
                .leaf_fetch_ms
                .fetch_add(started.elapsed().as_millis() as u64, Ordering::Relaxed);
            stats
                .leaf_bytes
                .fetch_add(bytes.len() as u64, Ordering::Relaxed);
            if cached_before {
                stats.leaf_cache_hits.fetch_add(1, Ordering::Relaxed);
            } else {
                stats.leaf_cache_misses.fetch_add(1, Ordering::Relaxed);
            }
            Ok::<_, IndexerError>((cid, bytes))
        })
        .buffer_unordered(fetch_width)
        .try_collect()
        .await?;
    let leaf_map: HashMap<ContentId, ContentBytes> = leaf_pairs.into_iter().collect();

    // Sidecars: NotFound => absent (None); other errors abort (preserved).
    let sidecar_pairs: Vec<(ContentId, Option<ContentBytes>)> = stream::iter(sidecar_cids)
        .map(|cid| async {
            let _permit = upload_budget.acquire().await.expect("upload budget open");
            let started = Instant::now();
            match fetch_noting_local(content_store.as_ref(), &cid).await {
                Ok((bytes, cached_before)) => {
                    stats.sidecar_fetches.fetch_add(1, Ordering::Relaxed);
                    stats
                        .sidecar_fetch_ms
                        .fetch_add(started.elapsed().as_millis() as u64, Ordering::Relaxed);
                    stats
                        .sidecar_bytes
                        .fetch_add(bytes.len() as u64, Ordering::Relaxed);
                    if cached_before {
                        stats.sidecar_cache_hits.fetch_add(1, Ordering::Relaxed);
                    } else {
                        stats.sidecar_cache_misses.fetch_add(1, Ordering::Relaxed);
                    }
                    Ok::<_, IndexerError>((cid, Some(bytes)))
                }
                // A sidecar listed in the manifest may not exist in storage;
                // treat NotFound as "absent", same as the previous path.
                Err(fluree_db_core::Error::NotFound(_)) => {
                    stats.sidecar_absent.fetch_add(1, Ordering::Relaxed);
                    Ok((cid, None))
                }
                Err(e) => Err(IndexerError::StorageRead(format!(
                    "prefetch sidecar {cid}: {e}"
                ))),
            }
        })
        .buffer_unordered(fetch_width)
        .try_collect()
        .await?;
    let sidecar_map: HashMap<ContentId, ContentBytes> = sidecar_pairs
        .into_iter()
        .filter_map(|(cid, opt)| opt.map(|b| (cid, b)))
        .collect();

    // Bounded channel from the blocking CoW to the async uploader. The bound
    // gives backpressure: the CoW parks on `blocking_send` (it's already on a
    // blocking-pool thread, so parking there starves no tokio worker) when the
    // uploader is behind, capping resident blobs to the channel depth + the
    // in-flight puts. Depth tracks the upload budget so the CoW can run a few
    // leaves ahead without unbounded buffering.
    let (tx, mut rx) = tokio::sync::mpsc::channel::<NewLeafBlob>(upload_buffer.max(1));

    // CPU-bound CoW update on a blocking thread. The fetch closures read the
    // prefetched in-memory maps — no `block_on`, no S3 inside the closure. Each
    // produced blob is streamed out via `blocking_send`.
    let cow = tokio::task::spawn_blocking(move || {
        let _guard = parent_span.enter();
        update_branch_streaming(
            &branch_bytes,
            &sorted_records,
            &sorted_ops,
            &sorted_superseded,
            &sorted_superseded_ops,
            &branch_config,
            &|cid| {
                leaf_map
                    .get(cid)
                    .cloned()
                    .ok_or_else(|| std::io::Error::other(format!("leaf {cid} not prefetched")))
            },
            &|cid| Ok(sidecar_map.get(cid).cloned()),
            &mut |blob| {
                tx.blocking_send(blob)
                    .map_err(|_| std::io::Error::other("leaf upload channel closed"))
            },
        )
    });

    // Async uploader: drain the channel, uploading each blob under the global
    // budget and freeing its bytes. Runs concurrently with the CoW.
    let upload_budget_ref = upload_budget.as_ref();
    let uploader = {
        let content_store = content_store.as_ref();
        let tracker = &tracker;
        let warm_cache = warm_cache.as_ref();
        async move {
            let mut totals = LeafUploadCounts::default();
            while let Some(blob) = rx.recv().await {
                let c = upload_one_leaf_blob(
                    content_store,
                    tracker,
                    upload_budget_ref,
                    blob,
                    warm_cache,
                )
                .await?;
                totals.leaf_bytes += c.leaf_bytes;
                totals.sidecar_bytes += c.sidecar_bytes;
                totals.sidecar_count += c.sidecar_count;
            }
            Ok::<LeafUploadCounts, IndexerError>(totals)
        }
    };

    // Drive both to completion. If the uploader fails it drops `rx`, so the
    // CoW's next `blocking_send` errors out instead of hanging.
    let (cow_join, upload_res) = tokio::join!(cow, uploader);
    let upload_totals = upload_res?;
    let meta = cow_join
        .map_err(|e| IndexerError::from_join("branch update task", e))?
        .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;

    tracing::debug!(
        ?order,
        g_id,
        leaves = meta.new_leaf_count,
        leaf_bytes = upload_totals.leaf_bytes,
        sidecars = upload_totals.sidecar_count,
        sidecar_bytes = upload_totals.sidecar_bytes,
        "V6 Phase 2 streamed upload complete; offered artifacts to the local tier"
    );

    Ok((meta, stats.snapshot()))
}

enum Phase2TaskKind {
    DefaultExisting {
        leaves: Vec<LeafEntry>,
    },
    NamedExisting {
        branch_cid: ContentId,
    },
    /// A tree written whole from its graph's folded facts: an order the base
    /// root lacks (built from this window alone), or a pooled graph leaving
    /// the pool (its pooled log plus this window).
    Folded {
        facts: Arc<Vec<FoldedFact>>,
    },
}

/// The tree a Phase 2 task writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TreeTarget {
    /// A graph's own tree; graph 0 is the default graph, inline in the root.
    Graph(fluree_db_core::GraphId),
    /// The pooled tree shared by small named graphs.
    Pool,
}

impl TreeTarget {
    /// The graph id this tree's branch manifests record.
    fn branch_g_id(self) -> fluree_db_core::GraphId {
        match self {
            Self::Graph(g_id) => g_id,
            Self::Pool => 0,
        }
    }
}

struct Phase2Task {
    seq: usize,
    target: TreeTarget,
    /// Pooled tree only: graphs leaving the pool this window, sorted. The
    /// records carry an [`OP_PURGE`] marker per identity they hold.
    purge: Arc<Vec<fluree_db_core::GraphId>>,
    order: RunSortOrder,
    sorted_records: Vec<RunRecordV2>,
    sorted_ops: Vec<u8>,
    /// Superseded events for this (graph, order), sorted like
    /// `sorted_records`. Their real-transition subset becomes history-sidecar
    /// entries in the leaf merge, or in a folded tree.
    sorted_superseded: Vec<RunRecordV2>,
    sorted_superseded_ops: Vec<u8>,
    kind: Phase2TaskKind,
}

/// Graph-scoped V2 fact identity: `(g_id, s_id, p_id, o_type, o_key, o_i)`.
type FactIdentityKey = (fluree_db_core::GraphId, u64, u32, u16, u64, u32);

fn fact_identity_key(rec: &RunRecordV2) -> FactIdentityKey {
    (
        rec.g_id,
        rec.s_id.as_u64(),
        rec.p_id,
        rec.o_type,
        rec.o_key,
        rec.o_i,
    )
}

enum Phase2TaskUpdate {
    Default {
        leaf_entries: Vec<LeafEntry>,
    },
    Branch {
        branch_cid: ContentId,
    },
    /// A graph's folded tree with nothing to hold: the root keeps what it had
    /// for this order (no tree, or one with no leaves), which the next build
    /// again finds no base facts in.
    Unchanged,
}

struct Phase2TaskOutput {
    seq: usize,
    target: TreeTarget,
    order: RunSortOrder,
    /// The task merged into a base tree with leaves, rather than writing a
    /// folded tree. A graph's four tasks must agree
    /// ([`check_base_facts_agree`]).
    base_facts: bool,
    update: Phase2TaskUpdate,
    replaced_leaf_cids: Vec<ContentId>,
    replaced_sidecar_cids: Vec<ContentId>,
    new_leaf_count: usize,
    fetch_stats: Phase2FetchStatsSnapshot,
    /// Novelty ops whose identity matched an existing base row (SPOT task
    /// only; fresh-graph tasks have no base rows to match).
    matched: Vec<RunRecordV2>,
    /// Pooled GSPO task only: each graph's change in rows and history.
    graph_deltas: GraphDeltas,
}

/// Write and upload a tree for `order` holding `facts`.
#[allow(clippy::too_many_arguments)]
async fn write_folded_tree(
    facts: &[FoldedFact],
    seq: usize,
    target: TreeTarget,
    order: RunSortOrder,
    config: &IndexerConfig,
    content_store: &Arc<dyn fluree_db_core::storage::ContentStore>,
    tracker: &fluree_db_core::tracking::Tracker,
    upload_budget: &Semaphore,
    upload_buffer: usize,
) -> Result<Phase2TaskOutput> {
    let BranchUpdateResult {
        leaf_entries,
        new_leaf_blobs,
        replaced_leaf_cids,
        replaced_sidecar_cids,
        branch_bytes,
        branch_cid: expected_branch_cid,
        graph_deltas,
        ..
    } = build_folded_tree(facts, order, target.branch_g_id(), config)?;
    let new_leaf_count = new_leaf_blobs.len();
    // A graph whose facts net to no row and no history gets no tree: one with
    // no leaves would route no later novelty.
    if leaf_entries.is_empty() && target != TreeTarget::Pool {
        return Ok(Phase2TaskOutput {
            seq,
            target,
            order,
            base_facts: false,
            update: Phase2TaskUpdate::Unchanged,
            replaced_leaf_cids,
            replaced_sidecar_cids,
            new_leaf_count,
            fetch_stats: Phase2FetchStatsSnapshot::default(),
            matched: Vec::new(),
            graph_deltas,
        });
    }
    upload_leaf_blobs(
        content_store.as_ref(),
        tracker,
        upload_budget,
        upload_buffer,
        new_leaf_blobs,
    )
    .await?;
    let update = if target == TreeTarget::Graph(0) {
        Phase2TaskUpdate::Default { leaf_entries }
    } else {
        let branch_cid = content_store
            .put(ContentKind::IndexBranch, &branch_bytes)
            .await
            .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
        debug_assert!(branch_cid == expected_branch_cid);
        cache_artifact_bytes(&**content_store, &branch_cid, &branch_bytes, "index_branch");
        Phase2TaskUpdate::Branch { branch_cid }
    };
    Ok(Phase2TaskOutput {
        seq,
        target,
        order,
        base_facts: false,
        update,
        replaced_leaf_cids,
        replaced_sidecar_cids,
        new_leaf_count,
        fetch_stats: Phase2FetchStatsSnapshot::default(),
        matched: Vec::new(),
        graph_deltas,
    })
}

#[allow(clippy::too_many_arguments)]
async fn execute_phase2_task(
    task: Phase2Task,
    config: &IndexerConfig,
    content_store: Arc<dyn fluree_db_core::storage::ContentStore>,
    tracker: fluree_db_core::tracking::Tracker,
    upload_budget: Arc<Semaphore>,
    upload_buffer: usize,
) -> Result<Phase2TaskOutput> {
    let Phase2Task {
        seq,
        target,
        purge,
        order,
        sorted_records,
        sorted_ops,
        sorted_superseded,
        sorted_superseded_ops,
        kind,
    } = task;
    let g_id = target.branch_g_id();

    let branch_config = BranchUpdateConfig {
        pool: PoolUpdate {
            purge: purge.as_ref().clone(),
            // Like matches, one order's tally covers every order.
            tally: order == RunSortOrder::Gspo,
        },
        order,
        g_id,
        zstd_level: 1,
        leaflet_target_rows: config.leaflet_rows.max(1),
        leaf_target_rows: leaf_rows_for(order, config),
        // Fact identity is order-independent; collect base-row matches from
        // one task per tree — SPOT, or the pool's GSPO (they feed the Phase
        // 3b stats deltas).
        collect_matched: matches!(order, RunSortOrder::Spot | RunSortOrder::Gspo),
    };

    let started = Instant::now();
    // Resolve the warm-on-write cache once per task (co-located only; `None`
    // elsewhere). The CoW (update) paths seed it; fresh builds do not (they'd
    // decode the whole graph).
    let warm_cache = config
        .warm_cache_source
        .as_ref()
        .and_then(|s| s.warm_cache());
    // An order with no leaves has nothing to merge into (slicing routes
    // novelty only to existing leaves): write it from this window instead.
    let fold_own_window = || {
        let window = sorted_records
            .iter()
            .copied()
            .zip(sorted_ops.iter().copied())
            .filter(|&(_, op)| op != OP_PURGE)
            .chain(
                sorted_superseded
                    .iter()
                    .copied()
                    .zip(sorted_superseded_ops.iter().copied()),
            );
        fold_graph_log(&PooledGraphLog::default(), window).0
    };
    let output = match kind {
        Phase2TaskKind::DefaultExisting { leaves } if leaves.is_empty() => {
            write_folded_tree(
                &fold_own_window(),
                seq,
                target,
                order,
                config,
                &content_store,
                &tracker,
                &upload_budget,
                upload_buffer,
            )
            .await?
        }
        Phase2TaskKind::DefaultExisting { leaves } => {
            let branch_bytes =
                fluree_db_binary_index::format::branch::build_branch_bytes(order, g_id, &leaves);
            let (meta, fetch_stats) = run_update_branch(
                branch_bytes,
                sorted_records,
                sorted_ops,
                sorted_superseded,
                sorted_superseded_ops,
                branch_config,
                Arc::clone(&content_store),
                tracker.clone(),
                Arc::clone(&upload_budget),
                upload_buffer,
                warm_cache.clone(),
            )
            .await?;
            Phase2TaskOutput {
                seq,
                target,
                order,
                base_facts: true,
                update: Phase2TaskUpdate::Default {
                    leaf_entries: meta.leaf_entries,
                },
                replaced_leaf_cids: meta.replaced_leaf_cids,
                replaced_sidecar_cids: meta.replaced_sidecar_cids,
                new_leaf_count: meta.new_leaf_count,
                fetch_stats,
                matched: meta.matched,
                graph_deltas: meta.graph_deltas,
            }
        }
        Phase2TaskKind::NamedExisting { branch_cid } => {
            let branch_bytes = fetch_cached_index_bytes(
                content_store.as_ref(),
                &branch_cid,
                format!("fetch V3 branch g_id={g_id} {order:?}"),
            )
            .await?;
            let branch_is_empty =
                fluree_db_binary_index::format::branch::read_branch_from_bytes(&branch_bytes)
                    .map_err(|e| IndexerError::StorageRead(format!("decode V3 branch: {e}")))?
                    .leaves
                    .is_empty();
            if branch_is_empty {
                return write_folded_tree(
                    &fold_own_window(),
                    seq,
                    target,
                    order,
                    config,
                    &content_store,
                    &tracker,
                    &upload_budget,
                    upload_buffer,
                )
                .await;
            }
            let (meta, fetch_stats) = run_update_branch(
                branch_bytes,
                sorted_records,
                sorted_ops,
                sorted_superseded,
                sorted_superseded_ops,
                branch_config,
                Arc::clone(&content_store),
                tracker.clone(),
                Arc::clone(&upload_budget),
                upload_buffer,
                warm_cache.clone(),
            )
            .await?;
            let branch_cid = content_store
                .put(ContentKind::IndexBranch, &meta.branch_bytes)
                .await
                .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
            debug_assert!(branch_cid == meta.branch_cid);
            cache_artifact_bytes(
                &*content_store,
                &branch_cid,
                &meta.branch_bytes,
                "index_branch",
            );
            Phase2TaskOutput {
                seq,
                target,
                order,
                base_facts: true,
                update: Phase2TaskUpdate::Branch { branch_cid },
                replaced_leaf_cids: meta.replaced_leaf_cids,
                replaced_sidecar_cids: meta.replaced_sidecar_cids,
                new_leaf_count: meta.new_leaf_count,
                fetch_stats,
                matched: meta.matched,
                graph_deltas: meta.graph_deltas,
            }
        }
        Phase2TaskKind::Folded { facts } => {
            write_folded_tree(
                &facts,
                seq,
                target,
                order,
                config,
                &content_store,
                &tracker,
                &upload_budget,
                upload_buffer,
            )
            .await?
        }
    };

    tracing::debug!(
        seq,
        ?target,
        ?order,
        new_leaf_count = output.new_leaf_count,
        leaf_fetches = output.fetch_stats.leaf_fetches,
        leaf_cache_hits = output.fetch_stats.leaf_cache_hits,
        leaf_cache_misses = output.fetch_stats.leaf_cache_misses,
        leaf_bytes = output.fetch_stats.leaf_bytes,
        leaf_fetch_ms = output.fetch_stats.leaf_fetch_ms,
        sidecar_fetches = output.fetch_stats.sidecar_fetches,
        sidecar_cache_hits = output.fetch_stats.sidecar_cache_hits,
        sidecar_cache_misses = output.fetch_stats.sidecar_cache_misses,
        sidecar_absent = output.fetch_stats.sidecar_absent,
        sidecar_bytes = output.fetch_stats.sidecar_bytes,
        sidecar_fetch_ms = output.fetch_stats.sidecar_fetch_ms,
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
    tracker: fluree_db_core::tracking::Tracker,
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

    // Single global S3 budget shared across the whole fold: the reconcile
    // leaf prefetch (Phase 1), and the Phase 2 leaf prefetch + leaf/sidecar
    // uploads all acquire from this one semaphore, so total in-flight S3 reads
    // and writes are capped here regardless of phase.
    let upload_buffer = config.incremental_leaf_upload_concurrency.max(1);
    let upload_budget = Arc::new(Semaphore::new(upload_buffer));

    // ---- Phase 1: Resolve incremental commits ----
    let resolve_config = IncrementalResolveConfig {
        base_root_id: base_root_id.clone(),
        head_commit_id,
        from_t,
        max_commit_bytes: config.incremental_max_commit_bytes,
        fulltext_configured_properties: config.fulltext_configured_properties.clone(),
        pending_commit_cids: config.pending_commit_cids.clone(),
        commit_fetch_concurrency: config.incremental_max_concurrency.max(1),
    };
    let mut novelty = resolve_incremental_commits_v6(
        content_store.clone(),
        resolve_config,
        Arc::clone(&upload_budget),
    )
    .await
    .map_err(|e| match e {
        IncrementalResolveError::Resolve(crate::run_index::ResolverError::Unindexable(msg)) => {
            IndexerError::Unindexable(msg)
        }
        e => IndexerError::StorageWrite(format!("V6 incremental resolve: {e}")),
    })?;

    if novelty.records.is_empty() {
        tracing::debug!("no new records resolved; returning existing V6 root");
        // fuel is filled in by the outer entry point from the tracker tally.
        return Ok(IndexResult {
            root_id: base_root_id,
            index_t: novelty.max_t,
            ledger_id: fluree_db_core::IntoLedgerId::into_ledger_id(ledger_id),
            stats: IndexStats::default(),
            fuel: None,
            fence: None,
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
    check_order_sets(
        &novelty.base_root.default_graph_orders,
        &novelty.base_root.named_graphs,
    )?;
    let all_orders = [
        RunSortOrder::Spot,
        RunSortOrder::Psot,
        RunSortOrder::Post,
        RunSortOrder::Opst,
    ];

    // Group records + ops by g_id.
    let mut by_graph: std::collections::BTreeMap<fluree_db_core::GraphId, Vec<usize>> =
        std::collections::BTreeMap::new();
    for (idx, rec) in novelty.records.iter().enumerate() {
        by_graph.entry(rec.g_id).or_default().push(idx);
    }
    let mut superseded_by_graph: std::collections::BTreeMap<fluree_db_core::GraphId, Vec<usize>> =
        std::collections::BTreeMap::new();
    for (idx, rec) in novelty.superseded.iter().enumerate() {
        superseded_by_graph.entry(rec.g_id).or_default().push(idx);
    }

    let base_root = &novelty.base_root;
    let mut root_builder =
        IncrementalRootBuilder::from_old_root(novelty.base_root.clone(), ledger_id);
    root_builder.set_index_t(novelty.max_t);
    root_builder.add_commit_stats(
        novelty.delta_commit_size,
        novelty.delta_asserts,
        novelty.delta_retracts,
    );

    // Placement. A pooled graph this window writes stays pooled while the
    // policy keeps it there, judged on its recorded size grown by this
    // window; otherwise it is promoted — its pooled rows and history plus
    // this window's events become trees of its own, and the pool drops
    // them. A graph new to the index joins the pool on the same terms. A
    // graph with trees of its own keeps them; only a full rebuild repools.
    // A pooled graph the config now pins dedicated leaves whether or not
    // this window writes it.
    let placement = PlacementPolicy::new(&config.graph_placement, |iri| {
        novelty.shared.graphs.get(iri).map(|id| id + 1)
    });
    let base_pool = base_root.pool.as_ref();
    let has_own_trees: std::collections::HashSet<fluree_db_core::GraphId> =
        base_root.named_graphs.iter().map(|ng| ng.g_id).collect();
    let mut pool_writes: Vec<fluree_db_core::GraphId> = Vec::new();
    let mut promote: Vec<fluree_db_core::GraphId> = Vec::new();
    for (&g_id, indices) in &by_graph {
        if g_id < fluree_db_core::graph_registry::FIRST_USER_GRAPH_ID
            || has_own_trees.contains(&g_id)
        {
            continue;
        }
        let pooled = base_pool.and_then(|p| p.member(g_id));
        let size = window_size(
            pooled.map_or(GraphSize::default(), |m| GraphSize {
                rows: m.rows.into(),
                history: m.history.into(),
            }),
            indices.iter().map(|&i| novelty.ops[i]),
            superseded_by_graph.get(&g_id).map_or(0, Vec::len),
        );
        if placement.pools(g_id, size) {
            pool_writes.push(g_id);
        } else if pooled.is_some() {
            promote.push(g_id);
        }
    }
    if let Some(pool) = base_pool {
        promote.extend(
            pool.graph_ids()
                .filter(|g| !by_graph.contains_key(g) && placement.pinned_dedicated(*g)),
        );
    }
    promote.sort_unstable();
    let promoted = read_pooled_graph_logs(&content_store, base_root, &promote).await?;

    let concurrency = config.incremental_max_concurrency.max(1);
    // Graphs written to trees of their own: every graph with novelty outside
    // the pool, and every promoted graph.
    let mut graph_order: Vec<fluree_db_core::GraphId> = by_graph
        .keys()
        .copied()
        .filter(|g| pool_writes.binary_search(g).is_err())
        .chain(promote.iter().copied())
        .collect();
    graph_order.sort_unstable_by_key(|&g_id| (u8::from(g_id == 0), g_id));
    graph_order.dedup();

    let no_purge: Arc<Vec<fluree_db_core::GraphId>> = Arc::new(Vec::new());
    // Pooled secondary orders taking this window as a delta run, and the
    // window's pooled events.
    let mut delta_orders: Vec<RunSortOrder> = Vec::new();
    let mut delta_window: Vec<(RunRecordV2, u8)> = Vec::new();
    let mut phase2_tasks = Vec::with_capacity(
        graph_order.len().saturating_mul(all_orders.len())
            + RunSortOrder::pool_build_orders().len(),
    );
    let mut promoted_matched: Vec<RunRecordV2> = Vec::new();
    for g_id in graph_order.iter().copied() {
        let indices: &[usize] = by_graph.get(&g_id).map_or(&[], Vec::as_slice);
        let graph_records: Vec<RunRecordV2> = indices.iter().map(|&i| novelty.records[i]).collect();
        let graph_ops: Vec<u8> = indices.iter().map(|&i| novelty.ops[i]).collect();
        let superseded_indices = superseded_by_graph.get(&g_id);
        let graph_superseded: Vec<RunRecordV2> = superseded_indices
            .map(|idxs| idxs.iter().map(|&i| novelty.superseded[i]).collect())
            .unwrap_or_default();
        let graph_superseded_ops: Vec<u8> = superseded_indices
            .map(|idxs| idxs.iter().map(|&i| novelty.superseded_ops[i]).collect())
            .unwrap_or_default();
        let window = || {
            graph_records
                .iter()
                .copied()
                .zip(graph_ops.iter().copied())
                .chain(
                    graph_superseded
                        .iter()
                        .copied()
                        .zip(graph_superseded_ops.iter().copied()),
                )
        };
        let promoted_facts = promoted.get(&g_id).map(|log| {
            let (facts, matched) = fold_graph_log(log, window());
            promoted_matched.extend(matched);
            root_builder.remove_from_pool(g_id);
            Arc::new(facts)
        });
        // An order the base root lacks is written from this window alone.
        let mut window_facts: Option<Arc<Vec<FoldedFact>>> = None;
        let mut fold_window = || {
            Arc::clone(window_facts.get_or_insert_with(|| {
                Arc::new(fold_graph_log(&PooledGraphLog::default(), window()).0)
            }))
        };

        for &order in &all_orders {
            let cmp = cmp_v2_for_order(order);
            let mut sorted_indices: Vec<usize> = (0..graph_records.len()).collect();
            sorted_indices.sort_unstable_by(|&a, &b| cmp(&graph_records[a], &graph_records[b]));
            let sorted_records: Vec<RunRecordV2> =
                sorted_indices.iter().map(|&i| graph_records[i]).collect();
            let sorted_ops: Vec<u8> = sorted_indices.iter().map(|&i| graph_ops[i]).collect();
            let mut sorted_superseded_indices: Vec<usize> = (0..graph_superseded.len()).collect();
            sorted_superseded_indices
                .sort_unstable_by(|&a, &b| cmp(&graph_superseded[a], &graph_superseded[b]));
            let sorted_superseded: Vec<RunRecordV2> = sorted_superseded_indices
                .iter()
                .map(|&i| graph_superseded[i])
                .collect();
            let sorted_superseded_ops: Vec<u8> = sorted_superseded_indices
                .iter()
                .map(|&i| graph_superseded_ops[i])
                .collect();

            let kind = if let Some(facts) = &promoted_facts {
                Phase2TaskKind::Folded {
                    facts: Arc::clone(facts),
                }
            } else if g_id == 0 {
                match base_root
                    .default_graph_orders
                    .iter()
                    .find(|o| o.order == order)
                    .map(|o| o.leaves.clone())
                {
                    Some(leaves) => Phase2TaskKind::DefaultExisting { leaves },
                    None => Phase2TaskKind::Folded {
                        facts: fold_window(),
                    },
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
                    None => Phase2TaskKind::Folded {
                        facts: fold_window(),
                    },
                }
            };

            phase2_tasks.push(Phase2Task {
                seq: phase2_tasks.len(),
                target: TreeTarget::Graph(g_id),
                purge: Arc::clone(&no_purge),
                order,
                sorted_records,
                sorted_ops,
                sorted_superseded,
                sorted_superseded_ops,
                kind,
            });
        }
    }

    // The pool: one update per pooled order for every pooled graph this
    // window writes, which also drops the promoted graphs. Each promoted
    // identity rides along as an `OP_PURGE` marker so the update touches
    // every leaflet that holds it.
    if !pool_writes.is_empty() || !promote.is_empty() {
        let pooled_write = |rec: &RunRecordV2| pool_writes.binary_search(&rec.g_id).is_ok();
        let mut records: Vec<(RunRecordV2, u8)> = novelty
            .records
            .iter()
            .zip(&novelty.ops)
            .filter(|(rec, _)| pooled_write(rec))
            .map(|(rec, &op)| (*rec, op))
            .collect();
        let superseded: Vec<(RunRecordV2, u8)> = novelty
            .superseded
            .iter()
            .zip(&novelty.superseded_ops)
            .filter(|(rec, _)| pooled_write(rec))
            .map(|(rec, &op)| (*rec, op))
            .collect();
        for log in promoted.values() {
            let mut seen: std::collections::HashSet<FactIdentityKey> =
                std::collections::HashSet::new();
            for rec in log
                .rows
                .iter()
                .chain(log.history.iter().map(|(rec, _)| rec))
            {
                if seen.insert(fact_identity_key(rec)) {
                    records.push((*rec, OP_PURGE));
                }
            }
        }
        let purge = Arc::new(promote.clone());
        // No pool yet: write one from this window's pooled graphs alone.
        let fresh_pool = base_pool.is_none().then(|| {
            Arc::new(
                fold_graph_log(
                    &PooledGraphLog::default(),
                    records.iter().chain(&superseded).copied(),
                )
                .0,
            )
        });
        let base_rows: u64 =
            base_pool.map_or(0, |p| p.members.iter().map(|m| u64::from(m.rows)).sum());
        delta_window = records.iter().chain(&superseded).copied().collect();
        // A secondary order appends the window as a delta run, unless its runs
        // have outgrown their bounds or a graph is leaving the pool: then the
        // runs and the window merge into its base at once. The runs of every
        // order that merges are read together.
        let window_events = records.len() + superseded.len();
        let mut folding = match base_pool {
            Some(pool) => {
                let folds: Vec<_> = RunSortOrder::pool_build_orders()
                    .iter()
                    .filter(|&&order| {
                        order != RunSortOrder::Gspo
                            && compact_delta(
                                pool.delta(order),
                                window_events,
                                base_rows,
                                !promote.is_empty(),
                            )
                    })
                    .map(|&order| (order, pool.delta(order)))
                    .collect();
                delta_run_events(content_store.as_ref(), &folds, &promote).await?
            }
            None => Vec::new(),
        };
        for &order in RunSortOrder::pool_build_orders() {
            let cmp = cmp_v2_for_order(order);
            let (mut sorted, mut sorted_superseded) = (records.clone(), superseded.clone());
            let kind = match (base_pool, &fresh_pool) {
                (Some(pool), _) => {
                    let branch_cid = pool
                        .orders
                        .iter()
                        .find(|(o, _)| *o == order)
                        .map(|(_, cid)| cid.clone())
                        .ok_or_else(|| {
                            IndexerError::IncrementalAbort(format!(
                                "pooled tree has no {order:?} branch"
                            ))
                        })?;
                    // GSPO merges every window.
                    let fold = folding.iter_mut().find(|(o, _)| *o == order);
                    if order == RunSortOrder::Gspo {
                        Phase2TaskKind::NamedExisting { branch_cid }
                    } else if let Some((_, runs_events)) = fold {
                        let mut events = std::mem::take(runs_events);
                        let purge_markers = records.iter().filter(|&&(_, op)| op == OP_PURGE);
                        events.extend(with_replacements(
                            records
                                .iter()
                                .chain(&superseded)
                                .filter(|&&(_, op)| op != OP_PURGE)
                                .copied()
                                .collect(),
                        ));
                        let (winners, earlier) = split_lifecycles(events, order);
                        sorted = winners;
                        sorted.extend(purge_markers);
                        sorted_superseded = earlier;
                        root_builder.clear_pool_delta(order);
                        Phase2TaskKind::NamedExisting { branch_cid }
                    } else {
                        // Written once the GSPO merge has resolved the window.
                        delta_orders.push(order);
                        continue;
                    }
                }
                (None, Some(facts)) => Phase2TaskKind::Folded {
                    facts: Arc::clone(facts),
                },
                (None, None) => unreachable!("a missing pool is folded"),
            };
            sorted.sort_unstable_by(|a, b| cmp(&a.0, &b.0));
            sorted_superseded.sort_unstable_by(|a, b| cmp(&a.0, &b.0));
            phase2_tasks.push(Phase2Task {
                seq: phase2_tasks.len(),
                target: TreeTarget::Pool,
                purge: Arc::clone(&purge),
                order,
                sorted_records: sorted.iter().map(|&(rec, _)| rec).collect(),
                sorted_ops: sorted.iter().map(|&(_, op)| op).collect(),
                sorted_superseded: sorted_superseded.iter().map(|&(rec, _)| rec).collect(),
                sorted_superseded_ops: sorted_superseded.iter().map(|&(_, op)| op).collect(),
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
            let task_tracker = tracker.clone();
            let task_budget = Arc::clone(&upload_budget);
            async move {
                execute_phase2_task(
                    task,
                    config_ref,
                    content_store,
                    task_tracker,
                    task_budget,
                    upload_buffer,
                )
                .await
            }
        })
        .buffer_unordered(concurrency)
        .collect::<Vec<_>>()
        .await;

    let mut total_new_leaves = 0usize;
    let mut phase2_outputs: Vec<Phase2TaskOutput> =
        phase2_results.into_iter().collect::<Result<Vec<_>>>()?;
    phase2_outputs.sort_unstable_by_key(|output| output.seq);
    check_base_facts_agree(&phase2_outputs)?;

    // Fact identities the SPOT merges matched against existing base rows.
    // Phase 3b derives materialized-state stat deltas from this: an assert of
    // a base-present fact and a retract of a base-absent fact change no rows.
    let mut base_matched: std::collections::HashSet<FactIdentityKey> =
        std::collections::HashSet::new();

    base_matched.extend(promoted_matched.iter().map(fact_identity_key));

    let mut phase2_fetch_totals = Phase2FetchStatsSnapshot::default();
    let mut pool_deltas = GraphDeltas::new();
    for output in phase2_outputs {
        total_new_leaves += output.new_leaf_count;
        base_matched.extend(output.matched.iter().map(fact_identity_key));
        phase2_fetch_totals.leaf_fetches += output.fetch_stats.leaf_fetches;
        phase2_fetch_totals.leaf_cache_hits += output.fetch_stats.leaf_cache_hits;
        phase2_fetch_totals.leaf_cache_misses += output.fetch_stats.leaf_cache_misses;
        phase2_fetch_totals.leaf_bytes += output.fetch_stats.leaf_bytes;
        phase2_fetch_totals.leaf_fetch_ms += output.fetch_stats.leaf_fetch_ms;
        phase2_fetch_totals.sidecar_fetches += output.fetch_stats.sidecar_fetches;
        phase2_fetch_totals.sidecar_cache_hits += output.fetch_stats.sidecar_cache_hits;
        phase2_fetch_totals.sidecar_cache_misses += output.fetch_stats.sidecar_cache_misses;
        phase2_fetch_totals.sidecar_absent += output.fetch_stats.sidecar_absent;
        phase2_fetch_totals.sidecar_bytes += output.fetch_stats.sidecar_bytes;
        phase2_fetch_totals.sidecar_fetch_ms += output.fetch_stats.sidecar_fetch_ms;
        match (output.update, output.target) {
            (Phase2TaskUpdate::Default { leaf_entries }, _) => {
                root_builder.set_default_graph_order(output.order, leaf_entries);
            }
            (Phase2TaskUpdate::Branch { branch_cid }, TreeTarget::Graph(g_id)) => {
                root_builder.set_named_graph_branch(g_id, output.order, branch_cid);
            }
            (Phase2TaskUpdate::Branch { branch_cid }, TreeTarget::Pool) => {
                root_builder.set_pool_branch(output.order, branch_cid);
            }
            (Phase2TaskUpdate::Unchanged, _) => {}
        }
        for (g_id, delta) in output.graph_deltas {
            let total = pool_deltas.entry(g_id).or_default();
            total.rows += delta.rows;
            total.history += delta.history;
        }
        root_builder.add_replaced_cids(output.replaced_leaf_cids);
        root_builder.add_replaced_cids(output.replaced_sidecar_cids);
    }
    root_builder.update_pool_members(&pool_writes, &pool_deltas);

    tracing::debug!(
        new_leaves = total_new_leaves,
        graphs = by_graph.len(),
        concurrency,
        leaf_fetches = phase2_fetch_totals.leaf_fetches,
        leaf_cache_hits = phase2_fetch_totals.leaf_cache_hits,
        leaf_cache_misses = phase2_fetch_totals.leaf_cache_misses,
        leaf_bytes = phase2_fetch_totals.leaf_bytes,
        leaf_fetch_ms = phase2_fetch_totals.leaf_fetch_ms,
        sidecar_fetches = phase2_fetch_totals.sidecar_fetches,
        sidecar_cache_hits = phase2_fetch_totals.sidecar_cache_hits,
        sidecar_cache_misses = phase2_fetch_totals.sidecar_cache_misses,
        sidecar_absent = phase2_fetch_totals.sidecar_absent,
        sidecar_bytes = phase2_fetch_totals.sidecar_bytes,
        sidecar_fetch_ms = phase2_fetch_totals.sidecar_fetch_ms,
        "V6 Phase 2 complete: branch updates"
    );

    // The delta runs hold the window as the GSPO merge resolved it, from
    // each fact's presence at the window's start, so the secondary orders
    // read every `t` as GSPO does.
    if !delta_orders.is_empty() {
        let resolved = resolved_window_events(&delta_window, |rec| {
            base_matched.contains(&fact_identity_key(rec))
        });
        for &order in delta_orders.iter().filter(|_| !resolved.is_empty()) {
            let run = write_delta_run(content_store.as_ref(), order, &resolved).await?;
            root_builder.append_pool_delta_run(order, run);
        }
    }

    // ---- Phase 3: Dict updates ----
    // Incremental dict tree updates.
    use super::dicts::{
        upload_incremental_reverse_tree_async, upload_incremental_reverse_tree_async_strings,
    };
    let mut new_dict_refs = base_root.dict_refs.clone();
    // Resolve the warm-on-write cache once (co-located only); seeds the
    // reverse-dict leaves we rewrite so a reader resolving a just-added
    // IRI/string hits the cache instead of a cold read.
    let warm_cache = config
        .warm_cache_source
        .as_ref()
        .and_then(|s| s.warm_cache());

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
            warm_cache.as_deref(),
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
            warm_cache.as_deref(),
        )
        .await?;
        root_builder.add_replaced_cids(updated.replaced_cids);
        new_dict_refs.string_reverse = updated.tree_refs;
    }

    // One compaction budget per cycle, shared by every forward-pack stream.
    // The term dictionary's streams are served last, so they get a reserved
    // share (one of three dictionaries) plus whatever the others leave.
    let mut compaction_budget = if compaction_enabled() {
        CompactionBudget::new()
    } else {
        CompactionBudget::disabled()
    };
    let mut term_budget = if novelty.base_root.term_dict.is_some() || !novelty.new_terms.is_empty()
    {
        compaction_budget.split_off(3)
    } else {
        CompactionBudget::disabled()
    };
    let mut pack_sizes = PackSizeCache::new();

    // Forward pack updates (FPK1): append new pack artifacts for new subjects/strings,
    // then compact the tail of each stream that was touched. Appending alone adds a
    // routing entry per build forever; compaction is what bounds the table by data size.
    {
        use fluree_db_binary_index::dict::incremental::{
            build_incremental_string_packs, build_incremental_subject_packs_for_ns,
        };
        use fluree_db_binary_index::PackBranchEntry;

        // Every pack CID this cycle uploads, so any that compaction consumes
        // before publication can be garbaged explicitly.
        let mut uploaded_pack_cids: Vec<ContentId> = Vec::new();
        // Streams compacted alongside their own novelty; the rest are swept
        // afterwards with whatever budget survives.
        let mut compacted_streams: std::collections::HashSet<Option<u16>> =
            std::collections::HashSet::new();

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
                pack_sizes.insert(pack_cid.clone(), pack.bytes.len() as u64);
                uploaded_pack_cids.push(pack_cid.clone());
                updated_refs.push(PackBranchEntry {
                    first_id: pack.first_id,
                    last_id: pack.last_id,
                    pack_cid,
                });
            }

            uploaded_pack_cids.extend(
                compact_forward_packs(
                    content_store.as_ref(),
                    kind,
                    &mut updated_refs,
                    &mut pack_sizes,
                    &mut compaction_budget,
                    CompactionSpans::default(),
                    "string",
                )
                .await?,
            );
            compacted_streams.insert(None);

            let total_packs = updated_refs.len();
            new_dict_refs.forward_packs.string_fwd_packs = updated_refs;

            tracing::debug!(
                new_packs = pack_result.new_packs.len(),
                new_strings = novelty.new_strings.len(),
                total_packs,
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
                    pack_sizes.insert(pack_cid.clone(), pack.bytes.len() as u64);
                    uploaded_pack_cids.push(pack_cid.clone());
                    updated_refs.push(PackBranchEntry {
                        first_id: pack.first_id,
                        last_id: pack.last_id,
                        pack_cid,
                    });
                }

                uploaded_pack_cids.extend(
                    compact_forward_packs(
                        content_store.as_ref(),
                        kind,
                        &mut updated_refs,
                        &mut pack_sizes,
                        &mut compaction_budget,
                        CompactionSpans::default(),
                        &format!("subject ns={ns_code}"),
                    )
                    .await?,
                );
                compacted_streams.insert(Some(*ns_code));

                let total_packs = updated_refs.len();

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
                    total_packs,
                    "V6 Phase 3: subject forward packs updated"
                );
            }
        }

        // Maintenance: a stream that received no novelty this cycle still
        // carries whatever fragmentation it accumulated, and its packs are
        // still loaded and mapped with the index. A namespace that goes quiet
        // would otherwise keep its tiny packs forever. Sweep those with the
        // leftover budget.
        //
        // Coverage rotates with `index_t` so that a ledger with more streams
        // than one cycle's budget can service still reaches all of them over
        // time, without persisting a cursor anywhere.
        {
            let mut pending: Vec<Option<u16>> = Vec::new();
            if !compacted_streams.contains(&None) {
                pending.push(None);
            }
            pending.extend(
                new_dict_refs
                    .forward_packs
                    .subject_fwd_ns_packs
                    .iter()
                    .map(|(ns, _)| Some(*ns))
                    .filter(|key| !compacted_streams.contains(key)),
            );

            if !pending.is_empty() {
                let offset = (base_root.index_t.unsigned_abs() as usize) % pending.len();
                pending.rotate_left(offset);
            }

            for key in pending {
                if compaction_budget.exhausted() {
                    break;
                }
                let (kind, label, refs) = match key {
                    None => (
                        ContentKind::DictBlob {
                            dict: fluree_db_core::DictKind::StringForward,
                        },
                        "string (maintenance)".to_string(),
                        &mut new_dict_refs.forward_packs.string_fwd_packs,
                    ),
                    Some(ns_code) => {
                        let Some(entry) = new_dict_refs
                            .forward_packs
                            .subject_fwd_ns_packs
                            .iter_mut()
                            .find(|(ns, _)| *ns == ns_code)
                        else {
                            continue;
                        };
                        (
                            ContentKind::DictBlob {
                                dict: fluree_db_core::DictKind::SubjectForward,
                            },
                            format!("subject ns={ns_code} (maintenance)"),
                            &mut entry.1,
                        )
                    }
                };

                uploaded_pack_cids.extend(
                    compact_forward_packs(
                        content_store.as_ref(),
                        kind,
                        refs,
                        &mut pack_sizes,
                        &mut compaction_budget,
                        CompactionSpans::default(),
                        &label,
                    )
                    .await?,
                );
            }
        }

        // Packs created and consumed within this cycle are referenced by no
        // root, so the base-vs-new diff in `set_dict_refs` cannot see them.
        let transient = unreferenced_uploads(
            &uploaded_pack_cids,
            &new_dict_refs.forward_packs,
            &base_root.dict_refs.forward_packs,
        );
        if !transient.is_empty() {
            tracing::debug!(
                count = transient.len(),
                "V6 Phase 3: forward packs created and compacted within one cycle"
            );
            root_builder.add_replaced_cids(transient);
        }
    }

    root_builder.set_dict_refs(new_dict_refs);

    // Triple-term dictionary: append this window's new terms to each inner
    // predicate's pack stream and compact, as for subjects; the reverse tree is
    // updated copy-on-write like the subject tree. A cycle without new terms
    // still compacts streams an earlier cycle left fragmented.
    let base_terms = novelty.base_root.term_dict.clone();
    if !novelty.new_terms.is_empty() || base_terms.is_some() {
        term_budget.absorb(std::mem::replace(
            &mut compaction_budget,
            CompactionBudget::disabled(),
        ));
        let (forward_packs, consumed) = update_term_forward_packs(
            content_store.as_ref(),
            base_terms.as_ref(),
            &novelty.new_terms,
            base_root.index_t.unsigned_abs(),
            &mut pack_sizes,
            &mut term_budget,
        )
        .await?;

        if novelty.new_terms.is_empty() {
            // Maintenance only: the dictionary changed if a merge ran.
            if let (Some(mut refs), false) = (base_terms, consumed.is_empty()) {
                refs.forward_packs = forward_packs;
                root_builder.set_term_dict(Some(refs), consumed);
            }
        } else {
            let base_count = base_terms.as_ref().map_or(0, |b| b.term_count);
            // Each reverse tree is updated copy-on-write over the base's, or
            // written fresh when there is no base dictionary (the ledger's
            // first terms meet an index that has none). A base dictionary
            // that predates the object tree keeps none until a rebuild.
            let update_tree = |base: Option<fluree_db_binary_index::DictTreeRefs>,
                               object_first: bool| {
                let content_store = content_store.as_ref();
                let new_terms = &novelty.new_terms;
                let warm_cache = warm_cache.as_deref();
                async move {
                    match base {
                        Some(base) => {
                            super::dicts::upload_incremental_reverse_tree_async_terms(
                                content_store,
                                &base,
                                new_terms,
                                object_first,
                                warm_cache,
                            )
                            .await
                        }
                        None => Ok(super::types::UpdatedReverseTree {
                            tree_refs:
                                fluree_db_binary_index::dict::term_dict::upload_reverse_tree(
                                    content_store,
                                    super::dicts::term_reverse_entries(new_terms, object_first)?,
                                )
                                .await
                                .map_err(|e| IndexerError::StorageWrite(e.to_string()))?,
                            replaced_cids: Vec::new(),
                        }),
                    }
                }
            };
            let updated_tree =
                update_tree(base_terms.as_ref().map(|b| b.reverse.clone()), false).await?;
            let object_tree = match &base_terms {
                Some(base) => match &base.object_reverse {
                    Some(tree) => Some(update_tree(Some(tree.clone()), true).await?),
                    None => None,
                },
                None => Some(update_tree(None, true).await?),
            };
            let mut replaced = updated_tree.replaced_cids;
            let object_reverse = object_tree.map(|tree| {
                replaced.extend(tree.replaced_cids);
                tree.tree_refs
            });
            let refs = fluree_db_binary_index::TermDictRefs {
                forward_packs,
                reverse: updated_tree.tree_refs,
                watermarks: novelty.term_watermarks.clone(),
                term_count: base_count + novelty.new_terms.len() as u64,
                object_reverse,
            };
            tracing::debug!(
                new_terms = novelty.new_terms.len(),
                term_count = refs.term_count,
                "V6 Phase 3: triple-term dictionary updated"
            );
            replaced.extend(consumed);
            root_builder.set_term_dict(Some(refs), replaced);
        }
    }

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
    root_builder.note_list_meta(novelty.shared.saw_list_meta);

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

        let mut arenas_by_gid: BTreeMap<fluree_db_core::GraphId, GraphArenaRefs> = BTreeMap::new();
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
                    let cid = upload_dict_blob_cached(
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

                        let old_manifest_bytes = fetch_cached_index_bytes(
                            content_store.as_ref(),
                            &existing.manifest,
                            "read existing vector manifest",
                        )
                        .await?;
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
                                    let old_last_bytes = fetch_cached_index_bytes(
                                        content_store.as_ref(),
                                        &old_last_cid,
                                        "read vector last shard",
                                    )
                                    .await?;
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
                                    let new_last_cid = upload_dict_blob_cached(
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
                            let shard_cid = upload_dict_blob_cached(
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
                        let manifest_cid = upload_dict_blob_cached(
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
                            let shard_cid = upload_dict_blob_cached(
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
                        let manifest_cid = upload_dict_blob_cached(
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
                    (fluree_db_core::GraphId, u32, u16),
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
                        let blob = fetch_cached_index_bytes(
                            content_store.as_ref(),
                            &ft_ref.arena_cid,
                            format!(
                                "fulltext incremental: prior arena fetch for (g_id={g_id}, p_id={p_id}, lang_id={lang_id}) cid={}",
                                ft_ref.arena_cid
                            ),
                        )
                        .await?;
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
                    cache_artifact_bytes(&*content_store, &arena_cid, &blob, "fulltext_arena");

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
                let mut grouped: BTreeMap<
                    (fluree_db_core::GraphId, u32),
                    Vec<&crate::spatial_hook::SpatialEntry>,
                > = BTreeMap::new();
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
                        let root_bytes = fetch_cached_index_bytes(
                            content_store.as_ref(),
                            &sp_ref.root_cid,
                            format!("spatial root load for g{g_id}:p{p_id}"),
                        )
                        .await?;
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
                            let bytes = fetch_cached_index_bytes(
                                content_store.as_ref(),
                                cid,
                                "spatial blob fetch",
                            )
                            .await?;
                            blob_cache.insert(cid.digest_hex(), bytes);
                        }
                        for leaflet_cid in &sp_ref.leaflets {
                            let bytes = fetch_cached_index_bytes(
                                content_store.as_ref(),
                                leaflet_cid,
                                "spatial leaflet fetch",
                            )
                            .await?;
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
                        let cid = content_store
                            .put(ContentKind::SpatialIndex, blob_bytes)
                            .await
                            .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
                        cache_artifact_bytes(&*content_store, &cid, blob_bytes, "spatial_blob");
                    }

                    // Build CIDs.
                    let spatial_codec = ContentKind::SpatialIndex.to_codec();
                    let root_json = serde_json::to_vec(&write_result.root)
                        .map_err(|e| IndexerError::Other(format!("spatial root serialize: {e}")))?;
                    let root_cid = content_store
                        .put(ContentKind::SpatialIndex, &root_json)
                        .await
                        .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
                    cache_artifact_bytes(&*content_store, &root_cid, &root_json, "spatial_root");
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
            match fetch_cached_index_bytes(
                content_store.as_ref(),
                cid,
                format!("sketch blob {cid}"),
            )
            .await
            {
                Ok(bytes) => match stats::HllSketchBlob::from_bytes(&bytes) {
                    Ok(blob) => {
                        let props = blob.into_properties();
                        tracing::debug!(
                            entries = props.len(),
                            bytes = bytes.len(),
                            "loaded prior HLL sketches for incremental refresh"
                        );
                        props
                    }
                    // A newer format means a downgraded indexer. Reseeding
                    // would silently thin NDV on every build; fail instead.
                    Err(e @ stats::SketchDecodeError::Unsupported(_)) => return Err(e.into()),
                    Err(e) => {
                        tracing::error!(error = %e, %cid, "failed to decode prior sketches, starting fresh");
                        std::collections::HashMap::new()
                    }
                },
                Err(e) => {
                    tracing::error!(error = %e, %cid, "failed to load sketch blob, starting fresh");
                    std::collections::HashMap::new()
                }
            }
        } else {
            std::collections::HashMap::new()
        };

        // Base carry-forward must not depend on the sketch blob alone. When
        // it is absent or unreadable, the hook would start from zero and the
        // new root would persist DELTA-ONLY per-graph property stats — base
        // predicates vanish and net-zero churn shows count 0, compounding on
        // every subsequent incremental (observed: 4 of 8 predicates left
        // after one incremental over a sketchless bulk import). Seed
        // counts/datatypes/last_modified_t from the base root's persisted
        // per-graph property stats instead. HLL registers cannot be
        // reconstructed from the persisted ndv ESTIMATES, so remember those
        // estimates and clamp the finalized ndv below — HLL ndv is monotone
        // (registers only grow), so the base estimate is a valid floor. The
        // clamp applies on every incremental (not just the fallback) so ndv
        // survives register loss in a previously fallback-seeded sketch.
        let mut prior_properties = prior_properties;
        let mut base_ndv_floor: std::collections::HashMap<stats::GraphPropertyKey, (u64, u64)> =
            std::collections::HashMap::new();
        if let Some(graphs) = base_root.stats.as_ref().and_then(|s| s.graphs.as_ref()) {
            for g in graphs {
                for prop in &g.properties {
                    let key = stats::GraphPropertyKey {
                        g_id: g.g_id,
                        p_id: prop.p_id,
                    };
                    base_ndv_floor.insert(key.clone(), (prop.ndv_values, prop.ndv_subjects));
                    // Per-key fallback: a blob entry (with real HLL registers)
                    // wins; a key the blob lacks is seeded from the base
                    // root's persisted per-graph stats.
                    let entry = prior_properties.entry(key).or_insert_with(|| {
                        let datatypes = prop
                            .datatypes
                            .iter()
                            .map(|&(tag, c)| (tag, c as i64))
                            .collect();
                        stats::IdPropertyHll::from_sketches(
                            prop.count as i64,
                            crate::hll::HllSketch256::new(),
                            crate::hll::HllSketch256::new(),
                            prop.last_modified_t,
                            datatypes,
                        )
                    });
                    // Historical accumulation (monotone across publishes): the
                    // base's persisted historical set, plus the base's exact
                    // current-state tags — the latter is what makes adoption
                    // sound when the base predates the historical wire tail.
                    entry.seed_historical_tags(
                        prop.historical_datatypes
                            .iter()
                            .copied()
                            .chain(prop.datatypes.iter().map(|&(tag, _)| tag)),
                    );
                }
            }
        }

        // Historical accumulation boundary (`IndexStats::historical_since_t`).
        // With per-graph base stats in hand, the seeding above covers state at
        // the base publish exactly, and the novelty walk observes every tag in
        // the window `(base_t, index_t]` — a tag visible at any `t` in that
        // window was either visible at `base_t` (seeded) or asserted in the
        // window (walked). So a base that carries a boundary passes it
        // forward, and a base that predates the tail adopts at its own `t`.
        // Without per-graph base stats there is nothing sound to seed from and
        // no boundary is claimed.
        let historical_since_t: Option<i64> = base_root
            .stats
            .as_ref()
            .filter(|s| s.graphs.is_some())
            .map(|s| s.historical_since_t.unwrap_or(base_root.index_t));

        // Seed hook with prior properties.
        let rdf_type_p_id = novelty
            .shared
            .predicates
            .get(fluree_vocab::rdf::TYPE)
            .unwrap_or(u32::MAX);
        let mut stats_hook = stats::IdStatsHook::with_prior_properties(prior_properties);
        stats_hook.set_rdf_type_p_id(rdf_type_p_id);
        stats_hook.set_track_ref_targets(true);

        // Live link counts carry forward from the base. A base that predates
        // them leaves them unknown: a count of only this window's links would
        // read as the ledger's.
        let reifies_p_id = novelty.shared.predicates.get(fluree_vocab::rdf::REIFIES);
        let base_links = base_root.stats.as_ref().and_then(|s| s.links.as_ref());
        if let (Some(reifies), Some(links)) = (reifies_p_id, base_links) {
            for link in links {
                let prefix = novelty
                    .shared
                    .ns_prefixes
                    .get(&link.sid.0)
                    .map(String::as_str)
                    .unwrap_or("");
                let iri = format!("{prefix}{}", link.sid.1);
                if let Some(inner) = novelty.shared.predicates.get(&iri) {
                    stats_hook.seed_term_rows(reifies, inner, link.count);
                }
            }
        }
        let links_known = base_links.is_some();

        // Seed per-graph flake totals from base root stats.
        if let Some(ref base_stats) = base_root.stats {
            if let Some(ref graphs) = base_stats.graphs {
                for g in graphs {
                    *stats_hook.graph_flakes_mut().entry(g.g_id).or_insert(0) += g.flakes as i64;
                }
            }
        }

        // Feed all novelty records. Counts are base-seeded, so each record's
        // signed delta reflects its materialized state transition (derived
        // from the SPOT merge's base-row matches), not its raw op: a
        // re-assert of a base-present fact and a retract of a base-absent
        // fact change no rows and contribute 0.
        for (i, rec) in novelty.records.iter().enumerate() {
            let op = novelty.ops[i];
            let sr = stats::stats_record_from_v2(rec, op);
            let base_present = base_matched.contains(&fact_identity_key(rec));
            stats_hook.on_record_with_base_presence(&sr, base_present);
        }

        // Upload HLL sketches (before finalize consumes the hook).
        let sketch_ref = match super::upload::upload_stats_sketch(
            content_store.as_ref(),
            novelty.max_t,
            stats_hook.properties(),
        )
        .await?
        {
            Some((cid, bytes)) => {
                cache_artifact_bytes(&*content_store, &cid, &bytes, "stats_sketch");
                Some(cid)
            }
            None => None,
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

            // ndv floor from the base root (see the seeding block above).
            if !base_ndv_floor.is_empty() {
                for g in &mut final_graphs {
                    for prop in &mut g.properties {
                        if let Some(&(nv, ns)) = base_ndv_floor.get(&stats::GraphPropertyKey {
                            g_id: g.g_id,
                            p_id: prop.p_id,
                        }) {
                            prop.ndv_values = prop.ndv_values.max(nv);
                            prop.ndv_subjects = prop.ndv_subjects.max(ns);
                        }
                    }
                }
            }

            // The planner reads this roll-up, not the per-graph entries, so it
            // must be taken AFTER the floor: rolled up before it, a seeded
            // property (empty registers) published ndv 0 and every
            // bound-subject probe on it was estimated at the 10-row fallback —
            // one write to a bulk-imported ledger made BSBM Q5 4x slower.
            // The floor preserves the prior NDV estimate without restoring
            // missing HLL registers. Follow-up: persist sketches during bulk
            // import so incremental NDV merging includes the imported data.
            let properties = crate::stats::aggregate_property_entries_from_graphs(
                &final_graphs,
                &trie,
                |p_id| {
                    novelty
                        .shared
                        .predicates
                        .resolve(p_id)
                        .map(ToString::to_string)
                },
            );

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
                    (fluree_db_core::GraphId, u64),
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
                        None,
                    )
                    .await
                    {
                        Ok(s) => Some(Arc::new(s)),
                        Err(e) => {
                            // We need the base store to seed base class entries and to
                            // re-attribute re-typed subjects. Without it, base entries
                            // resolve to sid64==0 and get dropped, publishing novelty-only
                            // (wrong) class-property stats — which the rdf:type-star COUNT
                            // fold then serves verbatim at HEAD. Abort to a full rebuild
                            // (which loads its own data and recomputes class stats exactly),
                            // mirroring the spatial-snapshot and re-type-workload gates above.
                            return Err(IndexerError::IncrementalAbort(format!(
                                "V6 store load for class attribution failed: {e}; \
                                 falling back to full rebuild for correct class stats"
                            )));
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
                    let mut subjects_by_graph: std::collections::HashMap<
                        fluree_db_core::GraphId,
                        Vec<u64>,
                    > = std::collections::HashMap::new();
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

                    // Prewarm the PSOT leaves the per-graph rdf:type scans will
                    // touch. Keys are built exactly as batched_lookup_predicate_refs
                    // derives them (p_id pinned, s_id spanning the subject range),
                    // so the warmed leaves match the scan. Cache-only; the scan's
                    // results are byte-identical. Reuses the one shared upload_budget.
                    for (&scan_g_id, scan_sids) in &subjects_by_graph {
                        if scan_sids.is_empty() {
                            continue;
                        }
                        let min_s = scan_sids[0];
                        let max_s = *scan_sids.last().unwrap_or(&min_s);
                        let min_key = fluree_db_binary_index::format::run_record_v2::RunRecordV2 {
                            s_id: fluree_db_core::subject_id::SubjectId::from_u64(min_s),
                            o_key: 0,
                            p_id: rdf_type_p_id,
                            t: 0,
                            o_i: 0,
                            o_type: 0,
                            g_id: scan_g_id,
                        };
                        let max_key = fluree_db_binary_index::format::run_record_v2::RunRecordV2 {
                            s_id: fluree_db_core::subject_id::SubjectId::from_u64(max_s),
                            o_key: u64::MAX,
                            p_id: rdf_type_p_id,
                            t: 0,
                            o_i: u32::MAX,
                            o_type: u16::MAX,
                            g_id: scan_g_id,
                        };
                        store
                            .prefetch_leaves_for_range(
                                scan_g_id,
                                fluree_db_binary_index::format::run_record::RunSortOrder::Psot,
                                &min_key,
                                &max_key,
                                &upload_budget,
                            )
                            .await;
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
                                // Loads the base class membership that all re-attribution
                                // (datatype/lang AND ref-class) is computed against. A
                                // failure here makes every downstream class delta unsound,
                                // so abort to a full rebuild rather than warn-and-continue.
                                return Err(IndexerError::IncrementalAbort(format!(
                                    "Phase 3b base PSOT class lookup failed for graph \
                                     {scan_g_id}: {e} (deferring class-stat recompute to \
                                     full rebuild)"
                                )));
                            }
                        }
                    }
                }

                // Snapshot BASE (pre-batch) class membership before applying this
                // batch's rdf:type deltas. Property *retractions* must be attributed
                // to the class set the subject had in the base index (where the base
                // stats counted them), while *assertions* attribute to the NET set.
                // When a subject's type is unchanged, base == net and this is a no-op
                // distinction. See issue #1266.
                let base_subject_classes = subject_classes.clone();

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

                // Subjects whose class membership CHANGED this batch (re-type, add or
                // remove a type, or full delete). Their existing-property attribution
                // can't be derived from in-batch deltas alone, so the routing loop
                // below skips them and the base-index re-scan (Stage 2) handles them.
                // A no-op rdf:type assertion (same type) leaves base == net and is not
                // considered changed. See issue #1266.
                let changed_membership: std::collections::HashSet<(fluree_db_core::GraphId, u64)> =
                    novelty_subject_class_deltas
                        .keys()
                        .filter(|key| base_subject_classes.get(*key) != subject_classes.get(*key))
                        .copied()
                        .collect();

                // Rebuild gate (issue #1266): re-typing/deleting an EXISTING subject
                // forces a base-index re-scan to move its class-scoped datatype/lang/ref
                // stats. Above a threshold, recomputing class stats from scratch via the
                // full-rebuild SPOT pass is cheaper and simpler, so abort incremental and
                // let the caller fall back to rebuild. Only count subjects that existed in
                // the base index (those drive the re-scan work); brand-new typed subjects
                // are free. An IncrementalAbort is caught in lib.rs and routed to rebuild.
                let retype_workload = changed_membership
                    .iter()
                    .filter(|key| base_subject_classes.contains_key(*key))
                    .count();
                if retype_workload > config.incremental_retype_max_subjects {
                    tracing::warn!(
                        retype_workload,
                        threshold = config.incremental_retype_max_subjects,
                        "Phase 3b: existing-subject rdf:type changes exceed threshold; \
                         aborting incremental for full rebuild (class stats recomputed \
                         from the SPOT pass)"
                    );
                    return Err(IndexerError::IncrementalAbort(format!(
                        "re-type workload {retype_workload} exceeds threshold {} \
                         (deferring class-stat recompute to full rebuild)",
                        config.incremental_retype_max_subjects
                    )));
                }

                // Build class→properties, class→prop→dts, class→prop→langs, ref_edges.
                let attribution_maps_started = Instant::now();
                let mut class_properties: std::collections::HashMap<
                    (fluree_db_core::GraphId, u64),
                    std::collections::HashSet<u32>,
                > = std::collections::HashMap::new();
                let mut class_prop_dts: std::collections::HashMap<
                    (fluree_db_core::GraphId, u64),
                    std::collections::HashMap<u32, std::collections::HashMap<u8, i64>>,
                > = std::collections::HashMap::new();
                let mut class_prop_lang_deltas: std::collections::HashMap<
                    (fluree_db_core::GraphId, u64),
                    std::collections::HashMap<u32, std::collections::HashMap<u16, i64>>,
                > = std::collections::HashMap::new();
                let mut ref_edges: std::collections::HashMap<
                    (fluree_db_core::GraphId, u64),
                    std::collections::HashMap<u32, std::collections::HashMap<u64, i64>>,
                > = std::collections::HashMap::new();

                // Attribute per-subject property deltas to classes with base-vs-net
                // routing (issue #1266): an assertion (positive delta) is attributed
                // to the subject's NET class set; a retraction (negative delta) to its
                // BASE class set (where the base stats counted it). When the subject's
                // type is unchanged base == net, so this matches the prior behavior.
                // Routing retractions to base makes a delete (type + props retracted in
                // one batch) decrement the right class instead of an emptied net set.
                //
                // NOTE: a subject re-typed WITHOUT touching its properties has no entry
                // in `novelty_subject_props` and is handled by the base-index re-scan
                // below, not here.
                for &(g_id, s_id) in novelty_subject_props.keys() {
                    if changed_membership.contains(&(g_id, s_id)) {
                        // Handled by the base-index re-scan below (Stage 2); skipping
                        // here avoids double-counting its in-batch deltas.
                        continue;
                    }
                    let net_cls = subject_classes.get(&(g_id, s_id));
                    let base_cls = base_subject_classes.get(&(g_id, s_id));
                    let mut classes_touched: std::collections::HashSet<u64> =
                        std::collections::HashSet::new();
                    if let Some(c) = net_cls {
                        classes_touched.extend(c.iter().copied());
                    }
                    if let Some(c) = base_cls {
                        classes_touched.extend(c.iter().copied());
                    }
                    let s_dts = novelty_subject_prop_dts.get(&(g_id, s_id));
                    let s_langs = novelty_subject_prop_langs.get(&(g_id, s_id));
                    for &class_sid64 in &classes_touched {
                        let in_net = net_cls.is_some_and(|c| c.contains(&class_sid64));
                        let in_base = base_cls.is_some_and(|c| c.contains(&class_sid64));
                        if let Some(s_dts) = s_dts {
                            for (&pid, dt_map) in s_dts {
                                for (&dt, &cnt) in dt_map {
                                    if (cnt > 0 && in_net) || (cnt < 0 && in_base) {
                                        *class_prop_dts
                                            .entry((g_id, class_sid64))
                                            .or_default()
                                            .entry(pid)
                                            .or_default()
                                            .entry(dt)
                                            .or_insert(0) += cnt;
                                        class_properties
                                            .entry((g_id, class_sid64))
                                            .or_default()
                                            .insert(pid);
                                    }
                                }
                            }
                        }
                        if let Some(s_langs) = s_langs {
                            for (&pid, lang_map) in s_langs {
                                for (&lid, &cnt) in lang_map {
                                    if (cnt > 0 && in_net) || (cnt < 0 && in_base) {
                                        *class_prop_lang_deltas
                                            .entry((g_id, class_sid64))
                                            .or_default()
                                            .entry(pid)
                                            .or_default()
                                            .entry(lid)
                                            .or_insert(0) += cnt;
                                        class_properties
                                            .entry((g_id, class_sid64))
                                            .or_default()
                                            .insert(pid);
                                    }
                                }
                            }
                        }
                    }
                }
                for (&(g_id, subj), per_prop) in &subject_ref_history {
                    let net_subj = subject_classes.get(&(g_id, subj));
                    let base_subj = base_subject_classes.get(&(g_id, subj));
                    let mut subj_classes_touched: std::collections::HashSet<u64> =
                        std::collections::HashSet::new();
                    if let Some(c) = net_subj {
                        subj_classes_touched.extend(c.iter().copied());
                    }
                    if let Some(c) = base_subj {
                        subj_classes_touched.extend(c.iter().copied());
                    }
                    for (&pid, objs) in per_prop {
                        for (&obj, &delta) in objs {
                            if delta == 0 {
                                continue;
                            }
                            let Some(obj_classes) = subject_classes.get(&(g_id, obj)) else {
                                continue;
                            };
                            // Route the edge by sign on the subject side (issue #1266):
                            // an asserted edge attributes to the subject's NET class set,
                            // a retracted edge to its BASE class set.
                            for &sc in &subj_classes_touched {
                                let in_net = net_subj.is_some_and(|c| c.contains(&sc));
                                let in_base = base_subj.is_some_and(|c| c.contains(&sc));
                                if !((delta > 0 && in_net) || (delta < 0 && in_base)) {
                                    continue;
                                }
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
                // Stage 2 (issue #1266): re-attribute the EXISTING properties of
                // subjects whose class membership changed this batch. The routing loop
                // above skipped these subjects; here we read their live property set
                // from the base index and move per-(property, datatype/lang) counts off
                // the classes they left and onto the classes they joined.
                //
                // For each changed subject S with base class set `B` and net set `N`,
                // and per-(p, dt) base count `old` plus in-batch delta `delta`
                // (`new = old + delta`):
                //   - class in B \ N (left):       apply -old
                //   - class in N \ B (joined):     apply  new
                //   - class in B ∩ N (kept):       apply  delta
                //
                // This block handles datatype + language distributions. Ref-class
                // edges (which depend on BOTH endpoints' classes) are re-attributed in
                // the dedicated ref pass further below — Case A (re-typed subjects'
                // outgoing edges, collected here from the same base scan) and Case B
                // (re-typed objects' inbound edges via reverse OPST).
                if let Some(ref store) = store_opt {
                    if !changed_membership.is_empty() {
                        let iri_ref_otype = fluree_db_core::o_type::OType::IRI_REF.as_u16();
                        // Outgoing ref edges (g_id, subject, p_id, object) of re-typed
                        // subjects, collected from the same base scan used for datatype
                        // re-attribution (Case A). Combined with re-typed objects' inbound
                        // edges (Case B, reverse OPST) below to move ref-class stats across
                        // the type change (issue #1266).
                        let mut outgoing_ref_edges: Vec<(fluree_db_core::GraphId, u64, u32, u64)> =
                            Vec::new();
                        let mut changed_by_graph: std::collections::HashMap<
                            fluree_db_core::GraphId,
                            Vec<u64>,
                        > = std::collections::HashMap::new();
                        for &(g_id, s_id) in &changed_membership {
                            changed_by_graph.entry(g_id).or_default().push(s_id);
                        }
                        for (&g_id, sids) in &changed_by_graph {
                            let base_props =
                                match fluree_db_binary_index::batched_lookup_subject_properties(
                                    store,
                                    g_id,
                                    sids,
                                    base_root.index_t,
                                ) {
                                    Ok(m) => m,
                                    Err(e) => {
                                        // This scan drives the datatype/language
                                        // re-attribution that makes the now-ungated
                                        // class-stat folds current-state-exact. A
                                        // warn-and-continue here would silently leave
                                        // re-typed subjects with stale per-(class,
                                        // property) stats, so abort to a full rebuild
                                        // (which recomputes class stats from the SPOT
                                        // pass). Caught in lib.rs and routed to rebuild.
                                        return Err(IndexerError::IncrementalAbort(format!(
                                            "Phase 3b base subject-property scan failed \
                                             for graph {g_id}: {e} (deferring class-stat \
                                             recompute to full rebuild)"
                                        )));
                                    }
                                };
                            for &s_id in sids {
                                let base_cls = base_subject_classes
                                    .get(&(g_id, s_id))
                                    .cloned()
                                    .unwrap_or_default();
                                let net_cls = subject_classes
                                    .get(&(g_id, s_id))
                                    .cloned()
                                    .unwrap_or_default();

                                // combined[p][dt] = (old_count_from_base, in_batch_delta)
                                let mut combined_dts: std::collections::HashMap<
                                    u32,
                                    std::collections::HashMap<u8, (i64, i64)>,
                                > = std::collections::HashMap::new();
                                let mut combined_langs: std::collections::HashMap<
                                    u32,
                                    std::collections::HashMap<u16, (i64, i64)>,
                                > = std::collections::HashMap::new();

                                if let Some(flakes) = base_props.get(&s_id) {
                                    for &(p_id, o_type, o_key) in flakes {
                                        if p_id == rdf_type_p_id {
                                            continue;
                                        }
                                        // Collect this subject's existing ref edges for
                                        // Case A ref-class re-attribution (o_key = target sid).
                                        if o_type == iri_ref_otype {
                                            outgoing_ref_edges.push((g_id, s_id, p_id, o_key));
                                        }
                                        let rec =
                                            fluree_db_binary_index::format::run_record_v2::RunRecordV2 {
                                                s_id: fluree_db_core::subject_id::SubjectId::from_u64(
                                                    s_id,
                                                ),
                                                o_key,
                                                p_id,
                                                t: 0,
                                                o_i: 0,
                                                o_type,
                                                g_id,
                                            };
                                        let sr = crate::stats::stats_record_from_v2(&rec, 1);
                                        combined_dts
                                            .entry(p_id)
                                            .or_default()
                                            .entry(sr.dt.as_u8())
                                            .or_insert((0, 0))
                                            .0 += 1;
                                        if sr.lang_id != 0
                                            && sr.dt
                                                == fluree_db_core::value_id::ValueTypeTag::LANG_STRING
                                        {
                                            combined_langs
                                                .entry(p_id)
                                                .or_default()
                                                .entry(sr.lang_id)
                                                .or_insert((0, 0))
                                                .0 += 1;
                                        }
                                    }
                                }
                                if let Some(s_dts) = novelty_subject_prop_dts.get(&(g_id, s_id)) {
                                    for (&pid, dt_map) in s_dts {
                                        for (&dt, &cnt) in dt_map {
                                            combined_dts
                                                .entry(pid)
                                                .or_default()
                                                .entry(dt)
                                                .or_insert((0, 0))
                                                .1 += cnt;
                                        }
                                    }
                                }
                                if let Some(s_langs) = novelty_subject_prop_langs.get(&(g_id, s_id))
                                {
                                    for (&pid, lang_map) in s_langs {
                                        for (&lid, &cnt) in lang_map {
                                            combined_langs
                                                .entry(pid)
                                                .or_default()
                                                .entry(lid)
                                                .or_insert((0, 0))
                                                .1 += cnt;
                                        }
                                    }
                                }

                                let mut classes_all: std::collections::HashSet<u64> =
                                    std::collections::HashSet::new();
                                classes_all.extend(base_cls.iter().copied());
                                classes_all.extend(net_cls.iter().copied());
                                for &c in &classes_all {
                                    let in_base = base_cls.contains(&c);
                                    let in_net = net_cls.contains(&c);
                                    for (&pid, dt_map) in &combined_dts {
                                        for (&dt, &(old, delta)) in dt_map {
                                            let amt = if in_net && in_base {
                                                delta
                                            } else if in_net {
                                                old + delta
                                            } else {
                                                -old
                                            };
                                            if amt != 0 {
                                                *class_prop_dts
                                                    .entry((g_id, c))
                                                    .or_default()
                                                    .entry(pid)
                                                    .or_default()
                                                    .entry(dt)
                                                    .or_insert(0) += amt;
                                                class_properties
                                                    .entry((g_id, c))
                                                    .or_default()
                                                    .insert(pid);
                                            }
                                        }
                                    }
                                    for (&pid, lang_map) in &combined_langs {
                                        for (&lid, &(old, delta)) in lang_map {
                                            let amt = if in_net && in_base {
                                                delta
                                            } else if in_net {
                                                old + delta
                                            } else {
                                                -old
                                            };
                                            if amt != 0 {
                                                *class_prop_lang_deltas
                                                    .entry((g_id, c))
                                                    .or_default()
                                                    .entry(pid)
                                                    .or_default()
                                                    .entry(lid)
                                                    .or_insert(0) += amt;
                                                class_properties
                                                    .entry((g_id, c))
                                                    .or_default()
                                                    .insert(pid);
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        // Ref-class edge re-attribution (issue #1266): move the ref-class
                        // edges of re-typed subjects/objects from their old (subject_class,
                        // p, object_class) buckets to their new ones. Only UNTOUCHED edges
                        // (present in the base index, not asserted/retracted this batch) —
                        // edges touched this batch are owned by the forward
                        // subject_ref_history pass above, so we skip them here.
                        for (&g_id, sids) in &changed_by_graph {
                            let mut edges: std::collections::HashSet<(u64, u32, u64)> =
                                std::collections::HashSet::new();
                            // Case A: outgoing edges of re-typed subjects (from the base scan).
                            for &(eg, s, p, o) in &outgoing_ref_edges {
                                if eg == g_id {
                                    edges.insert((s, p, o));
                                }
                            }
                            // Case B: inbound edges of re-typed objects (reverse OPST). Skip
                            // edges whose subject also changed — Case A already owns those.
                            match fluree_db_binary_index::batched_lookup_inbound_refs(
                                store,
                                g_id,
                                sids,
                                base_root.index_t,
                            ) {
                                Ok(inbound) => {
                                    for (&obj, ins) in &inbound {
                                        for &(p, subj) in ins {
                                            if changed_membership.contains(&(g_id, subj)) {
                                                continue;
                                            }
                                            edges.insert((subj, p, obj));
                                        }
                                    }
                                }
                                Err(e) => {
                                    // Case B inbound edges of re-typed objects. A miss here
                                    // would silently leave stale ref-class edge stats; abort
                                    // to a full rebuild for consistency with the other base
                                    // re-attribution scans (the rebuild recomputes ref_classes
                                    // exactly). Cheap insurance for the #1270 fold to come.
                                    return Err(IndexerError::IncrementalAbort(format!(
                                        "Phase 3b inbound ref scan failed for graph {g_id}: \
                                         {e} (deferring class-stat recompute to full rebuild)"
                                    )));
                                }
                            }
                            // Drop edges touched this batch — the forward pass owns them.
                            edges.retain(|&(s, p, o)| {
                                !subject_ref_history
                                    .get(&(g_id, s))
                                    .and_then(|per_prop| per_prop.get(&p))
                                    .map(|objs| objs.contains_key(&o))
                                    .unwrap_or(false)
                            });
                            if edges.is_empty() {
                                continue;
                            }
                            // Resolve classes for endpoints not already known from this batch
                            // (stable base entities) via one batched rdf:type lookup; endpoints
                            // in either batch map (re-typed / novelty) use those maps. A subject
                            // typed for the FIRST time this batch has no base entry — only a net
                            // one — so both maps must be checked or it would be mistaken for a
                            // stable (still class-less) base entity.
                            let mut external_sids: Vec<u64> = Vec::new();
                            for &(s, _p, o) in &edges {
                                if !base_subject_classes.contains_key(&(g_id, s))
                                    && !subject_classes.contains_key(&(g_id, s))
                                {
                                    external_sids.push(s);
                                }
                                if !base_subject_classes.contains_key(&(g_id, o))
                                    && !subject_classes.contains_key(&(g_id, o))
                                {
                                    external_sids.push(o);
                                }
                            }
                            external_sids.sort_unstable();
                            external_sids.dedup();
                            let external_base = if external_sids.is_empty() {
                                std::collections::HashMap::new()
                            } else {
                                // Resolves classes for stable ref-edge endpoints. A failure
                                // would mis-attribute (or drop) ref-class edges; abort to a
                                // full rebuild rather than `unwrap_or_default()` (which would
                                // silently treat every endpoint as class-less).
                                match fluree_db_binary_index::batched_lookup_predicate_refs(
                                    store,
                                    g_id,
                                    rdf_type_p_id,
                                    &external_sids,
                                    base_root.index_t,
                                ) {
                                    Ok(m) => m,
                                    Err(e) => {
                                        return Err(IndexerError::IncrementalAbort(format!(
                                            "Phase 3b external ref-endpoint class lookup failed \
                                             for graph {g_id}: {e} (deferring class-stat \
                                             recompute to full rebuild)"
                                        )));
                                    }
                                }
                            };
                            // (base_classes, net_classes) for an endpoint. A stable base
                            // entity has base == net (its type did not change this batch).
                            // Batch-local membership is keyed on EITHER map: a first-time-typed
                            // subject is absent from base but present in net; keying on base
                            // alone would resolve it externally as class-less and drop the +1
                            // side of its edge moves.
                            let resolve = |sid: u64| -> (Vec<u64>, Vec<u64>) {
                                let key = (g_id, sid);
                                if base_subject_classes.contains_key(&key)
                                    || subject_classes.contains_key(&key)
                                {
                                    let base: Vec<u64> = base_subject_classes
                                        .get(&key)
                                        .map(|b| b.iter().copied().collect())
                                        .unwrap_or_default();
                                    let net: Vec<u64> = subject_classes
                                        .get(&key)
                                        .map(|s| s.iter().copied().collect())
                                        .unwrap_or_default();
                                    (base, net)
                                } else {
                                    let cls = external_base.get(&sid).cloned().unwrap_or_default();
                                    (cls.clone(), cls)
                                }
                            };
                            // Per affected edge: -1 at every (base_subj_class, p, base_obj_class),
                            // +1 at every (net_subj_class, p, net_obj_class).
                            for &(s, p, o) in &edges {
                                let (bs, ns) = resolve(s);
                                let (bo, no) = resolve(o);
                                for &sc in &bs {
                                    for &oc in &bo {
                                        *ref_edges
                                            .entry((g_id, sc))
                                            .or_default()
                                            .entry(p)
                                            .or_default()
                                            .entry(oc)
                                            .or_insert(0) -= 1;
                                    }
                                }
                                for &sc in &ns {
                                    for &oc in &no {
                                        *ref_edges
                                            .entry((g_id, sc))
                                            .or_default()
                                            .entry(p)
                                            .or_default()
                                            .entry(oc)
                                            .or_insert(0) += 1;
                                    }
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
                let mut entries_by_key: std::collections::HashMap<
                    (fluree_db_core::GraphId, u64),
                    is::ClassStatEntry,
                > = std::collections::HashMap::new();

                // Prewarm the subject reverse-tree leaves the serial seed loop
                // below will read. The loop resolves each base class Sid via
                // find_subject_id_by_parts (a sync reverse_lookup); on a cold
                // remote store those lookups are serial S3 fetches. The store
                // method encodes keys identically, so this only warms the disk
                // cache — seed results are byte-identical. Reuses the one shared
                // upload_budget; runs in Phase 3b (after Phase 1/2), so no
                // Phase 1 contention.
                if let Some(store) = store_opt.as_ref() {
                    if let Some(ref base_stats) = base_root.stats {
                        if let Some(ref graphs) = base_stats.graphs {
                            let parts: Vec<(u16, String)> = graphs
                                .iter()
                                .filter_map(|g| g.classes.as_ref())
                                .flatten()
                                .map(|entry| {
                                    (
                                        entry.class_sid.namespace_code,
                                        entry.class_sid.name.to_string(),
                                    )
                                })
                                .collect();
                            store
                                .prefetch_subject_reverse_leaves(&parts, &upload_budget)
                                .await;
                        }
                    }
                }

                // Seed from base root per-graph class entries, resolving every
                // class Sid → sid64 in one batched reverse-tree pass (a lookup
                // per class rereads a whole leaf).
                if let (Some(store), Some(graphs)) = (
                    store_opt.as_ref(),
                    base_root.stats.as_ref().and_then(|s| s.graphs.as_ref()),
                ) {
                    let entries: Vec<(fluree_db_core::GraphId, &is::ClassStatEntry)> = graphs
                        .iter()
                        .filter_map(|g| g.classes.as_ref().map(|c| (g.g_id, c)))
                        .flat_map(|(g_id, classes)| classes.iter().map(move |e| (g_id, e)))
                        .collect();
                    let parts: Vec<(u16, &str)> = entries
                        .iter()
                        .map(|(_, e)| (e.class_sid.namespace_code, e.class_sid.name.as_ref()))
                        .collect();
                    base_class_sid_lookups.fetch_add(parts.len(), Ordering::Relaxed);
                    // A failed read would drop the class from the published
                    // stats; recompute them in a full rebuild instead.
                    let sid64s = store.find_subject_ids_by_parts(&parts).map_err(|e| {
                        IndexerError::IncrementalAbort(format!(
                            "Phase 3b base class Sid lookup failed: {e} (deferring \
                             class-stat recompute to full rebuild)"
                        ))
                    })?;
                    for ((g_id, entry), sid64) in entries.into_iter().zip(sid64s) {
                        if let Some(sid64) = sid64.filter(|&s| s != 0) {
                            base_class_sid_hits.fetch_add(1, Ordering::Relaxed);
                            entries_by_key.insert((g_id, sid64), entry.clone());
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

                let mut class_sid_cache: std::collections::HashMap<u64, fluree_db_core::Sid> =
                    entries_by_key
                        .iter()
                        .filter_map(|(&(_, class_sid64), entry)| {
                            if entry.class_sid.name.is_empty()
                                && entry.class_sid.namespace_code == 0
                            {
                                None
                            } else {
                                Some((class_sid64, entry.class_sid.clone()))
                            }
                        })
                        .collect();
                tracing::debug!(
                    cached_class_sids = class_sid_cache.len(),
                    "Phase 3b: cached class Sid values for property attribution merge"
                );

                // Ensure every class touched by a property/lang/ref delta this batch
                // has an entry, even if it has no instance-count delta and is absent
                // from the base stats (issue #1266: a class that only lost a property
                // via retraction, or gained one via re-typing, must be revisited so its
                // decrement/move is applied rather than dropped).
                let mut delta_class_keys: std::collections::HashSet<(
                    fluree_db_core::GraphId,
                    u64,
                )> = std::collections::HashSet::new();
                delta_class_keys.extend(class_properties.keys().copied());
                delta_class_keys.extend(class_prop_dts.keys().copied());
                delta_class_keys.extend(class_prop_lang_deltas.keys().copied());
                delta_class_keys.extend(ref_edges.keys().copied());
                for (g_id, class_sid64) in delta_class_keys {
                    entries_by_key
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
                }

                // Build property attribution for each class entry.
                let property_merge_started = Instant::now();
                tracing::debug!(
                    total_entries = entries_by_key.len(),
                    classes_with_properties = class_properties.len(),
                    class_prop_dts = class_prop_dts.len(),
                    class_prop_lang_deltas = class_prop_lang_deltas.len(),
                    classes_with_ref_edges = ref_edges.len(),
                    "Phase 3b: property attribution merge starting"
                );
                let ref_class_sid_lookups = AtomicUsize::new(0usize);
                let ref_class_sid_hits = AtomicUsize::new(0usize);
                let mut merged_class_entries = 0usize;
                let total_class_entries = entries_by_key.len();
                let mut visited_class_entries = 0usize;
                let mut last_property_merge_log = Instant::now();
                let mut next_merged_class_progress = 500usize;
                for (&(g_id, class_sid64), entry) in &mut entries_by_key {
                    visited_class_entries += 1;
                    let class_dts = class_prop_dts.get(&(g_id, class_sid64));
                    let class_refs = ref_edges.get(&(g_id, class_sid64));
                    let class_langs = class_prop_lang_deltas.get(&(g_id, class_sid64));
                    let class_props_present = class_properties.get(&(g_id, class_sid64));
                    // Revisit a class if it was touched by ANY delta this batch. The
                    // prior gate used only the assert-only presence set, so a class
                    // whose sole change was a retraction (or a re-type moving a
                    // property away) was never revisited and its decrement was dropped
                    // (issue #1266). Untouched base classes still skip the body and
                    // keep their existing property usage unchanged.
                    if class_dts.is_some()
                        || class_refs.is_some()
                        || class_langs.is_some()
                        || class_props_present.is_some()
                    {
                        let class_merge_started = Instant::now();
                        let existing_property_count = entry.properties.len();
                        let novelty_property_count =
                            class_props_present.map_or(0, std::collections::HashSet::len);

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
                        if let Some(props) = class_props_present {
                            prop_set.extend(props);
                        }
                        if let Some(m) = class_dts {
                            prop_set.extend(m.keys().copied());
                        }
                        if let Some(m) = class_langs {
                            prop_set.extend(m.keys().copied());
                        }
                        if let Some(m) = class_refs {
                            prop_set.extend(m.keys().copied());
                        }

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

                        let total_class_properties = prop_set.len();
                        let mut merged_properties = Vec::with_capacity(total_class_properties);
                        let mut merged_property_count = 0usize;
                        let mut last_class_property_log = Instant::now();
                        for &cp_id in &prop_set {
                            let iri = novelty.shared.predicates.resolve(cp_id).unwrap_or("");
                            let p_sid = match trie.longest_match(iri) {
                                Some((code, plen)) => fluree_db_core::Sid::new(code, &iri[plen..]),
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
                                    *merged_langs.entry(tag.clone()).or_insert(0) += *cnt as i64;
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

                            // Merge by Sid directly. The old path converted every
                            // base ref-class Sid back through the reverse dictionary,
                            // which can demand-load dictionary leaves during Phase 3b.
                            let mut merged_refs: std::collections::HashMap<
                                fluree_db_core::Sid,
                                i64,
                            > = std::collections::HashMap::new();
                            if let Some(bpu) = base_pu {
                                for rc in &bpu.ref_classes {
                                    *merged_refs.entry(rc.class_sid.clone()).or_insert(0) +=
                                        rc.count as i64;
                                }
                            }
                            if let Some(ref_delta) = class_refs.and_then(|m| m.get(&cp_id)) {
                                let ref_delta_count = ref_delta.len();
                                let mut merged_ref_delta_count = 0usize;
                                let mut last_ref_delta_log = Instant::now();
                                for (&target, &cnt) in ref_delta {
                                    ref_class_sid_lookups.fetch_add(1, Ordering::Relaxed);
                                    let cs = if let Some(cs) = class_sid_cache.get(&target) {
                                        ref_class_sid_hits.fetch_add(1, Ordering::Relaxed);
                                        cs.clone()
                                    } else {
                                        let sid =
                                            fluree_db_core::subject_id::SubjectId::from_u64(target);
                                        let cs = if let Some(suffix) =
                                            new_subject_suffix.get(&(sid.ns_code(), sid.local_id()))
                                        {
                                            resolve_class_sid_novelty_hits
                                                .fetch_add(1, Ordering::Relaxed);
                                            fluree_db_core::Sid::new(sid.ns_code(), suffix.as_str())
                                        } else {
                                            resolve_class_sid_fallbacks
                                                .fetch_add(1, Ordering::Relaxed);
                                            fluree_db_core::Sid::new(
                                                sid.ns_code(),
                                                sid.local_id().to_string(),
                                            )
                                        };
                                        class_sid_cache.insert(target, cs.clone());
                                        cs
                                    };
                                    *merged_refs.entry(cs).or_insert(0) += cnt;
                                    merged_ref_delta_count += 1;
                                    if last_ref_delta_log.elapsed()
                                        >= std::time::Duration::from_secs(5)
                                    {
                                        tracing::debug!(
                                            g_id,
                                            class_sid64,
                                            cp_id,
                                            merged_ref_delta_count,
                                            ref_delta_count,
                                            class_sid_cache_size = class_sid_cache.len(),
                                            elapsed_ms =
                                                class_merge_started.elapsed().as_millis() as u64,
                                            phase_elapsed_ms =
                                                phase3b_started.elapsed().as_millis() as u64,
                                            "Phase 3b: novelty ref-class deltas still merging"
                                        );
                                        last_ref_delta_log = Instant::now();
                                    }
                                }
                            }
                            let ref_classes: Vec<is::ClassRefCount> = merged_refs
                                .into_iter()
                                .filter(|(_, v)| *v > 0)
                                .map(|(class_sid, cnt)| is::ClassRefCount {
                                    class_sid,
                                    count: cnt as u64,
                                })
                                .collect();

                            merged_properties.push(is::ClassPropertyUsage {
                                property_sid: p_sid,
                                datatypes,
                                langs,
                                ref_classes,
                            });
                            merged_property_count += 1;

                            if last_class_property_log.elapsed()
                                >= std::time::Duration::from_secs(5)
                            {
                                tracing::debug!(
                                    g_id,
                                    class_sid64,
                                    merged_property_count,
                                    total_class_properties,
                                    ref_class_sid_lookups =
                                        ref_class_sid_lookups.load(Ordering::Relaxed),
                                    elapsed_ms = class_merge_started.elapsed().as_millis() as u64,
                                    phase_elapsed_ms = phase3b_started.elapsed().as_millis() as u64,
                                    "Phase 3b: class property attribution entry still merging"
                                );
                                last_class_property_log = Instant::now();
                            }
                        }
                        entry.properties = merged_properties;
                        merged_class_entries += 1;
                        let class_merge_ms = class_merge_started.elapsed().as_millis() as u64;
                        if class_merge_ms >= 1_000 {
                            tracing::debug!(
                                g_id,
                                class_sid64,
                                existing_property_count,
                                novelty_property_count,
                                merged_properties = entry.properties.len(),
                                class_merge_ms,
                                visited_class_entries,
                                total_entries = total_class_entries,
                                "Phase 3b: slow class property attribution merge"
                            );
                        }
                    }
                    // Resolve class SID if still placeholder.
                    if entry.class_sid.name.is_empty() && entry.class_sid.namespace_code == 0 {
                        entry.class_sid = resolve_class_sid(
                            class_sid64,
                            store_opt.as_deref(),
                            &new_subject_suffix,
                        );
                    }
                    if merged_class_entries >= next_merged_class_progress {
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
                        last_property_merge_log = Instant::now();
                        next_merged_class_progress = next_merged_class_progress.saturating_add(500);
                    } else if last_property_merge_log.elapsed() >= std::time::Duration::from_secs(5)
                    {
                        tracing::debug!(
                            visited_class_entries,
                            merged_class_entries,
                            total_entries = total_class_entries,
                            ref_class_sid_lookups = ref_class_sid_lookups.load(Ordering::Relaxed),
                            ref_class_sid_hits = ref_class_sid_hits.load(Ordering::Relaxed),
                            resolve_class_sid_calls =
                                resolve_class_sid_calls.load(Ordering::Relaxed),
                            phase_elapsed_ms = phase3b_started.elapsed().as_millis() as u64,
                            elapsed_ms = property_merge_started.elapsed().as_millis() as u64,
                            "Phase 3b: property attribution merge still running"
                        );
                        last_property_merge_log = Instant::now();
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
                let mut by_graph: std::collections::HashMap<
                    fluree_db_core::GraphId,
                    Vec<is::ClassStatEntry>,
                > = std::collections::HashMap::new();
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
            let links = links_known.then(|| {
                crate::stats::link_stat_entries(&id_stats_result.term_rows, reifies_p_id, |p_id| {
                    let iri = novelty.shared.predicates.resolve(p_id).unwrap_or("");
                    match trie.longest_match(iri) {
                        Some((code, prefix_len)) => (code, iri[prefix_len..].to_string()),
                        None => (0u16, iri.to_string()),
                    }
                })
            });

            is::IndexStats {
                flakes: id_stats_result.total_flakes,
                size: 0,
                properties: Some(properties),
                classes: root_classes,
                graphs: Some(final_graphs),
                historical_since_t,
                links,
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
        let phase3c_started = Instant::now();

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
            tracing::debug!(
                subclass_p_id,
                subprop_p_id,
                novelty_records = novelty.records.len(),
                "Phase 3c: incremental V6 schema refresh starting"
            );
            match fluree_db_binary_index::read::binary_index_store::BinaryIndexStore::load_from_root_v6(
                content_store.clone(),
                base_root,
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
                    tracing::debug!(
                        elapsed_ms = phase3c_started.elapsed().as_millis() as u64,
                        "Phase 3c: incremental V6 schema refreshed"
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        "Phase 3c: V6 store load for schema extraction failed; \
                         carrying forward base schema"
                    );
                }
            }
        } else {
            tracing::debug!(
                elapsed_ms = phase3c_started.elapsed().as_millis() as u64,
                "Phase 3c: no schema records in novelty; carrying forward base schema"
            );
        }
    }

    // ---- Phase 3d: release a legacy annotation arena ----
    if let Some(ref arena) = base_root.legacy_annotation_arena {
        root_builder.release_legacy_annotation_arena(
            fluree_db_binary_index::legacy_annotation_arena_cids(content_store.as_ref(), arena)
                .await?,
        );
    }

    // ---- Phase 4: Root assembly ----
    let phase4_started = Instant::now();
    tracing::debug!(
        base_index_t = base_root.index_t,
        new_index_t = novelty.max_t,
        total_new_leaves,
        "Phase 4: incremental root assembly starting"
    );
    root_builder.set_prev_index(Some(BinaryPrevIndexRef {
        t: base_root.index_t,
        id: base_root_id.clone(),
    }));

    let (new_root, replaced_cids) = root_builder.build();
    tracing::debug!(
        replaced_cids = replaced_cids.len(),
        elapsed_ms = phase4_started.elapsed().as_millis() as u64,
        "Phase 4: root builder finalized"
    );

    let mut final_root = new_root;
    super::root_assembly::attach_garbage_manifest(
        content_store.as_ref(),
        &mut final_root,
        ledger_id,
        &replaced_cids,
    )
    .await?;

    if let Some(stats) = final_root.stats.as_mut() {
        stats.distribute_total_size_by_flakes(final_root.total_commit_size);
    }

    super::root_assembly::reconcile_ns_at_publish(
        &final_root.namespace_codes,
        &novelty.shared.ns_prefixes,
        final_root.index_t,
    )?;
    // Bail to a full rebuild rather than panicking in encode(); a rebuild
    // re-cuts packs by size and is the only thing that shrinks this table.
    super::root_assembly::ensure_pack_counts_encodable(&final_root)?;

    let root_encode_started = Instant::now();
    let root_bytes = final_root.encode();
    tracing::debug!(
        bytes = root_bytes.len(),
        elapsed_ms = root_encode_started.elapsed().as_millis() as u64,
        "Phase 4: encoded incremental index root"
    );
    let root_write_started = Instant::now();
    tracing::debug!(
        bytes = root_bytes.len(),
        index_t = final_root.index_t,
        "Phase 4: writing incremental index root"
    );
    let root_id = content_store
        .put(ContentKind::IndexRoot, &root_bytes)
        .await
        .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
    cache_artifact_bytes(&*content_store, &root_id, &root_bytes, "index_root");

    tracing::debug!(
        %root_id,
        index_t = final_root.index_t,
        replaced = replaced_cids.len(),
        new_leaves = total_new_leaves,
        root_write_ms = root_write_started.elapsed().as_millis() as u64,
        phase4_elapsed_ms = phase4_started.elapsed().as_millis() as u64,
        "V6 incremental index root published"
    );

    Ok(IndexResult {
        root_id,
        index_t: final_root.index_t,
        ledger_id: fluree_db_core::IntoLedgerId::into_ledger_id(ledger_id),
        stats: IndexStats {
            flake_count: novelty.records.len(),
            leaf_count: total_new_leaves,
            branch_count: by_graph.len(),
            total_bytes: root_bytes.len(),
        },
        // Outer entry point fills fuel from the tracker tally.
        fuel: None,
        fence: None,
    })
}

/// Fold every pooled secondary order's delta runs into its base without new
/// commits, for a ledger whose delta would otherwise wait for its next
/// write. The root keeps its `t`, GSPO, dictionaries and stats; the runs and
/// the leaves they replace become garbage. `None` when the root holds no
/// delta.
pub async fn compact_pool_deltas(
    content_store: Arc<dyn ContentStore>,
    tracker: fluree_db_core::tracking::Tracker,
    ledger_id: &str,
    record: &fluree_db_nameservice::NsRecord,
    config: IndexerConfig,
) -> Result<Option<IndexResult>> {
    let base_root_id = record.index_head_id.clone().ok_or(IndexerError::NoIndex)?;
    let root_bytes = content_store
        .get(&base_root_id)
        .await
        .map_err(|e| IndexerError::StorageRead(format!("index root: {e}")))?;
    let base_root = fluree_db_binary_index::format::index_root::IndexRoot::decode(&root_bytes)
        .map_err(|e| IndexerError::StorageRead(format!("index root: {e}")))?;
    let Some(pool) = base_root.pool.as_ref() else {
        return Ok(None);
    };
    let deltas: Vec<_> = pool
        .deltas
        .iter()
        .filter(|(_, runs)| !runs.is_empty())
        .map(|(order, runs)| (*order, runs.as_slice()))
        .collect();
    let mut tasks = Vec::new();
    for (order, events) in delta_run_events(content_store.as_ref(), &deltas, &[]).await? {
        let branch_cid = pool
            .orders
            .iter()
            .find(|(o, _)| *o == order)
            .map(|(_, cid)| cid.clone())
            .ok_or_else(|| {
                IndexerError::IncrementalAbort(format!("pooled tree has no {order:?} branch"))
            })?;
        let (mut sorted, mut superseded) = split_lifecycles(events, order);
        let cmp = cmp_v2_for_order(order);
        sorted.sort_unstable_by(|a, b| cmp(&a.0, &b.0));
        superseded.sort_unstable_by(|a, b| cmp(&a.0, &b.0));
        tasks.push(Phase2Task {
            seq: tasks.len(),
            target: TreeTarget::Pool,
            purge: Arc::new(Vec::new()),
            order,
            sorted_records: sorted.iter().map(|&(rec, _)| rec).collect(),
            sorted_ops: sorted.iter().map(|&(_, op)| op).collect(),
            sorted_superseded: superseded.iter().map(|&(rec, _)| rec).collect(),
            sorted_superseded_ops: superseded.iter().map(|&(_, op)| op).collect(),
            kind: Phase2TaskKind::NamedExisting { branch_cid },
        });
    }
    if tasks.is_empty() {
        return Ok(None);
    }
    let started = Instant::now();
    let orders = tasks.len();
    let upload_buffer = config.incremental_leaf_upload_concurrency.max(1);
    let upload_budget = Arc::new(Semaphore::new(upload_buffer));
    let config_ref = &config;
    let outputs: Vec<Phase2TaskOutput> = stream::iter(tasks)
        .map(|task| {
            let content_store = content_store.clone();
            let tracker = tracker.clone();
            let upload_budget = Arc::clone(&upload_budget);
            async move {
                execute_phase2_task(
                    task,
                    config_ref,
                    content_store,
                    tracker,
                    upload_budget,
                    upload_buffer,
                )
                .await
            }
        })
        .buffer_unordered(config.incremental_max_concurrency.max(1))
        .try_collect()
        .await?;

    let mut root_builder = IncrementalRootBuilder::from_old_root(base_root.clone(), ledger_id);
    let mut new_leaves = 0usize;
    for output in outputs {
        let Phase2TaskUpdate::Branch { branch_cid } = output.update else {
            return Err(IndexerError::IncrementalAbort(
                "pooled compaction produced no branch".into(),
            ));
        };
        new_leaves += output.new_leaf_count;
        root_builder.set_pool_branch(output.order, branch_cid);
        root_builder.clear_pool_delta(output.order);
        root_builder.add_replaced_cids(output.replaced_leaf_cids);
        root_builder.add_replaced_cids(output.replaced_sidecar_cids);
    }
    root_builder.set_prev_index(Some(BinaryPrevIndexRef {
        t: base_root.index_t,
        id: base_root_id,
    }));
    let (mut root, replaced_cids) = root_builder.build();
    super::root_assembly::attach_garbage_manifest(
        content_store.as_ref(),
        &mut root,
        ledger_id,
        &replaced_cids,
    )
    .await?;
    let root_bytes = root.encode();
    let root_id = content_store
        .put(ContentKind::IndexRoot, &root_bytes)
        .await
        .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
    cache_artifact_bytes(&*content_store, &root_id, &root_bytes, "index_root");
    tracing::info!(
        ledger_id,
        %root_id,
        index_t = root.index_t,
        orders,
        new_leaves,
        replaced = replaced_cids.len(),
        elapsed_ms = started.elapsed().as_millis() as u64,
        "pooled delta compacted"
    );
    Ok(Some(IndexResult {
        root_id,
        index_t: root.index_t,
        ledger_id: fluree_db_core::IntoLedgerId::into_ledger_id(ledger_id),
        stats: IndexStats {
            flake_count: 0,
            leaf_count: new_leaves,
            branch_count: orders,
            total_bytes: root_bytes.len(),
        },
        fuel: None,
        fence: None,
    }))
}

/// Pooled secondary orders' delta runs read back as the windows they
/// recorded, ready to fold into one merge (see [`with_replacements`]),
/// without the events of the graphs in `leaving` (sorted). Every order's runs
/// are read at once, and each order's events stay in run order.
async fn delta_run_events(
    content_store: &dyn ContentStore,
    orders: &[(RunSortOrder, &[fluree_db_binary_index::PoolDeltaRun])],
    leaving: &[fluree_db_core::GraphId],
) -> Result<Vec<(RunSortOrder, Events)>> {
    let mut out: Vec<(RunSortOrder, Events)> = orders
        .iter()
        .map(|&(order, _)| (order, Events::new()))
        .collect();
    let fetches: Vec<(usize, ContentId)> = orders
        .iter()
        .enumerate()
        .flat_map(|(i, (_, runs))| runs.iter().map(move |run| (i, run.cid.clone())))
        .collect();
    let mut runs = stream::iter(fetches)
        .map(|(i, cid)| async move {
            let bytes = content_store
                .get(&cid)
                .await
                .map_err(|e| IndexerError::StorageRead(format!("pool delta run: {e}")))?;
            let (_, events) = fluree_db_binary_index::format::pool_delta::decode_delta_run(&bytes)
                .map_err(|e| IndexerError::StorageRead(format!("pool delta run: {e}")))?;
            Ok::<_, IndexerError>((i, events))
        })
        .buffered(fluree_db_binary_index::format::pool_delta::RUN_FETCH_WIDTH);
    while let Some((i, events)) = runs.try_next().await? {
        out[i].1.extend(with_replacements(
            events
                .into_iter()
                .filter(|e| leaving.binary_search(&e.g_id).is_err())
                .map(delta_record)
                .collect(),
        ));
    }
    Ok(out)
}

// ============================================================================
// Helpers
// ============================================================================

/// Byte/object counts from a single leaf-blob upload, accumulated for tracing.
#[derive(Default, Clone, Copy)]
struct LeafUploadCounts {
    leaf_bytes: u64,
    sidecar_bytes: u64,
    sidecar_count: u64,
}

/// Merge input bytes one index cycle may rewrite, shared across every
/// forward-dictionary stream. Caps the latency compaction adds to a publish;
/// whatever is left unmerged is picked up by the next cycle.
const COMPACTION_BUDGET_BYTES: u64 = 64 * 1024 * 1024;

/// Storage operations one index cycle may spend on compaction, shared across
/// every stream.
///
/// A byte budget alone does not bound *requests*: a backlog of 113-byte packs
/// consumes almost none of it while still costing a size probe and a full GET
/// per pack. Charging every operation is what actually caps the round trips a
/// publish waits on, and sharing one counter across streams stops a ledger with
/// many namespaces from multiplying the cost.
const COMPACTION_MAX_REQUESTS: usize = 2048;

/// Packs at the end of a stream examined every cycle, whatever else happens.
///
/// New packs always land here, so this window is what keeps the table bounded.
/// It must never be starved by backlog repair — a table larger than the scan
/// budget would otherwise hide its own tail and grow without limit.
const COMPACTION_TAIL_SPAN: usize = 64;

/// Packs examined per step when sweeping older regions for fragmentation.
const COMPACTION_HEAD_SPAN: usize = 256;

/// Concurrent storage operations. Probes and fetches are independent objects,
/// so serializing them would make a merge cost `run_length` round trips.
const COMPACTION_CONCURRENCY: usize = 16;

/// How far each scan reaches. Overridable so tests can drive the real windowing
/// without building a table the size of the production spans.
#[derive(Clone, Copy)]
struct CompactionSpans {
    tail: usize,
    head: usize,
}

impl Default for CompactionSpans {
    fn default() -> Self {
        Self {
            tail: COMPACTION_TAIL_SPAN,
            head: COMPACTION_HEAD_SPAN,
        }
    }
}

/// Whether forward pack compaction runs at all.
///
/// Compaction rewrites and re-uploads dictionary data during an index build, so
/// a deployment needs a way to stop it without a rollback — the same reason
/// `FLUREE_MMAP_MIN_BYTES` exists. Off means packs are appended and
/// never merged, which is the pre-compaction behaviour exactly.
///
/// Defaults to on; only `0`, `false`, `off`, or `no` disable it. Read once per
/// process — set it at startup, not per build.
fn compaction_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| match std::env::var("FLUREE_DICT_COMPACTION") {
        Ok(v) => !matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "0" | "false" | "off" | "no"
        ),
        Err(_) => true,
    })
}

/// Cycle-wide compaction allowance, shared by every forward-dictionary stream.
struct CompactionBudget {
    bytes: u64,
    requests: usize,
}

impl CompactionBudget {
    fn new() -> Self {
        Self {
            bytes: COMPACTION_BUDGET_BYTES,
            requests: COMPACTION_MAX_REQUESTS,
        }
    }

    fn exhausted(&self) -> bool {
        self.bytes == 0 || self.requests == 0
    }

    /// A budget that permits nothing, so every compaction site is a no-op.
    fn disabled() -> Self {
        Self {
            bytes: 0,
            requests: 0,
        }
    }

    /// Reserve up to `n` operations, returning how many were granted. For work
    /// that can proceed on a partial grant, such as probing pack sizes until the
    /// budget runs out.
    fn take_requests(&mut self, n: usize) -> usize {
        let granted = n.min(self.requests);
        self.requests -= granted;
        granted
    }

    /// Split off `1 / divisor` of what remains, for streams that must not be
    /// starved by the ones served before them.
    fn split_off(&mut self, divisor: u64) -> Self {
        let bytes = self.bytes / divisor;
        let requests = self.requests / divisor as usize;
        self.bytes -= bytes;
        self.requests -= requests;
        Self { bytes, requests }
    }

    /// Take over what another budget left unspent.
    fn absorb(&mut self, other: Self) {
        self.bytes += other.bytes;
        self.requests += other.requests;
    }

    /// Reserve exactly `n` operations, or nothing at all.
    ///
    /// A merge is indivisible — it either fetches every input pack or does not
    /// happen — so a partial grant must leave the budget untouched for whatever
    /// smaller work comes next, rather than being spent on a merge that then
    /// declines to run.
    fn reserve_requests(&mut self, n: usize) -> bool {
        if self.requests < n {
            return false;
        }
        self.requests -= n;
        true
    }
}

/// Encoded sizes of forward packs, memoized for one index cycle.
///
/// Sizes for packs written during this cycle are known exactly and inserted at
/// write time, so the common case costs no I/O at all. Only packs inherited
/// from earlier cycles are probed, and a probe reads 40 bytes.
type PackSizeCache = HashMap<ContentId, u64>;

/// Encoded byte lengths for `cids`, in order, stopping at the first that cannot
/// be established or that the request budget will not cover.
///
/// Sizes come from the cycle memo, then local file metadata, then a 40-byte
/// header range read — never a full fetch. Only reads that actually reach
/// storage are charged. A pack that cannot be sized truncates the candidate
/// region rather than failing the build: compaction is an optimization and must
/// never fail a publish.
async fn pack_encoded_sizes(
    content_store: &dyn ContentStore,
    cids: &[ContentId],
    cache: &mut PackSizeCache,
    budget: &mut CompactionBudget,
) -> Vec<u64> {
    use fluree_db_binary_index::dict::forward_pack::{
        pack_encoded_len_from_header, PACK_HEADER_SIZE,
    };

    let mut sizes = Vec::with_capacity(cids.len());

    // Serve memo hits and local packs without spending budget, and collect
    // whatever still needs a remote probe.
    let mut pending: Vec<(usize, &ContentId)> = Vec::new();
    for (i, cid) in cids.iter().enumerate() {
        if let Some(&size) = cache.get(cid) {
            sizes.push(Some(size));
        } else if let Ok(Some(bytes)) = content_store.get_local(cid) {
            let size = bytes.len() as u64;
            cache.insert(cid.clone(), size);
            sizes.push(Some(size));
        } else {
            sizes.push(None);
            pending.push((i, cid));
        }
    }

    let grant = budget.take_requests(pending.len());
    let pending = &pending[..grant];
    if !pending.is_empty() {
        let probes: Vec<_> = pending
            .iter()
            .map(|(_, cid)| content_store.get_range(cid, 0..PACK_HEADER_SIZE as u64))
            .collect();
        let headers: Vec<_> = stream::iter(probes)
            .buffered(COMPACTION_CONCURRENCY)
            .collect()
            .await;

        for ((i, cid), header) in pending.iter().zip(headers) {
            let Some(size) = header
                .ok()
                .and_then(|h| pack_encoded_len_from_header(&h).ok())
            else {
                continue;
            };
            cache.insert((*cid).clone(), size);
            sizes[*i] = Some(size);
        }
    }

    // Truncate at the first unknown: the planner needs a contiguous prefix.
    sizes.into_iter().map_while(|s| s).collect()
}

/// Append new terms to their inner predicates' pack streams and compact:
/// the streams that grew first, then the others in rotation (by `rotation`,
/// so no cursor is persisted), until `budget` runs out. Returns the routing
/// table and the packs a merge consumed (base packs no longer routed, and this
/// cycle's uploads a later merge absorbed), which are garbage.
async fn update_term_forward_packs(
    content_store: &dyn ContentStore,
    base: Option<&fluree_db_binary_index::TermDictRefs>,
    new_terms: &[(u32, u32, Vec<u8>)],
    rotation: u64,
    pack_sizes: &mut PackSizeCache,
    budget: &mut CompactionBudget,
) -> Result<(
    Vec<(u32, Vec<fluree_db_binary_index::PackBranchEntry>)>,
    Vec<ContentId>,
)> {
    use fluree_db_binary_index::dict::forward_pack::KIND_TERM_FWD;
    use fluree_db_binary_index::dict::incremental::build_incremental_packs_for_stream;
    use fluree_db_binary_index::dict::term_dict::pack_ns_code;
    use fluree_db_binary_index::PackBranchEntry;

    let kind = |p_id: u32| ContentKind::DictBlob {
        dict: fluree_db_core::DictKind::TermForward { p_id },
    };
    let mut forward_packs: Vec<(u32, Vec<PackBranchEntry>)> =
        base.map(|b| b.forward_packs.clone()).unwrap_or_default();
    let mut by_pred: std::collections::BTreeMap<u32, Vec<(u64, &[u8])>> =
        std::collections::BTreeMap::new();
    for (p_id, seq, key) in new_terms {
        by_pred
            .entry(*p_id)
            .or_default()
            .push((u64::from(*seq), key.as_slice()));
    }

    let mut uploaded: Vec<ContentId> = Vec::new();
    for (p_id, entries) in &by_pred {
        let existing: Vec<PackBranchEntry> = forward_packs
            .iter()
            .find(|(p, _)| p == p_id)
            .map(|(_, refs)| refs.clone())
            .unwrap_or_default();
        let pack_result = build_incremental_packs_for_stream(
            KIND_TERM_FWD,
            pack_ns_code(*p_id),
            &existing,
            entries,
        )
        .map_err(|e| IndexerError::StorageWrite(format!("term fwd pack build p_id={p_id}: {e}")))?;
        let mut updated = existing;
        for pack in &pack_result.new_packs {
            let pack_cid = content_store
                .put(kind(*p_id), &pack.bytes)
                .await
                .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
            pack_sizes.insert(pack_cid.clone(), pack.bytes.len() as u64);
            uploaded.push(pack_cid.clone());
            updated.push(PackBranchEntry {
                first_id: pack.first_id,
                last_id: pack.last_id,
                pack_cid,
            });
        }
        uploaded.extend(
            compact_forward_packs(
                content_store,
                kind(*p_id),
                &mut updated,
                pack_sizes,
                budget,
                CompactionSpans::default(),
                &format!("term p_id={p_id}"),
            )
            .await?,
        );
        if let Some(entry) = forward_packs.iter_mut().find(|(p, _)| p == p_id) {
            entry.1 = updated;
        } else {
            forward_packs.push((*p_id, updated));
        }
    }

    // Maintenance: a stream that went quiet keeps whatever fragmentation an
    // earlier cycle could not afford to merge.
    let mut quiet: Vec<u32> = forward_packs
        .iter()
        .filter(|(p, refs)| refs.len() > 1 && !by_pred.contains_key(p))
        .map(|(p, _)| *p)
        .collect();
    if !quiet.is_empty() {
        let offset = (rotation as usize) % quiet.len();
        quiet.rotate_left(offset);
    }
    for p_id in quiet {
        if budget.exhausted() {
            break;
        }
        let Some(entry) = forward_packs.iter_mut().find(|(p, _)| *p == p_id) else {
            continue;
        };
        uploaded.extend(
            compact_forward_packs(
                content_store,
                kind(p_id),
                &mut entry.1,
                pack_sizes,
                budget,
                CompactionSpans::default(),
                &format!("term p_id={p_id} (maintenance)"),
            )
            .await?,
        );
    }
    forward_packs.sort_by_key(|(p, _)| *p);

    let live: std::collections::HashSet<&ContentId> = forward_packs
        .iter()
        .flat_map(|(_, refs)| refs.iter().map(|r| &r.pack_cid))
        .collect();
    let mut consumed: Vec<ContentId> = base
        .iter()
        .flat_map(|b| b.forward_packs.iter())
        .flat_map(|(_, refs)| refs.iter().map(|r| r.pack_cid.clone()))
        .chain(uploaded)
        .filter(|cid| !live.contains(cid))
        .collect();
    consumed.sort();
    consumed.dedup();
    Ok((forward_packs, consumed))
}

/// Merge qualifying runs within one forward-dictionary stream, in place.
///
/// Examines the **tail** first and unconditionally, because new packs land
/// there and that window is what keeps the table bounded. Only then does it
/// sweep older regions for inherited fragmentation, with whatever budget
/// remains. Doing it the other way round lets a table larger than the scan
/// budget hide its own tail and grow without limit.
///
/// Returns the CIDs of packs this call uploaded, whether or not they survived
/// into `refs` — a cascade rewrites its own intermediate output, and the caller
/// must account for anything that did not survive (see `unreferenced_uploads`).
///
/// Compaction never fails a build. A pack that cannot be sized or fetched just
/// makes its region ineligible this cycle. A *structural* failure is different
/// — a merge that produced a malformed or mis-ranged pack means the inputs were
/// not what the routing table claimed, so that propagates.
async fn compact_forward_packs(
    content_store: &dyn ContentStore,
    kind: ContentKind,
    refs: &mut Vec<fluree_db_binary_index::PackBranchEntry>,
    cache: &mut PackSizeCache,
    budget: &mut CompactionBudget,
    spans: CompactionSpans,
    stream_label: &str,
) -> Result<Vec<ContentId>> {
    let mut uploaded = Vec::new();

    // Tail first, and repeatedly: each merge can expose another.
    while !budget.exhausted() {
        let start = refs.len().saturating_sub(spans.tail);
        if !compact_window(
            content_store,
            kind,
            refs,
            start,
            spans.tail,
            cache,
            budget,
            stream_label,
            &mut uploaded,
        )
        .await?
        {
            break;
        }
    }

    // Then sweep older regions. Progress is not persisted between cycles, so
    // this restarts at the head each time; the request budget is what stops a
    // long run of mature packs from being re-probed indefinitely.
    let mut cursor = 0usize;
    while !budget.exhausted() && cursor + 1 < refs.len().saturating_sub(spans.tail) {
        if compact_window(
            content_store,
            kind,
            refs,
            cursor,
            spans.head,
            cache,
            budget,
            stream_label,
            &mut uploaded,
        )
        .await?
        {
            continue; // merged here; try the same region again
        }
        cursor += spans.head;
    }

    Ok(uploaded)
}

/// Attempt one merge within `refs[start .. start + span]`. Returns whether a
/// merge happened.
#[allow(clippy::too_many_arguments)]
async fn compact_window(
    content_store: &dyn ContentStore,
    kind: ContentKind,
    refs: &mut Vec<fluree_db_binary_index::PackBranchEntry>,
    start: usize,
    span: usize,
    cache: &mut PackSizeCache,
    budget: &mut CompactionBudget,
    stream_label: &str,
    uploaded: &mut Vec<ContentId>,
) -> Result<bool> {
    use fluree_db_binary_index::dict::forward_pack::concat_forward_packs;
    use fluree_db_binary_index::dict::incremental::{plan_compaction, COMPACTION_MAX_OUTPUT_BYTES};

    let end = refs.len().min(start + span);
    if start + 1 >= end {
        return Ok(false);
    }

    let cids: Vec<ContentId> = refs[start..end]
        .iter()
        .map(|e| e.pack_cid.clone())
        .collect();
    let sizes = pack_encoded_sizes(content_store, &cids, cache, budget).await;

    let window = &refs[start..start + sizes.len()];
    let Some(plan) = plan_compaction(window, &sizes, COMPACTION_MAX_OUTPUT_BYTES, 0) else {
        return Ok(false);
    };
    let (merge_start, merge_end) = (start + plan.start, start + plan.end);

    if plan.input_bytes > budget.bytes {
        tracing::debug!(
            stream = stream_label,
            input_bytes = plan.input_bytes,
            bytes_remaining = budget.bytes,
            "forward pack compaction deferred: cycle byte budget exhausted"
        );
        budget.bytes = 0;
        return Ok(false);
    }

    let input_packs = merge_end - merge_start;
    if !budget.reserve_requests(input_packs) {
        tracing::debug!(
            stream = stream_label,
            input_packs,
            "forward pack compaction deferred: cycle request budget exhausted"
        );
        return Ok(false);
    }

    let started = Instant::now();
    let input_cids = &cids[plan.start..plan.end];
    let fetches: Vec<_> = input_cids
        .iter()
        .map(|cid| content_store.get(cid))
        .collect();
    let fetched: std::result::Result<Vec<ContentBytes>, fluree_db_core::error::Error> =
        stream::iter(fetches)
            .buffered(COMPACTION_CONCURRENCY)
            .try_collect()
            .await;

    let bytes = match fetched {
        Ok(b) => b,
        Err(e) => {
            tracing::debug!(
                stream = stream_label,
                error = %e,
                "forward pack compaction skipped: input fetch failed"
            );
            return Ok(false);
        }
    };

    let borrowed: Vec<&[u8]> = bytes.iter().map(|b| &b[..]).collect();
    let merged = concat_forward_packs(&borrowed).map_err(|e| {
        IndexerError::StorageWrite(format!("forward pack compaction ({stream_label}): {e}"))
    })?;
    drop(bytes);

    let output_bytes = merged.len() as u64;
    let first_id = refs[merge_start].first_id;
    let last_id = refs[merge_end - 1].last_id;

    let pack_cid = content_store
        .put(kind, &merged)
        .await
        .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;

    uploaded.push(pack_cid.clone());
    cache.insert(pack_cid.clone(), output_bytes);
    refs.splice(
        merge_start..merge_end,
        [fluree_db_binary_index::PackBranchEntry {
            first_id,
            last_id,
            pack_cid,
        }],
    );

    budget.bytes = budget.bytes.saturating_sub(plan.input_bytes);

    tracing::debug!(
        stream = stream_label,
        input_packs,
        input_bytes = plan.input_bytes,
        output_bytes,
        merged_at = merge_start,
        packs_remaining = refs.len(),
        bytes_remaining = budget.bytes,
        requests_remaining = budget.requests,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "forward packs compacted"
    );

    Ok(true)
}

/// Uploads from this cycle that no longer appear in the published routing
/// tables, and were not inherited from the base root.
///
/// `set_dict_refs` records garbage by diffing the base root against the new
/// one, which cannot see a pack that was created *and* consumed inside a single
/// cycle — a freshly appended pack that compaction immediately absorbed, or a
/// cascade's own intermediate output. Those are referenced by no root at all,
/// so without this they would sit in the store forever.
fn unreferenced_uploads(
    uploaded: &[ContentId],
    live: &fluree_db_binary_index::DictPackRefs,
    base: &fluree_db_binary_index::DictPackRefs,
) -> Vec<ContentId> {
    let referenced: std::collections::HashSet<&ContentId> = live
        .string_fwd_packs
        .iter()
        .chain(live.subject_fwd_ns_packs.iter().flat_map(|(_, v)| v.iter()))
        .chain(base.string_fwd_packs.iter())
        .chain(base.subject_fwd_ns_packs.iter().flat_map(|(_, v)| v.iter()))
        .map(|e| &e.pack_cid)
        .collect();

    uploaded
        .iter()
        .filter(|cid| !referenced.contains(cid))
        .cloned()
        .collect()
}

/// Upload ONE leaf (+ its sidecar) under a single permit from the global
/// upload budget, charge the per-leaflet fuel, seed the artifact cache, then
/// drop the bytes. The `blob` is consumed by value so its byte buffers are
/// freed the moment this future completes — they never accumulate.
///
/// The base per-write fuel is charged automatically by
/// [`crate::fuel::MeteredContentStore`] when `content_store` is the metered
/// wrapper; `tracker` here covers only the per-leaflet FLI3 portion.
async fn upload_one_leaf_blob(
    content_store: &dyn ContentStore,
    tracker: &fluree_db_core::tracking::Tracker,
    upload_budget: &Semaphore,
    blob: NewLeafBlob,
    warm_cache: Option<&Arc<LeafletCache>>,
) -> Result<LeafUploadCounts> {
    let _permit = upload_budget
        .acquire()
        .await
        .map_err(|e| IndexerError::StorageWrite(format!("upload budget closed: {e}")))?;

    let NewLeafBlob {
        info,
        leaflet_origins,
        replaced_leaf_cid,
    } = blob;
    let mut counts = LeafUploadCounts::default();

    let leaf_cid = content_store
        .put(ContentKind::IndexLeaf, &info.leaf_bytes)
        .await
        .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
    crate::fuel::charge_extra_leaflets(tracker, info.re_encoded_leaflet_count)?;
    debug_assert!(leaf_cid == info.leaf_cid);
    cache_artifact_bytes(content_store, &leaf_cid, &info.leaf_bytes, "index_leaf");
    counts.leaf_bytes = info.leaf_bytes.len() as u64;

    // Warm-on-write (co-located only): carry what readers had cached for the
    // replaced leaf over to the new one, so a query reading it after the swap
    // hits the cache. See `warm_replaced_leaflets`.
    if let (Some(cache), Some(replaced)) = (warm_cache, replaced_leaf_cid.as_ref()) {
        warm_replaced_leaflets(
            cache,
            replaced,
            &info.leaf_cid,
            info.leaf_bytes,
            &leaflet_origins,
        );
    }

    if let Some(sc_bytes) = info.sidecar_bytes.as_deref() {
        let sidecar_cid = content_store
            .put(ContentKind::HistorySidecar, sc_bytes)
            .await
            .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
        if let Some(expected) = &info.sidecar_cid {
            debug_assert!(&sidecar_cid == expected);
        }
        cache_artifact_bytes(content_store, &sidecar_cid, sc_bytes, "history_sidecar");
        counts.sidecar_bytes = sc_bytes.len() as u64;
        counts.sidecar_count = 1;
    }
    // `info`'s sidecar buffer drops here; `leaf_bytes` was moved into the warm
    // step above (or already dropped when no warm cache is configured).
    Ok(counts)
}

/// Warm-on-write: give a rewritten leaf the cache entries readers had for the
/// leaf it replaced, so a co-located query reading it after the swap hits the
/// cache instead of decoding from disk.
///
/// Only leaflets whose source leaflet was cached are warmed, with the columns
/// that were cached:
/// - a byte-copied leaflet decodes to the same columns, so it takes the cached
///   batch under its new key without a decode;
/// - a re-encoded leaflet is decoded from the bytes in hand, on a blocking
///   thread from a small pool (see [`warm_decode_permits`]). When the pool is
///   busy the leaf is skipped and its first reader decodes it, so a large
///   build cannot queue decode work, or fill the cache, beyond what readers
///   were using.
///
/// The leaf directory is seeded whenever a leaflet is.
fn warm_replaced_leaflets(
    cache: &Arc<LeafletCache>,
    replaced_cid: &ContentId,
    leaf_cid: &ContentId,
    leaf_bytes: Vec<u8>,
    origins: &[crate::run_index::LeafletOrigin],
) {
    use fluree_db_binary_index::V3BatchCacheKey;

    let replaced_id = LeafletCache::cid_cache_key(&replaced_cid.to_bytes());
    let leaf_id = LeafletCache::cid_cache_key(&leaf_cid.to_bytes());
    let mut copied = false;
    let mut to_decode = Vec::new();
    for (idx, origin) in origins.iter().enumerate() {
        let Some((columns, batch)) = cache.v3_leaflet_entry(replaced_id, origin.leaflet_idx) else {
            continue;
        };
        if origin.copied {
            cache.insert_v3_batch(
                V3BatchCacheKey {
                    leaf_id,
                    leaflet_idx: idx as u32,
                    columns: columns.0,
                },
                batch,
            );
            copied = true;
        } else {
            to_decode.push((idx, columns));
        }
    }
    if to_decode.is_empty() {
        if copied {
            warm_leaf_dir(cache, leaf_id, &leaf_bytes);
        }
        return;
    }
    let Ok(permit) = Arc::clone(warm_decode_permits()).try_acquire_owned() else {
        tracing::debug!(
            leaflets = to_decode.len(),
            "warm-on-write pool busy; leaving the leaf to its first reader"
        );
        if copied {
            warm_leaf_dir(cache, leaf_id, &leaf_bytes);
        }
        return;
    };
    let cache = Arc::clone(cache);
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        decode_into_cache(&cache, leaf_id, &leaf_bytes, &to_decode);
    });
}

/// Blocking threads warm-on-write may decode on at once: a quarter of the
/// cores, at least one.
fn warm_decode_permits() -> &'static Arc<Semaphore> {
    static PERMITS: std::sync::OnceLock<Arc<Semaphore>> = std::sync::OnceLock::new();
    PERMITS.get_or_init(|| {
        let cores = std::thread::available_parallelism().map_or(4, std::num::NonZeroUsize::get);
        Arc::new(Semaphore::new((cores / 4).max(1)))
    })
}

fn warm_leaf_dir(cache: &LeafletCache, leaf_id: u128, leaf_bytes: &[u8]) {
    use fluree_db_binary_index::format::leaf::{
        decode_leaf_dir_v3_with_base, decode_leaf_header_v3,
    };
    if let Ok(dir) =
        decode_leaf_header_v3(leaf_bytes).and_then(|h| decode_leaf_dir_v3_with_base(leaf_bytes, &h))
    {
        cache.insert_leaf_dir(leaf_id, Arc::new(dir));
    }
}

/// Decode `leaflets` (index, columns) of a just-written leaf into the cache.
/// Best-effort: a decode error leaves the reader to decode it as before.
fn decode_into_cache(
    cache: &LeafletCache,
    leaf_id: u128,
    leaf_bytes: &[u8],
    leaflets: &[(usize, fluree_db_binary_index::read::column_types::ColumnSet)],
) {
    use fluree_db_binary_index::format::leaf::{
        decode_leaf_dir_v3_with_base, decode_leaf_header_v3,
    };
    use fluree_db_binary_index::read::column_loader::load_leaflet_columns;
    use fluree_db_binary_index::read::column_types::{ColumnProjection, ColumnSet};
    use fluree_db_binary_index::V3BatchCacheKey;

    let Ok(header) = decode_leaf_header_v3(leaf_bytes) else {
        return;
    };
    let Ok(dir) = decode_leaf_dir_v3_with_base(leaf_bytes, &header) else {
        return;
    };
    for &(idx, columns) in leaflets {
        let Some(entry) = dir.entries.get(idx) else {
            continue;
        };
        if entry.row_count == 0 {
            continue;
        }
        let projection = ColumnProjection {
            output: columns,
            internal: ColumnSet::EMPTY,
        };
        if let Ok(batch) = load_leaflet_columns(
            leaf_bytes,
            entry,
            dir.payload_base,
            &projection,
            header.order,
        ) {
            cache.insert_v3_batch(
                V3BatchCacheKey {
                    leaf_id,
                    leaflet_idx: idx as u32,
                    columns: columns.0,
                },
                batch,
            );
        }
    }
    cache.insert_leaf_dir(leaf_id, Arc::new(dir));
}

/// Upload a fully-materialized set of leaf blobs (the fresh-build paths produce
/// the whole `Vec` up front) in parallel under the shared global budget,
/// consuming each blob by value so its bytes are freed after its put.
async fn upload_leaf_blobs(
    content_store: &dyn ContentStore,
    tracker: &fluree_db_core::tracking::Tracker,
    upload_budget: &Semaphore,
    upload_buffer: usize,
    blobs: Vec<NewLeafBlob>,
) -> Result<()> {
    let leaf_total = blobs.len();
    // The global semaphore is the real concurrency cap; bound the in-flight
    // future set so we don't materialize one pending future per leaf for a
    // large fresh build.
    let buffer = upload_buffer.max(1);
    let totals = stream::iter(blobs)
        .map(|blob| upload_one_leaf_blob(content_store, tracker, upload_budget, blob, None))
        .buffer_unordered(buffer)
        .try_fold(LeafUploadCounts::default(), |mut acc, c| async move {
            acc.leaf_bytes += c.leaf_bytes;
            acc.sidecar_bytes += c.sidecar_bytes;
            acc.sidecar_count += c.sidecar_count;
            Ok(acc)
        })
        .await?;
    tracing::debug!(
        leaves = leaf_total,
        leaf_bytes = totals.leaf_bytes,
        sidecars = totals.sidecar_count,
        sidecar_bytes = totals.sidecar_bytes,
        "V6 Phase 2 upload complete; offered artifacts to the local tier"
    );
    Ok(())
}

/// A pooled graph's persisted state: its rows (each the assert that made the
/// fact present) and its history transitions, together its whole log.
#[derive(Default)]
struct PooledGraphLog {
    rows: Vec<RunRecordV2>,
    history: Vec<(RunRecordV2, u8)>,
}

/// The persisted log of each of `graphs` in the pool, by graph. Empty (and
/// no store load) when `graphs` is.
async fn read_pooled_graph_logs(
    content_store: &Arc<dyn ContentStore>,
    base_root: &fluree_db_binary_index::IndexRoot,
    graphs: &[fluree_db_core::GraphId],
) -> Result<std::collections::HashMap<fluree_db_core::GraphId, PooledGraphLog>> {
    use fluree_db_binary_index::format::history_sidecar::{decode_sidecar_entries, HistEntryV2};
    use fluree_db_binary_index::format::run_record_v2::RunRecordV2 as Rec;
    use fluree_db_binary_index::read::binary_index_store::BinaryIndexStore;
    use fluree_db_binary_index::{BinaryCursor, BinaryFilter, ColumnProjection};

    let mut out = std::collections::HashMap::new();
    if graphs.is_empty() || base_root.pool.is_none() {
        return Ok(out);
    }
    let store = Arc::new(
        BinaryIndexStore::load_from_root_v6(content_store.clone(), base_root, None)
            .await
            .map_err(|e| {
                IndexerError::IncrementalAbort(format!("pooled graph promotion: store load: {e}"))
            })?,
    );
    // Sidecars hold every graph of their leaf; decode each once.
    let mut sidecars: std::collections::HashMap<ContentId, Vec<HistEntryV2>> =
        std::collections::HashMap::new();
    for &g_id in graphs {
        let Some(tree) = store.graph_tree(g_id, RunSortOrder::Spot) else {
            continue;
        };
        let bound = |low: bool| Rec {
            s_id: fluree_db_core::subject_id::SubjectId(if low { 0 } else { u64::MAX }),
            o_key: if low { 0 } else { u64::MAX },
            p_id: if low { 0 } else { u32::MAX },
            t: if low { 0 } else { u32::MAX },
            o_i: if low { 0 } else { u32::MAX },
            o_type: if low { 0 } else { u16::MAX },
            g_id,
        };
        let mut cursor = BinaryCursor::new(
            Arc::clone(&store),
            tree.order,
            Arc::clone(&tree.branch),
            &bound(true),
            &bound(false),
            BinaryFilter {
                g_id: tree.g_filter,
                ..Default::default()
            },
            ColumnProjection::all(),
        );
        let mut log = PooledGraphLog::default();
        while let Some(batch) = cursor
            .next_batch()
            .map_err(|e| IndexerError::StorageRead(format!("pooled graph promotion: {e}")))?
        {
            for i in 0..batch.row_count {
                log.rows.push(Rec {
                    s_id: fluree_db_core::subject_id::SubjectId(batch.s_id.get(i)),
                    o_key: batch.o_key.get(i),
                    p_id: batch.p_id.get_or(i, 0),
                    t: batch.t.get_or(i, 0),
                    o_i: batch.o_i.get_or(i, u32::MAX),
                    o_type: batch.o_type.get_or(i, 0),
                    g_id,
                });
            }
        }
        let leaves = tree.branch.find_leaves_in_range(
            &bound(true),
            &bound(false),
            cmp_v2_for_order(tree.order),
        );
        for leaf in &tree.branch.leaves[leaves] {
            let Some(cid) = &leaf.sidecar_cid else {
                continue;
            };
            if !sidecars.contains_key(cid) {
                let bytes = content_store.get(cid).await.map_err(|e| {
                    IndexerError::StorageRead(format!("pooled graph promotion: sidecar: {e}"))
                })?;
                let entries = decode_sidecar_entries(&bytes).map_err(|e| {
                    IndexerError::StorageRead(format!("pooled graph promotion: sidecar: {e}"))
                })?;
                sidecars.insert(cid.clone(), entries);
            }
            log.history
                .extend(sidecars[cid].iter().filter(|e| e.g_id == g_id).map(|e| {
                    (
                        Rec {
                            s_id: e.s_id,
                            o_key: e.o_key,
                            p_id: e.p_id,
                            t: e.t,
                            o_i: e.o_i,
                            o_type: e.o_type,
                            g_id,
                        },
                        e.op,
                    )
                }));
        }
        out.insert(g_id, log);
    }
    Ok(out)
}

/// Rows per leaf for a tree in `order`: the pool's own size for pooled orders.
fn leaf_rows_for(order: RunSortOrder, config: &IndexerConfig) -> usize {
    if order.is_pooled() {
        crate::run_index::build::graph_scope::pooled_leaf_rows(config.leaflet_rows)
    } else {
        config.leaf_rows
    }
}

/// Most delta runs a pooled secondary order keeps before compacting them:
/// loading a store fetches every run, and those fetches share the storage
/// client's request slots with every other ledger's reads.
const DELTA_MAX_RUNS: usize = 32;

/// Delta events a pooled secondary order may hold however small its base.
const DELTA_MIN_EVENTS: u64 = 16_000;

/// A delta may hold up to its base's live rows divided by this. A scan
/// merges every delta event in its range row by row, so the delta stays
/// small beside the rows it reads.
const DELTA_BASE_SHARE: u64 = 16;

/// Delta events a pooled secondary order holds at most, however large its
/// base: what a broad scan merges, and what readers keep in memory.
const DELTA_MAX_EVENTS: u64 = 64_000;

/// Whether a pooled secondary order should fold its delta runs, and this
/// window's events, into its base rather than append the window as a run:
/// at its run or size bound, or when a graph is leaving the pool, which only
/// the base can purge.
fn compact_delta(
    runs: &[fluree_db_binary_index::PoolDeltaRun],
    window_events: usize,
    base_rows: u64,
    promoting: bool,
) -> bool {
    let events: u64 = runs.iter().map(|r| u64::from(r.events)).sum::<u64>() + window_events as u64;
    let limit = (base_rows / DELTA_BASE_SHARE).clamp(DELTA_MIN_EVENTS, DELTA_MAX_EVENTS);
    promoting || runs.len() + 1 > DELTA_MAX_RUNS || events > limit
}

/// A window event as a delta run event.
fn delta_event(rec: RunRecordV2, op: u8) -> fluree_db_binary_index::OverlayOp {
    fluree_db_binary_index::OverlayOp {
        s_id: rec.s_id.as_u64(),
        p_id: rec.p_id,
        o_type: rec.o_type,
        o_key: rec.o_key,
        o_i: rec.o_i,
        t: i64::from(rec.t),
        op: op == 1,
        g_id: rec.g_id,
    }
}

/// A delta run event as a window event.
fn delta_record(e: fluree_db_binary_index::OverlayOp) -> (RunRecordV2, u8) {
    (
        RunRecordV2 {
            s_id: fluree_db_core::subject_id::SubjectId(e.s_id),
            o_key: e.o_key,
            p_id: e.p_id,
            t: u32::try_from(e.t).unwrap_or(u32::MAX),
            o_i: e.o_i,
            o_type: e.o_type,
            g_id: e.g_id,
        },
        u8::from(e.op),
    )
}

/// Window events: a record and its op.
type Events = Vec<(RunRecordV2, u8)>;

/// One window's pooled events grouped by fact, each group in time order.
fn by_fact(events: &[(RunRecordV2, u8)]) -> Vec<Events> {
    let mut sorted = events.to_vec();
    sorted.sort_unstable_by(|a, b| {
        fact_identity_key(&a.0)
            .cmp(&fact_identity_key(&b.0))
            .then(a.0.t.cmp(&b.0.t))
            .then(a.1.cmp(&b.1))
    });
    sorted
        .chunk_by(|a, b| fact_identity_key(&a.0) == fact_identity_key(&b.0))
        .map(<[_]>::to_vec)
        .collect()
}

/// A window's pooled events as a merge applies them from each fact's
/// presence at the window's start (`present`): the fact's transitions, plus
/// a reassertion of a present fact alone in the window, which the merge
/// records as replacing the row at its `t`. Events that change nothing are
/// dropped.
fn resolved_window_events(
    window: &[(RunRecordV2, u8)],
    present: impl Fn(&RunRecordV2) -> bool,
) -> Events {
    let mut out = Vec::with_capacity(window.len());
    for mut fact in by_fact(window) {
        let was_present = present(&fact[0].0);
        if was_present && matches!(fact.as_slice(), [(_, 1)]) {
            out.push(fact[0]);
            continue;
        }
        let row = fluree_db_binary_index::format::transitions::resolve_transitions(
            &mut fact,
            was_present,
        );
        out.extend(fact);
        out.extend(row.map(|rec| (rec, 1)));
    }
    out
}

/// One window's events ready to fold with other windows' into a single
/// merge: a fact's lone assert gains a retract at its `t`, so that over a
/// fact already present it replaces the row there, as the merge does when
/// it applies that window alone, and over an absent fact changes nothing.
fn with_replacements(window: Events) -> Events {
    let mut out = Vec::with_capacity(window.len());
    for fact in by_fact(&window) {
        if let [(rec, 1)] = fact.as_slice() {
            out.push((*rec, 0));
        }
        out.extend(fact);
    }
    out
}

/// Upload one pooled secondary order's delta run of `events`.
async fn write_delta_run(
    content_store: &dyn ContentStore,
    order: RunSortOrder,
    events: &[(RunRecordV2, u8)],
) -> Result<fluree_db_binary_index::PoolDeltaRun> {
    let mut ops: Vec<fluree_db_binary_index::OverlayOp> = events
        .iter()
        .map(|&(rec, op)| delta_event(rec, op))
        .collect();
    fluree_db_binary_index::read::types::sort_overlay_ops_stable(&mut ops, order);
    let bytes = fluree_db_binary_index::format::pool_delta::encode_delta_run(order, &ops)
        .map_err(|e| IndexerError::StorageWrite(format!("pool delta run: {e}")))?;
    // A run lives with the pool's leaves; it is reachable only from the
    // root, never from a branch.
    let cid = content_store
        .put(ContentKind::IndexLeaf, &bytes)
        .await
        .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
    tracing::debug!(
        ?order,
        events = ops.len(),
        bytes = bytes.len(),
        "pool delta run written"
    );
    Ok(fluree_db_binary_index::PoolDeltaRun {
        cid,
        events: ops.len() as u32,
    })
}

/// Split events into each fact's winner — its latest event — and the
/// earlier events it superseded, the shape the leaf merge consumes. A
/// same-`t` pair is a replacement (see [`with_replacements`]): its retract
/// comes first.
fn split_lifecycles(mut events: Events, order: RunSortOrder) -> (Events, Events) {
    let cmp = cmp_v2_for_order(order);
    let same = fluree_db_binary_index::format::run_record_v2::identity_v2_for_order(order);
    events.sort_by(|a, b| cmp(&a.0, &b.0).then(a.0.t.cmp(&b.0.t)).then(a.1.cmp(&b.1)));
    let (mut winners, mut earlier) = (Vec::new(), Vec::new());
    for (i, &event) in events.iter().enumerate() {
        match events.get(i + 1) {
            Some(next) if same(&event.0, &next.0) => earlier.push(event),
            _ => winners.push(event),
        }
    }
    (winners, earlier)
}

/// `base` grown by a window's winning ops and superseded events, estimated
/// before the merge resolves them: an assert adds a row, a retract removes
/// one and leaves two history entries (the row's assert and the retract),
/// and a superseded event leaves one.
fn window_size(base: GraphSize, ops: impl Iterator<Item = u8>, superseded: usize) -> GraphSize {
    let (mut asserts, mut retracts) = (0u64, 0u64);
    for op in ops {
        if op == 1 {
            asserts += 1;
        } else {
            retracts += 1;
        }
    }
    GraphSize {
        rows: (base.rows + asserts).saturating_sub(retracts),
        history: base.history + 2 * retracts + superseded as u64,
    }
}

/// One fact of a graph after this window: its row (absent when the fact's
/// final state is retracted) and the transitions its sidecar keeps.
struct FoldedFact {
    row: Option<RunRecordV2>,
    transitions: Vec<(RunRecordV2, u8)>,
}

impl FoldedFact {
    fn key(&self) -> &RunRecordV2 {
        self.row.as_ref().unwrap_or_else(|| &self.transitions[0].0)
    }
}

/// A graph's facts after this window: its persisted log (empty for a graph
/// with no tree yet) and the window's events, folded per fact from absent the
/// way a full rebuild folds the whole log. Also returns the persisted rows the
/// window touched — the base presence Phase 3b's stats deltas read.
fn fold_graph_log(
    base: &PooledGraphLog,
    window: impl IntoIterator<Item = (RunRecordV2, u8)>,
) -> (Vec<FoldedFact>, Vec<RunRecordV2>) {
    use fluree_db_binary_index::format::transitions::resolve_transitions;

    let mut events: std::collections::HashMap<FactIdentityKey, Vec<(RunRecordV2, u8)>> =
        std::collections::HashMap::new();
    let mut touched: std::collections::HashSet<FactIdentityKey> = std::collections::HashSet::new();
    for (rec, op) in window {
        let key = fact_identity_key(&rec);
        touched.insert(key);
        events.entry(key).or_default().push((rec, op));
    }
    let matched = base
        .rows
        .iter()
        .filter(|r| touched.contains(&fact_identity_key(r)))
        .copied()
        .collect();
    for row in &base.rows {
        events
            .entry(fact_identity_key(row))
            .or_default()
            .push((*row, 1));
    }
    for &(rec, op) in &base.history {
        events
            .entry(fact_identity_key(&rec))
            .or_default()
            .push((rec, op));
    }
    let facts = events
        .into_values()
        .filter_map(|mut evs| {
            let row = resolve_transitions(&mut evs, false);
            (row.is_some() || !evs.is_empty()).then_some(FoldedFact {
                row,
                transitions: evs,
            })
        })
        .collect();
    (facts, matched)
}

/// A tree for `order` holding folded facts, rows and history.
fn build_folded_tree(
    facts: &[FoldedFact],
    order: RunSortOrder,
    g_id: fluree_db_core::GraphId,
    config: &IndexerConfig,
) -> Result<BranchUpdateResult> {
    use fluree_db_binary_index::format::history_sidecar::HistEntryV2;
    use fluree_db_binary_index::format::leaf::LeafWriter;

    let cmp = cmp_v2_for_order(order);
    let mut by_key: Vec<&FoldedFact> = facts.iter().collect();
    by_key.sort_unstable_by(|a, b| cmp(a.key(), b.key()));
    let mut writer = LeafWriter::new(
        order,
        config.leaflet_rows.max(1),
        leaf_rows_for(order, config),
        1,
    );
    for fact in by_key {
        for &(rec, op) in &fact.transitions {
            writer.push_history_entry(HistEntryV2 {
                s_id: rec.s_id,
                p_id: rec.p_id,
                o_type: rec.o_type,
                o_key: rec.o_key,
                o_i: rec.o_i,
                t: rec.t,
                op,
                g_id: rec.g_id,
            });
        }
        if let Some(row) = fact.row {
            writer
                .push_record(row)
                .map_err(|e| IndexerError::StorageWrite(format!("folded tree writer: {e}")))?;
        }
    }
    finish_fresh_branch(writer, order, g_id)
}

/// Finish a fresh branch's leaf writer into its leaves and branch manifest.
fn finish_fresh_branch(
    mut writer: fluree_db_binary_index::format::leaf::LeafWriter,
    order: RunSortOrder,
    g_id: fluree_db_core::GraphId,
) -> Result<BranchUpdateResult> {
    use crate::run_index::build::incremental_leaf::NewLeafBlob;
    use fluree_db_binary_index::format::branch::{build_branch_bytes, LeafEntry};
    use fluree_db_binary_index::format::run_record_v2::read_ordered_key_v2;

    let graph_deltas = writer
        .take_pool_members()
        .into_iter()
        .map(|m| {
            (
                m.g_id,
                GraphDelta {
                    rows: i64::from(m.rows),
                    history: i64::from(m.history),
                },
            )
        })
        .collect();
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

        new_blobs.push(NewLeafBlob::fresh(info));
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
        matched: Vec::new(),
        graph_deltas,
    })
}

/// Refuse a base root in which a graph has base facts in some of the four
/// orders but not all of them.
///
/// A graph's four orders hold one fact set, so they have facts in all of them
/// or none. An order without them takes the fresh path and is built from this
/// window's novelty alone, without the graph's base facts. Aborting falls back
/// to a full rebuild, which builds every order from the commits.
///
/// An order with no leaves holds no facts, the same as a missing one. Bulk
/// import used to write such an order: a SPOT with no leaves for a graph it had
/// no quads for, such as the default graph of a TriG file whose quads are all
/// in named graphs, or a named graph from an empty `GRAPH` block. An
/// incremental build then used to leave that SPOT empty while writing the
/// window's facts to the other three orders, which this refuses.
///
/// The default graph's leaves are inline in the root. A named graph's are in
/// its branches, but a branch with no leaves is exactly
/// `build_branch_bytes(order, g_id, &[])` (the decoder reads one branch
/// version, and every writer builds branches with that function), so its CID
/// identifies it without a fetch. That costs a hash per branch, about as much
/// as decoding the root, so it's done only for a named graph with some orders
/// but not all, like the older import's. One with all four is taken as having
/// facts in all four here, and [`check_base_facts_agree`] checks the graphs
/// this window writes to once Phase 2 has fetched their branches.
fn check_order_sets(
    default_graph_orders: &[fluree_db_binary_index::format::index_root::DefaultGraphOrder],
    named_graphs: &[fluree_db_binary_index::format::index_root::NamedGraphRouting],
) -> Result<()> {
    use fluree_db_binary_index::format::branch::build_branch_bytes;
    let mut all = RunSortOrder::all_build_orders().to_vec();
    all.sort_unstable();
    let complete = |mut orders: Vec<RunSortOrder>| {
        orders.sort_unstable();
        orders.dedup();
        orders.is_empty() || orders == all
    };
    let default_orders = default_graph_orders
        .iter()
        .filter(|o| !o.leaves.is_empty())
        .map(|o| o.order)
        .collect();
    let named_complete = |ng: &fluree_db_binary_index::format::index_root::NamedGraphRouting| {
        complete(ng.orders.iter().map(|(order, _)| *order).collect())
            || complete(
                ng.orders
                    .iter()
                    .filter(|(order, cid)| {
                        *cid != ContentId::new(
                            ContentKind::IndexBranch,
                            &build_branch_bytes(*order, ng.g_id, &[]),
                        )
                    })
                    .map(|(order, _)| *order)
                    .collect(),
            )
    };
    let partial = if complete(default_orders) {
        named_graphs
            .iter()
            .find(|ng| !named_complete(ng))
            .map(|ng| ng.g_id)
    } else {
        Some(0)
    };
    match partial {
        Some(g_id) => Err(IndexerError::IncrementalAbort(format!(
            "base root has facts in some but not all sort orders for graph {g_id}"
        ))),
        None => Ok(()),
    }
}

/// Refuse a window in which a graph's four tasks disagree on whether the base
/// holds facts for it: some merged into a branch with leaves while others
/// wrote a folded tree. The pool is not checked here.
///
/// This is [`check_order_sets`] for a named graph with all four orders, which
/// it leaves alone: whether each branch has leaves is known here, once Phase 2
/// has fetched it, at no extra cost. An incremental build used to leave such a
/// graph a SPOT with no leaves beside three orders that hold its facts, so the
/// graph's next write is refused and the full rebuild restores SPOT; until then
/// it's carried as it is, as an untouched graph always is. It runs after the
/// tasks, so an abort here leaves this build's uploads unreferenced, as an
/// abort in Phase 3 does.
fn check_base_facts_agree(outputs: &[Phase2TaskOutput]) -> Result<()> {
    let mut base_facts: std::collections::BTreeMap<fluree_db_core::GraphId, bool> =
        std::collections::BTreeMap::new();
    for output in outputs {
        // The pool's secondary orders take writes as delta runs, not tasks.
        let TreeTarget::Graph(g_id) = output.target else {
            continue;
        };
        if *base_facts.entry(g_id).or_insert(output.base_facts) != output.base_facts {
            return Err(IndexerError::IncrementalAbort(format!(
                "base root has facts in some but not all sort orders for graph {g_id}"
            )));
        }
    }
    Ok(())
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

#[cfg(test)]
mod placement_tests {
    use super::*;

    #[test]
    fn a_delta_compacts_at_its_run_size_or_promotion_bound() {
        let run = |events| fluree_db_binary_index::PoolDeltaRun {
            cid: ContentId::new(ContentKind::IndexLeaf, b"run"),
            events,
        };
        let runs = vec![run(10); DELTA_MAX_RUNS - 1];
        assert!(!compact_delta(&runs, 10, 0, false));
        assert!(compact_delta(&runs, 10, 0, true), "a graph leaving");
        assert!(
            compact_delta(&[runs.clone(), vec![run(10)]].concat(), 10, 0, false),
            "runs"
        );

        let below = (DELTA_MIN_EVENTS - 10) as usize;
        assert!(!compact_delta(&[run(10)], below, 0, false));
        assert!(compact_delta(&[run(11)], below, 0, false), "small base");
        let base = DELTA_BASE_SHARE * (DELTA_MIN_EVENTS + 100);
        assert!(
            !compact_delta(&[run(110)], below, base, false),
            "base share"
        );
        assert!(compact_delta(&[run(111)], below, base, false));
        let huge = u64::MAX / 2;
        assert!(!compact_delta(&[], DELTA_MAX_EVENTS as usize, huge, false));
        assert!(
            compact_delta(&[run(1)], DELTA_MAX_EVENTS as usize, huge, false),
            "absolute"
        );
    }

    #[test]
    fn a_window_grows_a_graph_by_its_net_rows_and_its_transitions() {
        let base = GraphSize {
            rows: 10,
            history: 4,
        };
        // Two asserts, one retract (its row's assert and the retract become
        // history), one superseded event.
        assert_eq!(
            window_size(base, [1, 1, 0].into_iter(), 1),
            GraphSize {
                rows: 11,
                history: 7
            }
        );
        assert_eq!(
            window_size(GraphSize::default(), [0, 0].into_iter(), 0),
            GraphSize {
                rows: 0,
                history: 4
            },
            "rows never go negative"
        );
    }
}

#[cfg(test)]
mod compaction_tests {
    use super::*;
    use fluree_db_binary_index::dict::forward_pack::{encode_forward_pack, KIND_STRING_FWD};
    use fluree_db_binary_index::PackBranchEntry;
    use fluree_db_core::storage::content_store_for;
    use fluree_db_core::MemoryStorage;

    const LEDGER: &str = "test:main";

    fn dict_kind() -> ContentKind {
        ContentKind::DictBlob {
            dict: fluree_db_core::DictKind::StringForward,
        }
    }

    /// Upload one small pack covering `[first_id, first_id + count)`.
    async fn put_pack(store: &dyn ContentStore, first_id: u64, count: usize) -> PackBranchEntry {
        let owned: Vec<(u64, Vec<u8>)> = (0..count)
            .map(|i| {
                let id = first_id + i as u64;
                (id, format!("value_{id}").into_bytes())
            })
            .collect();
        let refs: Vec<(u64, &[u8])> = owned.iter().map(|(id, v)| (*id, v.as_slice())).collect();
        let bytes = encode_forward_pack(&refs, KIND_STRING_FWD, 0, 512).unwrap();
        let pack_cid = store.put(dict_kind(), &bytes).await.unwrap();
        PackBranchEntry {
            first_id,
            last_id: first_id + count as u64 - 1,
            pack_cid,
        }
    }

    /// A prefix of packs separated by ID gaps (so no run through it can ever
    /// merge), followed by a contiguous run of mergeable packs at the tail.
    async fn table_with_unmergeable_prefix(
        store: &dyn ContentStore,
        prefix: usize,
        tail: usize,
    ) -> Vec<PackBranchEntry> {
        let mut refs = Vec::new();
        let mut next = 0u64;
        for _ in 0..prefix {
            refs.push(put_pack(store, next, 2).await);
            next += 10; // leave a hole so this pack cannot join its neighbours
        }
        for _ in 0..tail {
            refs.push(put_pack(store, next, 2).await);
            next += 2; // contiguous
        }
        refs
    }

    /// Upload one small term pack of predicate `p_id` covering
    /// `[first_seq, first_seq + count)`.
    async fn put_term_pack(
        store: &dyn ContentStore,
        p_id: u32,
        first_seq: u64,
        count: usize,
    ) -> PackBranchEntry {
        use fluree_db_binary_index::dict::forward_pack::KIND_TERM_FWD;
        use fluree_db_binary_index::dict::term_dict::pack_ns_code;
        let owned: Vec<(u64, Vec<u8>)> = (0..count)
            .map(|i| (first_seq + i as u64, vec![i as u8; 22]))
            .collect();
        let refs: Vec<(u64, &[u8])> = owned.iter().map(|(id, v)| (*id, v.as_slice())).collect();
        let bytes = encode_forward_pack(&refs, KIND_TERM_FWD, pack_ns_code(p_id), 512).unwrap();
        let kind = ContentKind::DictBlob {
            dict: fluree_db_core::DictKind::TermForward { p_id },
        };
        PackBranchEntry {
            first_id: first_seq,
            last_id: first_seq + count as u64 - 1,
            pack_cid: store.put(kind, &bytes).await.unwrap(),
        }
    }

    /// A term stream fragmented by earlier cycles is compacted in a cycle
    /// that brings it no terms, from the term dictionary's reserved share
    /// even when the other dictionaries spent the rest of the budget.
    #[tokio::test]
    async fn quiet_term_streams_are_compacted_from_their_share() {
        let storage = MemoryStorage::new();
        let store = content_store_for(
            storage.clone(),
            &fluree_db_core::StorageNamespace::parse_legacy(LEDGER).unwrap(),
        );
        let mut fragmented = Vec::new();
        for i in 0..9u64 {
            fragmented.push(put_term_pack(&store, 7, i * 3, 3).await);
        }
        let single = vec![put_term_pack(&store, 9, 0, 3).await];
        let base = fluree_db_binary_index::TermDictRefs {
            forward_packs: vec![(7, fragmented.clone()), (9, single.clone())],
            reverse: fluree_db_binary_index::DictTreeRefs {
                branch: fragmented[0].pack_cid.clone(),
                leaves: Vec::new(),
            },
            watermarks: vec![(7, 26), (9, 2)],
            term_count: 30,
            object_reverse: None,
        };

        let mut cycle = CompactionBudget::new();
        let mut terms = cycle.split_off(3);
        cycle.requests = 0;
        terms.absorb(cycle);

        let (packs, consumed) = update_term_forward_packs(
            &store,
            Some(&base),
            &[],
            42,
            &mut PackSizeCache::new(),
            &mut terms,
        )
        .await
        .expect("maintenance");
        let stream = |p: u32| &packs.iter().find(|(q, _)| *q == p).unwrap().1;
        assert!(stream(7).len() < fragmented.len(), "{:?}", stream(7));
        assert_eq!(stream(7).first().unwrap().first_id, 0);
        assert_eq!(stream(7).last().unwrap().last_id, 26);
        assert_eq!(stream(9), &single, "a single-pack stream is left alone");
        assert!(!consumed.is_empty());
        assert!(
            consumed
                .iter()
                .all(|cid| fragmented.iter().any(|e| &e.pack_cid == cid)),
            "only merged-away packs are garbage: {consumed:?}"
        );
    }

    #[tokio::test]
    async fn tail_is_compacted_even_when_the_scan_budget_cannot_reach_it() {
        // New packs always land at the tail, so a design that sweeps from the
        // head and runs out of budget first would never see them and the table
        // would grow without limit. Budget here is far too small to walk the
        // prefix, so the tail is only reachable if it is examined first.
        let storage = MemoryStorage::new();
        let store = content_store_for(
            storage.clone(),
            &fluree_db_core::StorageNamespace::parse_legacy(LEDGER).unwrap(),
        );
        let mut refs = table_with_unmergeable_prefix(&store, 40, 10).await;
        let before = refs.len();

        let mut cache = PackSizeCache::new();
        let mut budget = CompactionBudget {
            bytes: COMPACTION_BUDGET_BYTES,
            requests: 24,
        };

        let uploaded = compact_forward_packs(
            &store,
            dict_kind(),
            &mut refs,
            &mut cache,
            &mut budget,
            CompactionSpans { tail: 8, head: 16 },
            "test",
        )
        .await
        .expect("compaction");

        assert!(
            refs.len() < before,
            "tail was never compacted: {before} packs before, {} after",
            refs.len()
        );
        assert!(!uploaded.is_empty(), "a merge must upload its output");

        // The merge must have happened at the tail, not in the prefix.
        let last = refs.last().unwrap();
        assert_eq!(last.last_id, 40 * 10 + 10 * 2 - 1);
        assert!(
            last.last_id - last.first_id + 1 > 2,
            "the final entry should span several merged packs"
        );
    }

    #[tokio::test]
    async fn every_id_survives_a_driver_merge() {
        let storage = MemoryStorage::new();
        let store = content_store_for(
            storage.clone(),
            &fluree_db_core::StorageNamespace::parse_legacy(LEDGER).unwrap(),
        );
        let mut refs = table_with_unmergeable_prefix(&store, 2, 12).await;

        let mut cache = PackSizeCache::new();
        let mut budget = CompactionBudget::new();
        compact_forward_packs(
            &store,
            dict_kind(),
            &mut refs,
            &mut cache,
            &mut budget,
            CompactionSpans { tail: 8, head: 16 },
            "test",
        )
        .await
        .expect("compaction");

        for entry in &refs {
            let bytes = store.get(&entry.pack_cid).await.expect("pack bytes");
            let pack = fluree_db_binary_index::dict::forward_pack::ForwardPack::from_bytes(&bytes)
                .expect("valid pack");
            assert_eq!(pack.header().first_id, entry.first_id);
            assert_eq!(pack.header().last_id, entry.last_id);
            for id in entry.first_id..=entry.last_id {
                assert_eq!(
                    pack.lookup_str(id).unwrap(),
                    Some(format!("value_{id}")),
                    "id {id} lost across a merge"
                );
            }
        }
    }

    #[tokio::test]
    async fn a_disabled_budget_merges_nothing() {
        // The off-switch has to be a true no-op: packs are appended and never
        // merged, exactly as before compaction existed.
        let storage = MemoryStorage::new();
        let store = content_store_for(
            storage.clone(),
            &fluree_db_core::StorageNamespace::parse_legacy(LEDGER).unwrap(),
        );
        let mut refs = table_with_unmergeable_prefix(&store, 0, 20).await;
        let before = refs.clone();

        let uploaded = compact_forward_packs(
            &store,
            dict_kind(),
            &mut refs,
            &mut PackSizeCache::new(),
            &mut CompactionBudget::disabled(),
            CompactionSpans::default(),
            "test",
        )
        .await
        .expect("compaction");

        assert!(uploaded.is_empty(), "a disabled budget must upload nothing");
        assert_eq!(refs.len(), before.len(), "routing table must be untouched");
        assert!(refs
            .iter()
            .zip(&before)
            .all(|(a, b)| a.pack_cid == b.pack_cid));
    }

    #[tokio::test]
    async fn the_request_budget_is_shared_and_charged() {
        // One stream must not be able to spend a fresh allowance: the counter
        // is cycle-wide, and every probe and fetch that reaches storage is
        // charged against it.
        let storage = MemoryStorage::new();
        let store = content_store_for(
            storage.clone(),
            &fluree_db_core::StorageNamespace::parse_legacy(LEDGER).unwrap(),
        );
        let mut refs = table_with_unmergeable_prefix(&store, 4, 20).await;

        let mut cache = PackSizeCache::new();
        let mut budget = CompactionBudget {
            bytes: COMPACTION_BUDGET_BYTES,
            requests: 64,
        };
        compact_forward_packs(
            &store,
            dict_kind(),
            &mut refs,
            &mut cache,
            &mut budget,
            CompactionSpans { tail: 8, head: 16 },
            "first",
        )
        .await
        .expect("compaction");

        assert!(
            budget.requests < 64,
            "storage operations were not charged to the shared budget"
        );
    }
}

#[cfg(test)]
mod warm_tests {
    use super::*;
    use crate::run_index::{update_leaf, LeafUpdateInput};
    use fluree_db_binary_index::format::leaf::{
        decode_leaf_dir_v3_with_base, decode_leaf_header_v3, LeafWriter,
    };
    use fluree_db_binary_index::read::column_loader::load_leaflet_columns;
    use fluree_db_binary_index::read::column_types::{ColumnProjection, ColumnSet};
    use fluree_db_binary_index::V3BatchCacheKey;
    use fluree_db_core::o_type::OType;
    use fluree_db_core::subject_id::SubjectId;
    use fluree_db_core::value_id::ObjKey;

    fn record(s_id: u64, t: u32) -> RunRecordV2 {
        RunRecordV2 {
            s_id: SubjectId(s_id),
            o_key: ObjKey::encode_i64(0).as_u64(),
            p_id: 1,
            t,
            o_i: u32::MAX,
            o_type: OType::XSD_INTEGER.as_u16(),
            g_id: 0,
        }
    }

    /// A folded tree materializes only facts whose window ends asserted;
    /// a fact asserted then retracted leaves its two transitions in the
    /// sidecar and no row.
    #[test]
    fn folded_tree_writes_no_row_for_a_retracted_fact() {
        // s=2 asserted at t=2; s=4 asserted at t=2, then retracted at t=3.
        let window = [(record(2, 2), 1), (record(4, 3), 0), (record(4, 2), 1)];
        let (facts, _) = fold_graph_log(&PooledGraphLog::default(), window);
        let tree = build_folded_tree(&facts, RunSortOrder::Spot, 0, &IndexerConfig::small())
            .expect("folded tree");
        let rows: u64 = tree.leaf_entries.iter().map(|e| e.row_count).sum();
        assert_eq!(rows, 1, "only the asserted fact is a row");
        let sidecars = tree
            .new_leaf_blobs
            .iter()
            .filter(|b| b.info.sidecar_bytes.is_some())
            .count();
        assert_eq!(sidecars, 1, "the retracted fact's history is kept");
    }

    /// A graph's task whose facts net to no row and no history publishes no
    /// tree: a branch with no leaves would route no later novelty.
    #[tokio::test]
    async fn fresh_task_with_nothing_to_hold_publishes_no_branch() {
        let store: Arc<dyn fluree_db_core::storage::ContentStore> =
            Arc::new(fluree_db_core::storage::content_store_for(
                fluree_db_core::MemoryStorage::new(),
                &fluree_db_core::StorageNamespace::parse_legacy("test/fresh-empty:main").unwrap(),
            ));
        // A retraction of a fact that was never asserted: no transition.
        let folded = || Phase2TaskKind::Folded {
            facts: Arc::new(fold_graph_log(&PooledGraphLog::default(), [(record(2, 2), 0)]).0),
        };
        for (g_id, kind) in [
            (0, folded()),
            (3, folded()),
            (0, Phase2TaskKind::DefaultExisting { leaves: Vec::new() }),
        ] {
            let task = Phase2Task {
                seq: 0,
                target: TreeTarget::Graph(g_id),
                purge: Arc::new(Vec::new()),
                order: RunSortOrder::Spot,
                sorted_records: vec![record(2, 2)],
                sorted_ops: vec![0],
                sorted_superseded: Vec::new(),
                sorted_superseded_ops: Vec::new(),
                kind,
            };
            let output = execute_phase2_task(
                task,
                &IndexerConfig::small(),
                Arc::clone(&store),
                fluree_db_core::tracking::Tracker::disabled(),
                Arc::new(Semaphore::new(4)),
                4,
            )
            .await
            .expect("phase 2 task");
            assert!(
                matches!(output.update, Phase2TaskUpdate::Unchanged),
                "g_id={g_id}: an empty tree must not be published"
            );
        }
    }

    /// A branch with no leaves holds no base facts, so its novelty is built
    /// fresh, for the default graph and for a named graph alike.
    #[tokio::test]
    async fn leafless_branch_is_built_fresh() {
        use fluree_db_binary_index::format::branch::{build_branch_bytes, read_branch_from_bytes};
        let store: Arc<dyn fluree_db_core::storage::ContentStore> =
            Arc::new(fluree_db_core::storage::content_store_for(
                fluree_db_core::MemoryStorage::new(),
                &fluree_db_core::StorageNamespace::parse_legacy("test/leafless:main").unwrap(),
            ));
        let leafless = build_branch_bytes(RunSortOrder::Spot, 3, &[]);
        let leafless_cid = store
            .put(ContentKind::IndexBranch, &leafless)
            .await
            .expect("put leafless branch");
        let kinds = [
            (0, Phase2TaskKind::DefaultExisting { leaves: Vec::new() }),
            (
                3,
                Phase2TaskKind::NamedExisting {
                    branch_cid: leafless_cid,
                },
            ),
        ];
        for (g_id, kind) in kinds {
            let task = Phase2Task {
                seq: 0,
                target: TreeTarget::Graph(g_id),
                purge: Arc::new(Vec::new()),
                order: RunSortOrder::Spot,
                sorted_records: vec![record(2, 2)],
                sorted_ops: vec![1],
                sorted_superseded: Vec::new(),
                sorted_superseded_ops: Vec::new(),
                kind,
            };
            let output = execute_phase2_task(
                task,
                &IndexerConfig::small(),
                Arc::clone(&store),
                fluree_db_core::tracking::Tracker::disabled(),
                Arc::new(Semaphore::new(4)),
                4,
            )
            .await
            .unwrap_or_else(|e| panic!("g_id={g_id}: {e}"));
            assert!(
                !output.base_facts,
                "g_id={g_id}: a branch with no leaves holds no base facts"
            );
            let rows: u64 = match output.update {
                Phase2TaskUpdate::Default { leaf_entries } => {
                    leaf_entries.iter().map(|e| e.row_count).sum()
                }
                Phase2TaskUpdate::Branch { branch_cid } => {
                    let bytes = store.get(&branch_cid).await.expect("new branch");
                    read_branch_from_bytes(&bytes)
                        .expect("decode branch")
                        .leaves
                        .iter()
                        .map(|e| e.row_count)
                        .sum()
                }
                Phase2TaskUpdate::Unchanged => 0,
            };
            assert_eq!(rows, 1, "g_id={g_id}: the asserted fact must land");
        }
    }

    /// Every graph in a base root has facts in all four orders or none, and an
    /// order with no leaves has none.
    #[test]
    fn partial_order_root_is_refused() {
        use fluree_db_binary_index::format::branch::{build_branch_bytes, LeafEntry};
        use fluree_db_binary_index::format::index_root::{DefaultGraphOrder, NamedGraphRouting};
        let leaf = LeafEntry {
            first_key: record(1, 1),
            last_key: record(1, 1),
            row_count: 1,
            leaf_cid: ContentId::new(ContentKind::IndexLeaf, b"leaf"),
            sidecar_cid: None,
        };
        // Each order with `leaves` leaves.
        let default = |orders: &[(RunSortOrder, usize)]| -> Vec<DefaultGraphOrder> {
            orders
                .iter()
                .map(|&(order, leaves)| DefaultGraphOrder {
                    order,
                    leaves: vec![leaf.clone(); leaves],
                })
                .collect()
        };
        // Each order with a branch that has leaves (`true`) or none (`false`),
        // the latter written as every writer writes one.
        let named = |orders: &[(RunSortOrder, bool)]| NamedGraphRouting {
            g_id: 3,
            orders: orders
                .iter()
                .map(|&(order, leaves)| {
                    let bytes = if leaves {
                        b"branch".to_vec()
                    } else {
                        build_branch_bytes(order, 3, &[])
                    };
                    (order, ContentId::new(ContentKind::IndexBranch, &bytes))
                })
                .collect(),
        };
        let [spot, psot, post, opst] = [
            RunSortOrder::Spot,
            RunSortOrder::Psot,
            RunSortOrder::Post,
            RunSortOrder::Opst,
        ];

        for (defaults, named_graphs) in [
            (default(&[]), vec![]),
            (
                default(&[(spot, 1), (psot, 1), (post, 1), (opst, 1)]),
                vec![named(&[
                    (spot, true),
                    (psot, true),
                    (post, true),
                    (opst, true),
                ])],
            ),
            // No facts anywhere: what bulk import wrote for a graph it had no
            // quads for, or a rebuild for a graph that netted to nothing.
            (default(&[(spot, 0)]), vec![named(&[(spot, false)])]),
            (
                default(&[(spot, 0), (psot, 0), (post, 0), (opst, 0)]),
                vec![named(&[
                    (spot, false),
                    (psot, false),
                    (post, false),
                    (opst, false),
                ])],
            ),
            // A named graph with all four orders is left to
            // `check_base_facts_agree`, which sees its branches' leaves.
            (
                default(&[]),
                vec![named(&[
                    (spot, false),
                    (psot, true),
                    (post, true),
                    (opst, true),
                ])],
            ),
        ] {
            assert!(check_order_sets(&defaults, &named_graphs).is_ok());
        }
        for (defaults, named_graphs) in [
            (default(&[(spot, 1), (psot, 1)]), vec![]),
            // SPOT left with no leaves while the window's facts went to the
            // other three orders.
            (
                default(&[(spot, 0), (psot, 1), (post, 1), (opst, 1)]),
                vec![],
            ),
            (default(&[]), vec![named(&[(spot, true), (psot, true)])]),
            (default(&[]), vec![named(&[(spot, false), (psot, true)])]),
        ] {
            assert!(matches!(
                check_order_sets(&defaults, &named_graphs),
                Err(IndexerError::IncrementalAbort(_))
            ));
        }
    }

    /// A graph whose tasks disagree on whether the base holds its facts is an
    /// abort; agreement either way is fine.
    #[test]
    fn tasks_that_disagree_on_base_facts_are_refused() {
        let output = |target: TreeTarget, base_facts: bool| Phase2TaskOutput {
            seq: 0,
            target,
            order: RunSortOrder::Spot,
            base_facts,
            update: Phase2TaskUpdate::Unchanged,
            replaced_leaf_cids: Vec::new(),
            replaced_sidecar_cids: Vec::new(),
            new_leaf_count: 0,
            fetch_stats: Phase2FetchStatsSnapshot::default(),
            matched: Vec::new(),
            graph_deltas: GraphDeltas::new(),
        };
        let graph = |g_id, base_facts| output(TreeTarget::Graph(g_id), base_facts);
        assert!(check_base_facts_agree(&[
            graph(0, true),
            graph(0, true),
            graph(3, false),
            graph(3, false),
            // The pool's orders are not one task each, and are not checked.
            output(TreeTarget::Pool, true),
            output(TreeTarget::Pool, false),
        ])
        .is_ok());
        assert!(matches!(
            check_base_facts_agree(&[graph(0, true), graph(3, false), graph(3, true)]),
            Err(IndexerError::IncrementalAbort(_))
        ));
    }

    fn cache_leaflet(
        cache: &LeafletCache,
        leaf_cid: &ContentId,
        leaf_bytes: &[u8],
        leaflet_idx: u32,
        columns: ColumnSet,
    ) {
        let header = decode_leaf_header_v3(leaf_bytes).unwrap();
        let dir = decode_leaf_dir_v3_with_base(leaf_bytes, &header).unwrap();
        let projection = ColumnProjection {
            output: columns,
            internal: ColumnSet::EMPTY,
        };
        let batch = load_leaflet_columns(
            leaf_bytes,
            &dir.entries[leaflet_idx as usize],
            dir.payload_base,
            &projection,
            header.order,
        )
        .unwrap();
        let leaf_id = LeafletCache::cid_cache_key(&leaf_cid.to_bytes());
        cache.insert_v3_batch(
            V3BatchCacheKey {
                leaf_id,
                leaflet_idx,
                columns: columns.0,
            },
            batch,
        );
    }

    /// A rewritten leaf gets only what readers had cached for the leaf it
    /// replaced: a copied leaflet takes its cached batch and columns without a
    /// decode, a re-encoded one is decoded for the cached columns, and a
    /// leaflet no reader touched stays cold.
    #[tokio::test(flavor = "multi_thread")]
    async fn warm_carries_only_cached_leaflets_to_the_rewritten_leaf() {
        // Three leaflets of 100 rows; the insert lands in leaflet 1 and splits it.
        let mut writer = LeafWriter::new(RunSortOrder::Spot, 100, 10_000, 1);
        writer.set_skip_history(true);
        for i in 1..=300 {
            writer.push_record(record(i * 2, 1)).unwrap();
        }
        let old = writer.finish().unwrap().remove(0);
        let novelty = [record(301, 5)];
        let output = update_leaf(&LeafUpdateInput {
            pool: Default::default(),
            leaf_bytes: &old.leaf_bytes,
            novelty: &novelty,
            novelty_ops: &[1],
            superseded: &[],
            superseded_ops: &[],
            order: RunSortOrder::Spot,
            g_id: 0,
            zstd_level: 1,
            leaflet_target_rows: 100,
            leaf_target_rows: 10_000,
            sidecar_bytes: None,
        })
        .unwrap();
        let blob = output.leaves.into_iter().next().unwrap();

        let cache = Arc::new(LeafletCache::with_max_bytes(64 << 20));
        cache_leaflet(&cache, &old.leaf_cid, &old.leaf_bytes, 0, ColumnSet::CORE);
        cache_leaflet(&cache, &old.leaf_cid, &old.leaf_bytes, 1, ColumnSet::ALL);
        let new_id = LeafletCache::cid_cache_key(&blob.info.leaf_cid.to_bytes());
        warm_replaced_leaflets(
            &cache,
            &old.leaf_cid,
            &blob.info.leaf_cid,
            blob.info.leaf_bytes.clone(),
            &blob.leaflet_origins,
        );

        let columns = |idx| cache.v3_leaflet_entry(new_id, idx).map(|(c, _)| c);
        assert_eq!(columns(0), Some(ColumnSet::CORE), "copied without a decode");
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while (columns(1).is_none() || columns(2).is_none()) && std::time::Instant::now() < deadline
        {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        assert_eq!(
            columns(1),
            Some(ColumnSet::ALL),
            "re-encoded halves decoded"
        );
        assert_eq!(columns(2), Some(ColumnSet::ALL));
        assert_eq!(columns(3), None, "never read, never warmed");
        assert!(cache.get_leaf_dir(new_id).is_some());

        let header = decode_leaf_header_v3(&blob.info.leaf_bytes).unwrap();
        let dir = decode_leaf_dir_v3_with_base(&blob.info.leaf_bytes, &header).unwrap();
        for idx in 0..3 {
            let (columns, warmed) = cache.v3_leaflet_entry(new_id, idx).unwrap();
            let fresh = load_leaflet_columns(
                &blob.info.leaf_bytes,
                &dir.entries[idx as usize],
                dir.payload_base,
                &ColumnProjection {
                    output: columns,
                    internal: ColumnSet::EMPTY,
                },
                header.order,
            )
            .unwrap();
            let rows = |b: &fluree_db_binary_index::read::column_types::ColumnBatch| {
                (0..b.row_count)
                    .map(|i| (b.s_id.get(i), b.o_key.get(i)))
                    .collect::<Vec<_>>()
            };
            assert_eq!(
                rows(&warmed),
                rows(&fresh),
                "leaflet {idx} holds the new leaf's rows"
            );
        }
    }
}
