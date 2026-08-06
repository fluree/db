//! V2 index build engine: merges V2 run files into per-graph FLI3/FBR3 indexes.
//!
//! Same merge-loop shape as V1 `build_index_from_run_paths_inner` but uses:
//! - `RunRecordV2` / `StreamingRunReader` / `KWayMerge`
//! - `LeafWriter` (segmentation-aware, columnar)
//! - `build_branch_bytes` (FBR3 with sidecar_cid)
//!
//! For the import-only milestone, all records are asserts (no retract-winner
//! handling), dedup is optional (usually safe to skip for fresh import), and
//! history sidecar production is skipped.

use super::merge::KWayMerge;
use crate::run_index::runs::run_file::StreamingRunFileWriter;
use crate::run_index::runs::streaming_reader::StreamingRunReader;
use fluree_db_binary_index::format::branch::{build_branch_bytes, LeafEntry};
use fluree_db_binary_index::format::history_sidecar::HistEntryV2;
use fluree_db_binary_index::format::leaf::{LeafInfo, LeafWriter};
use fluree_db_binary_index::format::run_record::RunSortOrder;
use fluree_db_binary_index::format::run_record_v2::{cmp_v2_for_order, RunRecordV2};
use fluree_db_binary_index::format::transitions::resolve_transitions;
use fluree_db_core::ContentId;
use fluree_db_core::GraphId;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const PROGRESS_BATCH_SIZE: u64 = 4096;

#[derive(Debug, Clone, Copy)]
struct ProcessMemorySnapshot {
    vm_rss_mb: u64,
    rss_anon_mb: u64,
    rss_file_mb: u64,
    vm_swap_mb: u64,
}

#[cfg(target_os = "linux")]
fn process_memory_snapshot() -> Option<ProcessMemorySnapshot> {
    fn kb_for(status: &str, key: &str) -> u64 {
        status
            .lines()
            .find_map(|line| {
                let rest = line.strip_prefix(key)?;
                rest.split_whitespace().next()?.parse::<u64>().ok()
            })
            .unwrap_or(0)
    }

    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    Some(ProcessMemorySnapshot {
        vm_rss_mb: kb_for(&status, "VmRSS:") / 1024,
        rss_anon_mb: kb_for(&status, "RssAnon:") / 1024,
        rss_file_mb: kb_for(&status, "RssFile:") / 1024,
        vm_swap_mb: kb_for(&status, "VmSwap:") / 1024,
    })
}

#[cfg(not(target_os = "linux"))]
fn process_memory_snapshot() -> Option<ProcessMemorySnapshot> {
    None
}

// ============================================================================
// Configuration
// ============================================================================

/// Configuration for building a V2 index from V2 run files.
#[derive(Debug, Clone)]
pub struct IndexBuildConfig {
    /// Directory containing V2 run files for this order.
    pub run_dir: PathBuf,
    /// Output directory for per-graph indexes.
    pub index_dir: PathBuf,
    /// Sort order for this index.
    pub sort_order: RunSortOrder,
    /// Target rows per leaflet (default 25000).
    pub leaflet_target_rows: usize,
    /// Target rows per leaf (default 250000).
    pub leaf_target_rows: usize,
    /// Zstd compression level.
    pub zstd_level: i32,
    /// Skip deduplication (safe for fresh bulk import).
    pub skip_dedup: bool,
    /// Skip history sidecar production (safe for append-only import).
    pub skip_history: bool,
    /// Graph ID for all records. Required because V2 run files (FRN2) do
    /// not carry g_id on the wire — the pipeline must be graph-scoped by
    /// construction (per-graph run directories + per-graph build calls).
    pub g_id: u16,
    /// Shared progress counter.
    pub progress: Option<Arc<AtomicU64>>,
    /// Max run files the k-way merge may hold open simultaneously. Run counts
    /// beyond this are first reduced by a lossless cascaded merge (see
    /// `cascade_runs_to_fan_in`). `usize::MAX` disables cascading (legacy
    /// unbudgeted behavior).
    pub fan_in_cap: usize,
}

// ============================================================================
// Results
// ============================================================================

/// Result for a single graph's V2 index build.
#[derive(Debug)]
pub struct PersistedLeafInfo {
    pub leaf_cid: ContentId,
    pub leaf_path: PathBuf,
    pub sidecar_cid: Option<ContentId>,
    pub sidecar_path: Option<PathBuf>,
    pub total_rows: u64,
    pub first_key: fluree_db_binary_index::format::run_record_v2::RunRecordV2,
    pub last_key: fluree_db_binary_index::format::run_record_v2::RunRecordV2,
    /// See [`LeafInfo::re_encoded_leaflet_count`]. Carried through the
    /// rebuild's spool-to-disk step so the upload step can charge the
    /// correct per-leaflet fuel.
    pub re_encoded_leaflet_count: u32,
}

/// Result for a single graph's V2 index build.
#[derive(Debug)]
pub struct GraphIndexResult {
    pub g_id: GraphId,
    pub total_rows: u64,
    /// Branch CID (content-addressed from branch bytes written to disk).
    pub branch_cid: ContentId,
    /// On-disk branch manifest path for later upload.
    pub branch_path: PathBuf,
    /// Produced leaf artifacts persisted to disk.
    pub leaf_infos: Vec<PersistedLeafInfo>,
    /// Per-leaf branch entries for root assembly.
    pub leaf_entries: Vec<LeafEntry>,
    pub graph_dir: PathBuf,
}

/// Result of the full V2 index build.
#[derive(Debug)]
pub struct IndexBuildResult {
    pub graphs: Vec<GraphIndexResult>,
    pub total_rows: u64,
    pub index_dir: PathBuf,
    pub elapsed: Duration,
}

// ============================================================================
// Build engine
// ============================================================================

/// Build a V2 index for a single sort order from V2 run files.
///
/// Discovers run files in `config.run_dir`, k-way merges them in sort order,
/// and produces per-graph FLI3 leaves + FBR3 branch manifests.
pub fn build_index(config: &IndexBuildConfig) -> Result<IndexBuildResult, IndexBuildError> {
    let t0 = Instant::now();

    // Discover run files.
    let mut run_paths = discover_run_files_v2(&config.run_dir)?;
    if run_paths.len() > config.fan_in_cap {
        // More runs than the FD budget allows open at once: losslessly
        // reduce the fan-in first (extra sequential merge passes trade
        // I/O for descriptors; output is byte-identical either way).
        tracing::info!(
            order = config.sort_order.dir_name(),
            runs = run_paths.len(),
            fan_in_cap = config.fan_in_cap,
            "run count exceeds fd budget; cascading merge passes"
        );
        run_paths = cascade_runs_to_fan_in(
            run_paths,
            config.sort_order,
            config.fan_in_cap,
            &config.run_dir.join("cascade"),
        )?;
    }
    if run_paths.is_empty() {
        // Empty graph/order: produce no artifacts.
        //
        // This is expected for reserved graphs (e.g., config graph) that may have
        // no data yet, and for user-defined named graphs that exist in graph_iris
        // but have no triples at the indexed t.
        return Ok(IndexBuildResult {
            graphs: Vec::new(),
            total_rows: 0,
            index_dir: config.index_dir.clone(),
            elapsed: t0.elapsed(),
        });
    }

    // Open streaming readers.
    let streams: Vec<StreamingRunReader> = run_paths
        .iter()
        .map(|p| StreamingRunReader::open(p))
        .collect::<io::Result<Vec<_>>>()?;

    let cmp = cmp_v2_for_order(config.sort_order);
    let mut merge = KWayMerge::new(streams, cmp)?;

    let order = config.sort_order;

    // V2 builds are graph-scoped: all records in the run directory belong
    // to config.g_id. No graph transition detection needed.
    let g_id = config.g_id;

    // Streams each completed leaf to disk as it is produced, so the build holds
    // only the active leaf's working set plus O(#leaves) branch metadata in RAM
    // — never the whole order's compressed FLI3/FHS1 blobs.
    let mut writer = PersistingLeafWriter::new(
        g_id,
        order,
        &config.index_dir,
        config.leaflet_target_rows,
        config.leaf_target_rows,
        config.zstd_level,
    )?;
    writer.set_skip_history(config.skip_history);

    let mut total_rows: u64 = 0;
    let mut progress_batch: u64 = 0;

    // Scratch: one identity's full event log, reused across identities.
    let mut events: Vec<(RunRecordV2, u8)> = Vec::new();

    loop {
        if config.skip_dedup {
            // Import path: no dedup, no history.
            let Some((record, op)) = merge.next_record()? else {
                break;
            };
            if op == 0 {
                continue;
            }
            writer.push_record(record)?;
            total_rows += 1;
        } else {
            // Rebuild: resolve each identity's full event log to its state
            // transitions. The row is the transition into the final asserted
            // state — the `t` at which the fact most recently became present
            // — matching the novelty and incremental materializers. The
            // transitions preceding the row become sidecar entries (unless
            // history is skipped); identities whose log nets to nothing,
            // such as retracts of never-asserted facts, vanish entirely.
            let Some((record, op, history)) = merge.next_deduped_with_history()? else {
                break;
            };
            events.clear();
            events.extend(history);
            events.push((record, op));
            // A rebuild sees the complete log, so lifecycles walk from
            // absent; `events` is left holding the sidecar transitions.
            let row = resolve_transitions(&mut events, false);
            if !config.skip_history {
                for &(rec, rec_op) in &events {
                    writer.push_history_entry(
                        fluree_db_binary_index::format::history_sidecar::HistEntryV2 {
                            s_id: rec.s_id,
                            p_id: rec.p_id,
                            o_type: rec.o_type,
                            o_key: rec.o_key,
                            o_i: rec.o_i,
                            t: rec.t,
                            op: rec_op,
                        },
                    );
                }
            }
            // Identities without a materialized row (final state absent)
            // still count toward progress below.
            if let Some(row) = row {
                writer.push_record(row)?;
                total_rows += 1;
            }
        }

        progress_batch += 1;
        if progress_batch >= PROGRESS_BATCH_SIZE {
            if let Some(ref ctr) = config.progress {
                ctr.fetch_add(progress_batch, Ordering::Relaxed);
            }
            progress_batch = 0;
        }
    }

    if progress_batch > 0 {
        if let Some(ref ctr) = config.progress {
            ctr.fetch_add(progress_batch, Ordering::Relaxed);
        }
    }

    let result = writer.finish()?;
    let graph_results = vec![result];

    // Reclaim the cascade's final-pass intermediates (a full zstd copy of
    // this order) now that the merge has consumed them, instead of carrying
    // them until the whole run dir is torn down. Best-effort: the build
    // result is already complete, and on an error path above the import-level
    // teardown removes the run dir wholesale anyway.
    drop(merge);
    let cascade_dir = config.run_dir.join("cascade");
    if cascade_dir.exists() {
        if let Err(e) = std::fs::remove_dir_all(&cascade_dir) {
            tracing::warn!(
                order = config.sort_order.dir_name(),
                error = %e,
                "failed to remove cascade scratch dir"
            );
        }
    }

    Ok(IndexBuildResult {
        graphs: graph_results,
        total_rows,
        index_dir: config.index_dir.clone(),
        elapsed: t0.elapsed(),
    })
}

// ============================================================================
// Helpers
// ============================================================================

fn create_graph_dir(index_dir: &Path, g_id: u16, order_name: &str) -> io::Result<PathBuf> {
    let graph_dir = index_dir.join(format!("graph_{g_id}/{order_name}"));
    std::fs::create_dir_all(&graph_dir)?;
    Ok(graph_dir)
}

/// Persist a single completed leaf (and its history sidecar, if any) to disk
/// as content-addressed files, returning only the metadata + paths. Dropping
/// `leaf_bytes`/`sidecar_bytes` here is what keeps the build from retaining
/// every compressed FLI3/FHS1 blob for the whole order.
fn persist_leaf(graph_dir: &Path, info: LeafInfo) -> io::Result<PersistedLeafInfo> {
    let LeafInfo {
        leaf_cid,
        leaf_bytes,
        sidecar_cid,
        sidecar_bytes,
        total_rows,
        first_key,
        last_key,
        re_encoded_leaflet_count,
    } = info;

    let leaf_path = graph_dir.join(leaf_cid.to_string());
    std::fs::write(&leaf_path, &leaf_bytes)?;

    let sidecar_path = match (&sidecar_cid, sidecar_bytes.as_ref()) {
        (Some(sc_cid), Some(sc_bytes)) => {
            let sc_path = graph_dir.join(sc_cid.to_string());
            std::fs::write(&sc_path, sc_bytes)?;
            Some(sc_path)
        }
        (None, None) => None,
        (Some(_), None) | (None, Some(_)) => {
            return Err(io::Error::other(
                "leaf sidecar CID/bytes mismatch while persisting index artifact",
            ));
        }
    };

    Ok(PersistedLeafInfo {
        leaf_cid,
        leaf_path,
        sidecar_cid,
        sidecar_path,
        total_rows,
        first_key,
        last_key,
        re_encoded_leaflet_count,
    })
}

/// Assemble the FBR3 branch manifest from the (already disk-persisted) leaf
/// metadata and write it. Branch entries must stay in leaf-flush order — i.e.
/// sorted by key range — so callers must preserve the order in which leaves
/// were produced; only the blob writes (in `persist_leaf`) are order-independent.
fn build_graph_index_result(
    g_id: GraphId,
    order: RunSortOrder,
    graph_dir: PathBuf,
    persisted_leaf_infos: Vec<PersistedLeafInfo>,
) -> io::Result<GraphIndexResult> {
    let total_rows: u64 = persisted_leaf_infos.iter().map(|l| l.total_rows).sum();

    let leaf_entries: Vec<LeafEntry> = persisted_leaf_infos
        .iter()
        .map(|info| LeafEntry {
            first_key: info.first_key,
            last_key: info.last_key,
            row_count: info.total_rows,
            leaf_cid: info.leaf_cid.clone(),
            sidecar_cid: info.sidecar_cid.clone(),
        })
        .collect();

    // Write branch manifest (FBR3).
    let branch_bytes = build_branch_bytes(order, g_id, &leaf_entries);
    let branch_hex = fluree_db_core::sha256_hex(&branch_bytes);
    let branch_cid = ContentId::from_hex_digest(
        fluree_db_core::content_kind::CODEC_FLUREE_INDEX_BRANCH,
        &branch_hex,
    )
    .expect("valid SHA-256 hex digest");

    let branch_path = graph_dir.join(branch_cid.to_string());
    std::fs::write(&branch_path, &branch_bytes)?;

    Ok(GraphIndexResult {
        g_id,
        total_rows,
        branch_cid,
        branch_path,
        leaf_infos: persisted_leaf_infos,
        leaf_entries,
        graph_dir,
    })
}

/// Indexer-side wrapper around the pure [`LeafWriter`] that streams each
/// completed leaf to disk as it is produced.
///
/// `LeafWriter` itself accumulates every completed leaf's compressed bytes in
/// memory until `finish()`. For a full-order build that is the entire order's
/// index — and the parallel secondary-order build can have several orders in
/// flight at once. This wrapper drains the writer after every `push_record`
/// and persists each completed leaf immediately, so retained memory is bounded
/// by the active leaf's working set plus the O(#leaves) branch metadata
/// (~100 bytes/leaf), not the order's full FLI3/FHS1 blob set.
///
/// Crate boundary: the `binary-index` crate stays pure (its `LeafWriter` still
/// returns in-memory `LeafInfo`, as the incremental paths require); only this
/// indexer-side wrapper touches the filesystem.
pub(crate) struct PersistingLeafWriter {
    inner: LeafWriter,
    g_id: GraphId,
    order: RunSortOrder,
    graph_dir: PathBuf,
    persisted: Vec<PersistedLeafInfo>,
}

impl PersistingLeafWriter {
    pub(crate) fn new(
        g_id: GraphId,
        order: RunSortOrder,
        index_dir: &Path,
        leaflet_target_rows: usize,
        leaf_target_rows: usize,
        zstd_level: i32,
    ) -> io::Result<Self> {
        let graph_dir = create_graph_dir(index_dir, g_id, order.dir_name())?;
        let inner = LeafWriter::new(order, leaflet_target_rows, leaf_target_rows, zstd_level);
        Ok(Self {
            inner,
            g_id,
            order,
            graph_dir,
            persisted: Vec::new(),
        })
    }

    pub(crate) fn set_skip_history(&mut self, skip: bool) {
        self.inner.set_skip_history(skip);
    }

    pub(crate) fn push_record(&mut self, record: RunRecordV2) -> io::Result<()> {
        self.inner.push_record(record)?;
        // A single push completes at most one leaf; drain + persist it now so
        // its blob bytes are not retained for the rest of the order.
        for info in self.inner.drain_completed_leaves() {
            self.persisted.push(persist_leaf(&self.graph_dir, info)?);
        }
        Ok(())
    }

    pub(crate) fn push_history_entry(&mut self, entry: HistEntryV2) {
        self.inner.push_history_entry(entry);
    }

    pub(crate) fn finish(mut self) -> io::Result<GraphIndexResult> {
        // `finish()` flushes the trailing leaf into the completed-leaves buffer;
        // persist that final batch (everything prior was drained per-push).
        let final_batch = self.inner.finish()?;
        for info in final_batch {
            self.persisted.push(persist_leaf(&self.graph_dir, info)?);
        }
        build_graph_index_result(self.g_id, self.order, self.graph_dir, self.persisted)
    }
}

/// Discover V2 run files in a directory (sorted by name).
pub fn discover_run_files_v2(dir: &Path) -> io::Result<Vec<PathBuf>> {
    let mut paths = Vec::new();
    if !dir.exists() {
        return Ok(paths);
    }
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().is_some_and(|ext| ext == "frn") {
            paths.push(path);
        }
    }
    paths.sort();
    Ok(paths)
}

/// Losslessly reduce a sorted run-file set to at most `fan_in` files via
/// multi-pass merges of **consecutive** groups, so the final k-way merge
/// never holds more than `fan_in` descriptors open.
///
/// Byte-identity argument (why a cascade is exactly a flat merge):
///
/// 1. The V2 comparators order on identity fields only — they exclude `t`
///    and op (`run_record_v2.rs`, proven by `comparator_excludes_t`) — and
///    `KWayMerge::next_record` performs no dedup, filtering, or op
///    resolution: every input `(record, op)` pair is emitted exactly once.
/// 2. For records comparing unequal, flat and cascaded merges trivially
///    agree. For records comparing equal, `KWayMerge` tie-breaks by
///    `stream_idx`. Flat merge therefore emits equal records in run-file
///    order. In the cascade, equal records within one group keep in-group
///    run order (in-group `stream_idx` = run order), and across groups the
///    next pass tie-breaks by intermediate-file index — which equals group
///    order, because groups are **consecutive** slices of the (sorted) path
///    list and outputs are named `merged_{i:06}` and consumed in that order.
///    Every run in group *g* precedes every run in group *g+1* in the flat
///    order too, so the emitted sequence is identical; induction extends
///    this to any cascade depth.
/// 3. Downstream consumers are pure functions of the emitted sequence: the
///    import path (`skip_dedup`) consumes `next_record` directly, and the
///    rebuild path's `next_deduped_with_history` resolves winners from the
///    same sequence. Intermediate passes preserve rather than resolve — op
///    bytes are carried verbatim (version-2 output whenever any input
///    carries ops) and nothing is dropped — so retract/assert resolution
///    still happens exactly once, in the final consumer.
///
/// Intermediates live under `scratch_dir/pass_{n}/`; each pass's inputs are
/// deleted once the pass completes (pass-0 inputs — the caller's original
/// run files, typically symlinks into per-chunk dirs — are never touched).
/// Passes needed: `ceil(log_fan_in(runs)) - 1`; disk overhead: one extra
/// zstd-compressed copy of the order per live pass.
pub(crate) fn cascade_runs_to_fan_in(
    mut run_paths: Vec<PathBuf>,
    order: RunSortOrder,
    fan_in: usize,
    scratch_dir: &Path,
) -> io::Result<Vec<PathBuf>> {
    // fan_in == 1 would make chunks(1) a no-op loop; 2 always terminates.
    // Defense-in-depth only: `plan_fd_usage` already floors
    // `merge_fan_in_per_order` at 8, so this can bite only a caller passing
    // a hand-built cap — the plan's floor is the authoritative one.
    let fan_in = fan_in.max(2);
    if scratch_dir.exists() {
        std::fs::remove_dir_all(scratch_dir)?;
    }

    let mut pass = 0usize;
    let mut prev_pass_dir: Option<PathBuf> = None;
    while run_paths.len() > fan_in {
        pass += 1;
        let pass_dir = scratch_dir.join(format!("pass_{pass}"));
        std::fs::create_dir_all(&pass_dir)?;

        let mut outputs = Vec::with_capacity(run_paths.len().div_ceil(fan_in));
        for (i, group) in run_paths.chunks(fan_in).enumerate() {
            let out_path = pass_dir.join(format!("merged_{i:06}.frn"));
            let streams: Vec<StreamingRunReader> = group
                .iter()
                .map(|p| StreamingRunReader::open(p))
                .collect::<io::Result<Vec<_>>>()?;
            // Version-2 output iff any input carries ops: op bytes must
            // survive the cascade for the final merge's dedup/history.
            let with_op = streams.iter().any(|s| s.header.has_op());
            let mut merge = KWayMerge::new(streams, cmp_v2_for_order(order))?;
            let mut writer = StreamingRunFileWriter::create(&out_path, order, with_op)?;
            while let Some((record, op)) = merge.next_record()? {
                writer.push(record, op)?;
            }
            writer.finish()?;
            outputs.push(out_path);
        }

        tracing::info!(
            order = order.dir_name(),
            pass,
            inputs = run_paths.len(),
            outputs = outputs.len(),
            "cascade merge pass complete"
        );

        // The just-consumed inputs of pass N were pass N-1's outputs; the
        // originals (pass 0 inputs) stay owned by the caller's run dir.
        if let Some(dir) = prev_pass_dir.replace(pass_dir) {
            std::fs::remove_dir_all(&dir)?;
        }
        run_paths = outputs;
    }

    Ok(run_paths)
}

// ============================================================================
// Errors
// ============================================================================

#[derive(Debug)]
pub enum IndexBuildError {
    Io(io::Error),
    NoRunFiles,
}

impl From<io::Error> for IndexBuildError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

impl std::fmt::Display for IndexBuildError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "I/O error during V2 index build: {e}"),
            Self::NoRunFiles => write!(f, "no V2 run files found"),
        }
    }
}

impl std::error::Error for IndexBuildError {}

// ============================================================================
// Build all orders from a base run directory
// ============================================================================

/// Configuration for `build_all_indexes`.
#[derive(Debug, Clone)]
pub struct BuildAllConfig {
    pub base_run_dir: PathBuf,
    pub index_dir: PathBuf,
    pub leaflet_target_rows: usize,
    pub leaf_target_rows: usize,
    pub zstd_level: i32,
    pub skip_dedup: bool,
    pub skip_history: bool,
    /// Graph ID — builds are graph-scoped (run files don't carry g_id).
    pub g_id: u16,
    pub progress: Option<Arc<AtomicU64>>,
    /// Max number of orders to build concurrently. Each order is an independent
    /// single-threaded k-way merge + leaf-encode pipeline over disjoint input
    /// and output directories, so running them in parallel turns the per-order
    /// build times from additive into `max()`. `0`/`1` preserves the legacy
    /// serial build; callers size this from the effective import core budget.
    pub max_concurrency: usize,
    /// Per-order merge fan-in cap, forwarded to [`IndexBuildConfig`]. Callers
    /// size this from the FD budget divided by the order concurrency (see
    /// `fd_plan::plan_fd_usage`); `usize::MAX` disables cascading.
    pub fan_in_cap: usize,
}

/// Build V2 indexes for all four orders from a base run directory.
///
/// Expects per-order subdirectories: `base_run_dir/{spot,psot,post,opst}/`.
/// Each subdirectory contains V2 run files sorted in that order.
///
/// Orders whose run directory exists are built concurrently, up to
/// `config.max_concurrency` at a time (work-stealing). Each order is fully
/// independent — disjoint run dirs in, disjoint `graph_{g}/{order}` dirs out,
/// independent results — so the only shared state is the atomic progress
/// counter. The import path drives SPOT separately on its own thread (its run
/// dir does not exist here), so in practice this parallelizes PSOT/POST/OPST.
pub fn build_all_indexes(
    config: &BuildAllConfig,
) -> Result<Vec<(RunSortOrder, IndexBuildResult)>, IndexBuildError> {
    // Collect the orders that actually have run files to build.
    let buildable: Vec<RunSortOrder> = RunSortOrder::all_build_orders()
        .iter()
        .copied()
        .filter(|order| config.base_run_dir.join(order.dir_name()).exists())
        .collect();

    if buildable.is_empty() {
        return Ok(Vec::new());
    }

    // Build one order: k-way merge its run files into FLI3/FBR3 artifacts.
    let build_one = |order: RunSortOrder| -> Result<IndexBuildResult, IndexBuildError> {
        let run_dir = config.base_run_dir.join(order.dir_name());
        let order_start = Instant::now();
        let run_count = discover_run_files_v2(&run_dir)?.len();
        if let Some(mem) = process_memory_snapshot() {
            tracing::info!(
                order = order.dir_name(),
                run_count,
                run_dir = %run_dir.display(),
                vm_rss_mb = mem.vm_rss_mb,
                rss_anon_mb = mem.rss_anon_mb,
                rss_file_mb = mem.rss_file_mb,
                vm_swap_mb = mem.vm_swap_mb,
                "starting order index build"
            );
        } else {
            tracing::info!(
                order = order.dir_name(),
                run_count,
                run_dir = %run_dir.display(),
                "starting order index build"
            );
        }

        let order_config = IndexBuildConfig {
            run_dir,
            index_dir: config.index_dir.clone(),
            sort_order: order,
            leaflet_target_rows: config.leaflet_target_rows,
            leaf_target_rows: config.leaf_target_rows,
            zstd_level: config.zstd_level,
            skip_dedup: config.skip_dedup,
            skip_history: config.skip_history,
            g_id: config.g_id,
            // Import progress reflects all order builds, not just one
            // representative order, so attach the shared counter to each.
            progress: config.progress.clone(),
            fan_in_cap: config.fan_in_cap,
        };

        let result = build_index(&order_config)?;
        if let Some(mem) = process_memory_snapshot() {
            tracing::info!(
                order = order.dir_name(),
                total_rows = result.total_rows,
                graphs = result.graphs.len(),
                elapsed_ms = order_start.elapsed().as_millis(),
                vm_rss_mb = mem.vm_rss_mb,
                rss_anon_mb = mem.rss_anon_mb,
                rss_file_mb = mem.rss_file_mb,
                vm_swap_mb = mem.vm_swap_mb,
                "completed order index build"
            );
        } else {
            tracing::info!(
                order = order.dir_name(),
                total_rows = result.total_rows,
                graphs = result.graphs.len(),
                elapsed_ms = order_start.elapsed().as_millis(),
                "completed order index build"
            );
        }
        Ok(result)
    };

    let concurrency = config.max_concurrency.max(1).min(buildable.len());
    tracing::info!(
        buildable_orders = ?buildable.iter().map(|o| o.dir_name()).collect::<Vec<_>>(),
        concurrency,
        "building secondary orders"
    );

    // Serial fast path (single order, or concurrency disabled): no thread spawn.
    if concurrency == 1 {
        let mut results = Vec::with_capacity(buildable.len());
        for order in buildable {
            results.push((order, build_one(order)?));
        }
        return Ok(results);
    }

    // Parallel path: work-stealing over the buildable orders, bounded to
    // `concurrency` threads. Mirrors the secondary-run-generation pool in
    // build_from_commits.rs. Workers push in completion order; we re-sort into
    // canonical `all_build_orders()` order before returning so the result is
    // deterministic regardless of thread scheduling. (The FIR6 encoder already
    // sorts orders by wire-id before serializing, so the root CID does not
    // depend on this — but a deterministic return contract avoids surprising
    // any future consumer that iterates results positionally.)
    let next = std::sync::atomic::AtomicUsize::new(0);
    let collected: std::sync::Mutex<Vec<(RunSortOrder, IndexBuildResult)>> =
        std::sync::Mutex::new(Vec::with_capacity(buildable.len()));

    std::thread::scope(|scope| -> Result<(), IndexBuildError> {
        let mut handles = Vec::with_capacity(concurrency);
        for _ in 0..concurrency {
            let next = &next;
            let collected = &collected;
            let buildable = &buildable;
            let build_one = &build_one;
            handles.push(scope.spawn(move || -> Result<(), IndexBuildError> {
                loop {
                    let i = next.fetch_add(1, Ordering::Relaxed);
                    if i >= buildable.len() {
                        break;
                    }
                    let order = buildable[i];
                    let result = build_one(order)?;
                    collected.lock().unwrap().push((order, result));
                }
                Ok(())
            }));
        }
        // Propagate the first worker error (others finish their current order).
        let mut first_err = None;
        for handle in handles {
            match handle.join() {
                Ok(Ok(())) => {}
                Ok(Err(e)) if first_err.is_none() => first_err = Some(e),
                Ok(Err(_)) => {}
                Err(_) => {
                    if first_err.is_none() {
                        first_err = Some(IndexBuildError::Io(io::Error::other(
                            "order index build thread panicked",
                        )));
                    }
                }
            }
        }
        match first_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    })?;

    let mut results = collected.into_inner().unwrap();
    // Deterministic order, independent of which thread finished first.
    results.sort_by_key(|(order, _)| order.to_wire_id());
    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::run_index::runs::run_file::write_run_file;
    use fluree_db_binary_index::format::leaf::{decode_leaf_dir_v3, decode_leaf_header_v3};
    use fluree_db_binary_index::format::run_record::LIST_INDEX_NONE;
    use fluree_db_binary_index::format::run_record_v2::RunRecordV2;
    use fluree_db_core::o_type::OType;
    use fluree_db_core::subject_id::SubjectId;

    fn make_rec(g_id: u16, s_id: u64, p_id: u32, o_type: u16, o_key: u64, t: u32) -> RunRecordV2 {
        RunRecordV2 {
            s_id: SubjectId(s_id),
            o_key,
            p_id,
            t,
            o_i: LIST_INDEX_NONE,
            o_type,
            g_id,
        }
    }

    #[test]
    fn build_single_order_post() {
        let dir = std::env::temp_dir().join("fluree_test_build_v2_post");
        let _ = std::fs::remove_dir_all(&dir);
        let run_dir = dir.join("runs");
        let index_dir = dir.join("index");
        std::fs::create_dir_all(&run_dir).unwrap();

        // Create a sorted run file with POST order.
        // POST sort: (p_id, o_type, o_key, o_i, s_id)
        let mut records = vec![
            make_rec(0, 10, 1, OType::XSD_INTEGER.as_u16(), 100, 1),
            make_rec(0, 20, 1, OType::XSD_INTEGER.as_u16(), 200, 2),
            make_rec(0, 30, 1, OType::XSD_INTEGER.as_u16(), 300, 3),
            make_rec(0, 40, 2, OType::XSD_STRING.as_u16(), 10, 4),
            make_rec(0, 50, 2, OType::XSD_STRING.as_u16(), 20, 5),
        ];
        // Already sorted for POST.
        use fluree_db_binary_index::format::run_record_v2::cmp_v2_post;
        records.sort_by(cmp_v2_post);

        write_run_file(
            &run_dir.join("run_00000.frn"),
            &records,
            RunSortOrder::Post,
            1,
            5,
        )
        .unwrap();

        // Build.
        let config = IndexBuildConfig {
            run_dir,
            index_dir: index_dir.clone(),
            sort_order: RunSortOrder::Post,
            leaflet_target_rows: 100,
            leaf_target_rows: 1000,
            zstd_level: 1,
            skip_dedup: true,
            skip_history: true,
            g_id: 0,
            progress: None,
            fan_in_cap: usize::MAX,
        };

        let result = build_index(&config).unwrap();
        assert_eq!(result.total_rows, 5);
        assert_eq!(result.graphs.len(), 1);

        let graph = &result.graphs[0];
        assert_eq!(graph.g_id, 0);
        assert_eq!(graph.total_rows, 5);
        assert!(!graph.leaf_infos.is_empty());

        // Verify FLI3 format.
        let leaf = &graph.leaf_infos[0];
        let leaf_bytes = std::fs::read(&leaf.leaf_path).unwrap();
        let header = decode_leaf_header_v3(&leaf_bytes).unwrap();
        assert_eq!(header.order, RunSortOrder::Post);

        // Should have 2 leaflets (p_id=1 and p_id=2 segmentation).
        let leaf_dir = decode_leaf_dir_v3(&leaf_bytes, &header).unwrap();
        assert_eq!(leaf_dir.len(), 2);
        assert_eq!(leaf_dir[0].p_const, Some(1));
        assert_eq!(leaf_dir[0].row_count, 3);
        assert_eq!(leaf_dir[1].p_const, Some(2));
        assert_eq!(leaf_dir[1].row_count, 2);

        // Verify o_type_const is set (single type per predicate).
        assert_eq!(leaf_dir[0].o_type_const, Some(OType::XSD_INTEGER.as_u16()));
        assert_eq!(leaf_dir[1].o_type_const, Some(OType::XSD_STRING.as_u16()));

        let _ = std::fs::remove_dir_all(&dir);
    }

    fn leaf_cids(r: &IndexBuildResult) -> Vec<(ContentId, Option<ContentId>, u64)> {
        r.graphs
            .iter()
            .flat_map(|g| g.leaf_infos.iter())
            .map(|l| (l.leaf_cid.clone(), l.sidecar_cid.clone(), l.total_rows))
            .collect()
    }

    fn branch_cids(r: &IndexBuildResult) -> Vec<ContentId> {
        r.graphs.iter().map(|g| g.branch_cid.clone()).collect()
    }

    /// A cascaded merge (fan_in_cap 3 forces two passes over 9 runs) must be
    /// byte-identical to the flat merge: leaf/sidecar/branch CIDs are
    /// content-addressed, so CID equality is byte equality. Import path
    /// (skip_dedup, no ops).
    #[test]
    fn cascade_matches_flat_merge_no_op() {
        use fluree_db_binary_index::format::run_record_v2::cmp_v2_spot;

        let dir = tempfile::tempdir().unwrap();
        let build = |fan_in_cap: usize, tag: &str| {
            let run_dir = dir.path().join(format!("runs_{tag}"));
            let index_dir = dir.path().join(format!("index_{tag}"));
            std::fs::create_dir_all(&run_dir).unwrap();
            for j in 0..9u64 {
                // Overlapping subjects across runs; identical-comparing
                // records (comparators exclude t) land in different runs so
                // the stream-idx tie-break is exercised across group seams.
                let mut records: Vec<RunRecordV2> = (0..200u64)
                    .map(|i| {
                        make_rec(
                            0,
                            (i * 3 + j) % 350,
                            1,
                            OType::XSD_INTEGER.as_u16(),
                            (i % 40) * 10,
                            j as u32 + 1,
                        )
                    })
                    .collect();
                records.sort_by(cmp_v2_spot);
                write_run_file(
                    &run_dir.join(format!("run_{j:05}.frn")),
                    &records,
                    RunSortOrder::Spot,
                    j as u32 + 1,
                    j as u32 + 1,
                )
                .unwrap();
            }
            let config = IndexBuildConfig {
                run_dir,
                index_dir,
                sort_order: RunSortOrder::Spot,
                leaflet_target_rows: 64,
                leaf_target_rows: 256,
                zstd_level: 1,
                skip_dedup: true,
                skip_history: true,
                g_id: 0,
                progress: None,
                fan_in_cap,
            };
            build_index(&config).unwrap()
        };

        let flat = build(usize::MAX, "flat");
        let cascaded = build(3, "cascade");

        assert_eq!(flat.total_rows, 9 * 200);
        assert_eq!(flat.total_rows, cascaded.total_rows);
        assert_eq!(leaf_cids(&flat), leaf_cids(&cascaded));
        assert_eq!(branch_cids(&flat), branch_cids(&cascaded));
    }

    /// Same byte-identity through the rebuild path: op bytes must survive
    /// cascade passes verbatim so dedup (max-t wins) and history-sidecar
    /// resolution happen exactly once, in the final merge, with identical
    /// results.
    #[test]
    fn cascade_matches_flat_merge_with_op() {
        use crate::run_index::runs::run_file::write_run_file_with_op;
        use fluree_db_binary_index::format::run_record_v2::cmp_v2_spot;

        let dir = tempfile::tempdir().unwrap();
        let build = |fan_in_cap: usize, tag: &str| {
            let run_dir = dir.path().join(format!("runs_{tag}"));
            let index_dir = dir.path().join(format!("index_{tag}"));
            std::fs::create_dir_all(&run_dir).unwrap();
            for j in 0..9u64 {
                // Identities recur across runs at distinct t with alternating
                // assert/retract, so the final merge resolves multi-event
                // lifecycles (rows + history entries + vanished facts).
                let mut recs: Vec<(RunRecordV2, u8)> = (0..150u64)
                    .map(|i| {
                        let rec = make_rec(
                            0,
                            (i * 5 + j * 2) % 100,
                            1,
                            OType::XSD_INTEGER.as_u16(),
                            (i % 25) * 4,
                            (j * 150 + i) as u32 + 1,
                        );
                        (rec, ((i + j) % 2) as u8)
                    })
                    .collect();
                recs.sort_by(|a, b| cmp_v2_spot(&a.0, &b.0));
                let records: Vec<RunRecordV2> = recs.iter().map(|(r, _)| *r).collect();
                let ops: Vec<u8> = recs.iter().map(|&(_, op)| op).collect();
                write_run_file_with_op(
                    &run_dir.join(format!("run_{j:05}.frn")),
                    &records,
                    &ops,
                    RunSortOrder::Spot,
                    j as u32 * 150 + 1,
                    j as u32 * 150 + 150,
                )
                .unwrap();
            }
            let config = IndexBuildConfig {
                run_dir,
                index_dir,
                sort_order: RunSortOrder::Spot,
                leaflet_target_rows: 64,
                leaf_target_rows: 256,
                zstd_level: 1,
                skip_dedup: false,
                skip_history: false,
                g_id: 0,
                progress: None,
                fan_in_cap,
            };
            build_index(&config).unwrap()
        };

        let flat = build(usize::MAX, "flat");
        let cascaded = build(3, "cascade");

        assert_eq!(flat.total_rows, cascaded.total_rows);
        assert_eq!(leaf_cids(&flat), leaf_cids(&cascaded));
        assert_eq!(branch_cids(&flat), branch_cids(&cascaded));
    }

    /// Decode every history entry from a persisted leaf's sidecar.
    fn read_all_history(leaf: &PersistedLeafInfo) -> Vec<HistEntryV2> {
        use fluree_db_binary_index::read::leaf_access::{FullBlobLeafHandle, LeafHandle};
        let leaf_bytes = std::fs::read(&leaf.leaf_path).unwrap();
        let sidecar_bytes = leaf
            .sidecar_path
            .as_ref()
            .map(|p| std::fs::read(p).unwrap());
        let handle = FullBlobLeafHandle::new(leaf_bytes, sidecar_bytes, 0).unwrap();
        (0..handle.dir().entries.len())
            .flat_map(|i| handle.load_sidecar_segment(i).unwrap())
            .collect()
    }

    /// A retract of a fact that was never asserted anywhere in the log is
    /// not a transition: it contributes no row and no history entries (a
    /// bare retract entry would make time-travel replay fabricate the fact's
    /// presence before the retraction). Real transitions — including the
    /// surviving row's own assert — are all recorded.
    #[test]
    fn build_skips_never_asserted_retract_lifecycles() {
        let dir = std::env::temp_dir().join("fluree_test_build_v2_never_asserted");
        let _ = std::fs::remove_dir_all(&dir);
        let run_dir = dir.join("runs");
        let index_dir = dir.join("index");
        std::fs::create_dir_all(&run_dir).unwrap();

        // SPOT-sorted (s ascending, then t):
        // s=10: asserted@1, retracted@2  -> no row; history keeps both events
        // s=20: retracted@3, never asserted -> no row, no history
        // s=30: asserted@4               -> row
        let records = vec![
            make_rec(0, 10, 1, OType::XSD_INTEGER.as_u16(), 100, 1),
            make_rec(0, 10, 1, OType::XSD_INTEGER.as_u16(), 100, 2),
            make_rec(0, 20, 1, OType::XSD_INTEGER.as_u16(), 200, 3),
            make_rec(0, 30, 1, OType::XSD_INTEGER.as_u16(), 300, 4),
        ];
        let ops = vec![1u8, 0, 0, 1];

        crate::run_index::runs::run_file::write_run_file_with_op(
            &run_dir.join("run_00000.frn"),
            &records,
            &ops,
            RunSortOrder::Spot,
            1,
            4,
        )
        .unwrap();

        let config = IndexBuildConfig {
            run_dir,
            index_dir: index_dir.clone(),
            sort_order: RunSortOrder::Spot,
            leaflet_target_rows: 100,
            leaf_target_rows: 1000,
            zstd_level: 1,
            skip_dedup: false,
            skip_history: false,
            g_id: 0,
            progress: None,
            fan_in_cap: usize::MAX,
        };

        let result = build_index(&config).unwrap();
        assert_eq!(result.total_rows, 1, "only s=30 survives to latest-state");

        let leaf = &result.graphs[0].leaf_infos[0];
        let history = read_all_history(leaf);

        let mut events: Vec<(u64, u32, u8)> = history
            .iter()
            .map(|e| (e.s_id.as_u64(), e.t, e.op))
            .collect();
        events.sort_unstable();
        assert_eq!(
            events,
            vec![(10, 1, 1), (10, 2, 0)],
            "transitions preceding a row are recorded; the surviving row \
             carries its own assert and the never-asserted retract leaves none"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Finding-4 parity: a fact asserted and later re-asserted (never
    /// retracted between) materializes with the FIRST assert's `t` — the `t`
    /// at which it became present — matching the novelty and incremental
    /// materializers instead of the old highest-`t` rule. The no-op
    /// re-assert records no history entry.
    #[test]
    fn build_reasserted_fact_keeps_first_assert_t() {
        let dir = std::env::temp_dir().join("fluree_test_build_v2_reassert_t");
        let _ = std::fs::remove_dir_all(&dir);
        let run_dir = dir.join("runs");
        let index_dir = dir.join("index");
        std::fs::create_dir_all(&run_dir).unwrap();

        let records = vec![
            make_rec(0, 10, 1, OType::XSD_INTEGER.as_u16(), 100, 1),
            make_rec(0, 10, 1, OType::XSD_INTEGER.as_u16(), 100, 6),
        ];
        let ops = vec![1u8, 1];
        crate::run_index::runs::run_file::write_run_file_with_op(
            &run_dir.join("run_00000.frn"),
            &records,
            &ops,
            RunSortOrder::Spot,
            1,
            6,
        )
        .unwrap();

        let config = IndexBuildConfig {
            run_dir,
            index_dir: index_dir.clone(),
            sort_order: RunSortOrder::Spot,
            leaflet_target_rows: 100,
            leaf_target_rows: 1000,
            zstd_level: 1,
            skip_dedup: false,
            skip_history: false,
            g_id: 0,
            progress: None,
            fan_in_cap: usize::MAX,
        };

        let result = build_index(&config).unwrap();
        assert_eq!(result.total_rows, 1);
        let leaf = &result.graphs[0].leaf_infos[0];
        assert_eq!(leaf.first_key.t, 1, "row carries the first assert's t");
        assert!(
            leaf.sidecar_path.is_none(),
            "the re-assert is a no-op and the row carries its own assert; \
             no sidecar entries exist"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A full lifecycle (assert, retract, re-assert) materializes the row at
    /// the re-add `t` with every transition in the sidecar.
    #[test]
    fn build_full_lifecycle_row_carries_readd_t() {
        let dir = std::env::temp_dir().join("fluree_test_build_v2_lifecycle_t");
        let _ = std::fs::remove_dir_all(&dir);
        let run_dir = dir.join("runs");
        let index_dir = dir.join("index");
        std::fs::create_dir_all(&run_dir).unwrap();

        let records = vec![
            make_rec(0, 10, 1, OType::XSD_INTEGER.as_u16(), 100, 1),
            make_rec(0, 10, 1, OType::XSD_INTEGER.as_u16(), 100, 4),
            make_rec(0, 10, 1, OType::XSD_INTEGER.as_u16(), 100, 6),
        ];
        let ops = vec![1u8, 0, 1];
        crate::run_index::runs::run_file::write_run_file_with_op(
            &run_dir.join("run_00000.frn"),
            &records,
            &ops,
            RunSortOrder::Spot,
            1,
            6,
        )
        .unwrap();

        let config = IndexBuildConfig {
            run_dir,
            index_dir: index_dir.clone(),
            sort_order: RunSortOrder::Spot,
            leaflet_target_rows: 100,
            leaf_target_rows: 1000,
            zstd_level: 1,
            skip_dedup: false,
            skip_history: false,
            g_id: 0,
            progress: None,
            fan_in_cap: usize::MAX,
        };

        let result = build_index(&config).unwrap();
        assert_eq!(result.total_rows, 1);
        let leaf = &result.graphs[0].leaf_infos[0];
        assert_eq!(leaf.first_key.t, 6, "row carries the re-add t");

        let history = read_all_history(leaf);
        let mut events: Vec<(u64, u32, u8)> = history
            .iter()
            .map(|e| (e.s_id.as_u64(), e.t, e.op))
            .collect();
        events.sort_unstable();
        assert_eq!(
            events,
            vec![(10, 1, 1), (10, 4, 0)],
            "the re-add assert is the row, not a sidecar entry"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn build_opst_type_segmentation() {
        let dir = std::env::temp_dir().join("fluree_test_build_v2_opst");
        let _ = std::fs::remove_dir_all(&dir);
        let run_dir = dir.join("runs");
        let index_dir = dir.join("index");
        std::fs::create_dir_all(&run_dir).unwrap();

        let mut records = vec![
            make_rec(0, 1, 1, OType::XSD_INTEGER.as_u16(), 100, 1),
            make_rec(0, 2, 2, OType::XSD_INTEGER.as_u16(), 200, 2),
            make_rec(0, 3, 1, OType::XSD_STRING.as_u16(), 10, 3),
            make_rec(0, 4, 2, OType::XSD_STRING.as_u16(), 20, 4),
        ];
        use fluree_db_binary_index::format::run_record_v2::cmp_v2_opst;
        records.sort_by(cmp_v2_opst);

        write_run_file(
            &run_dir.join("run_00000.frn"),
            &records,
            RunSortOrder::Opst,
            1,
            4,
        )
        .unwrap();

        let config = IndexBuildConfig {
            run_dir,
            index_dir: index_dir.clone(),
            sort_order: RunSortOrder::Opst,
            leaflet_target_rows: 100,
            leaf_target_rows: 1000,
            zstd_level: 1,
            skip_dedup: true,
            skip_history: true,
            g_id: 0,
            progress: None,
            fan_in_cap: usize::MAX,
        };

        let result = build_index(&config).unwrap();
        assert_eq!(result.total_rows, 4);

        let leaf = &result.graphs[0].leaf_infos[0];
        let leaf_bytes = std::fs::read(&leaf.leaf_path).unwrap();
        let header = decode_leaf_header_v3(&leaf_bytes).unwrap();
        let leaf_dir = decode_leaf_dir_v3(&leaf_bytes, &header).unwrap();

        // Should have 2 leaflets (INTEGER and STRING type segmentation).
        assert_eq!(leaf_dir.len(), 2);
        assert_eq!(leaf_dir[0].o_type_const, Some(OType::XSD_INTEGER.as_u16()));
        assert_eq!(leaf_dir[1].o_type_const, Some(OType::XSD_STRING.as_u16()));

        let _ = std::fs::remove_dir_all(&dir);
    }
}
