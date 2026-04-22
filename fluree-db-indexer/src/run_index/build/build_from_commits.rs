//! V3 index build from sorted commit artifacts.
//!
//! Orchestrates the full V3 pipeline: remap sorted commits → V2 run files →
//! k-way merge → FLI3/FBR3 artifacts. Operates synchronously within a
//! `spawn_blocking` context.
//!
//! Bulk import now writes V2-native sorted-commit artifacts directly, so this
//! module consumes those artifacts without a bulk-import-only V1 → V2 pass.

use crate::run_index::build::index_build::{
    build_all_indexes, finish_graph_v2, BuildAllConfig, IndexBuildResult,
};
use crate::run_index::build::merge::KWayMerge;
use crate::run_index::runs::run_writer::{
    MultiOrderConfig, MultiOrderRunWriter, MultiOrderRunWriterWithOp,
};
use crate::run_index::runs::spool::{
    link_chunk_run_files_to_flat, remap_commit_to_runs_with_op, remap_sorted_commit_v2_to_runs,
    MmapStringRemap, MmapSubjectRemap, SortedCommitMergeReaderV2, SubjectRemap,
};
use crate::stats::{stats_record_from_v2, SpotClassStats, DT_REF_ID};
use fluree_db_binary_index::format::leaf::LeafWriter;
use fluree_db_binary_index::format::run_record::RunSortOrder;
use fluree_db_binary_index::format::run_record_v2::cmp_v2_spot;
use fluree_db_core::o_type::OType;
use fluree_db_core::o_type_registry::OTypeRegistry;
use rustc_hash::{FxHashMap, FxHashSet};
use std::io;
use std::path::PathBuf;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::Instant;

pub const BUILD_STAGE_REMAP: u8 = 1;
pub const BUILD_STAGE_LINK_RUNS: u8 = 2;
pub const BUILD_STAGE_MERGE: u8 = 3;
const PROGRESS_BATCH_SIZE: u64 = 4096;

/// Input for a single sorted commit chunk.
#[derive(Clone)]
pub struct CommitInput {
    /// Path to the V2-native sorted commit file.
    pub commit_path: PathBuf,
    /// Number of records in the commit file.
    pub record_count: u64,
    /// Path to the mmap'd subject remap file.
    pub subject_remap_path: PathBuf,
    /// Path to the mmap'd string remap file.
    pub string_remap_path: PathBuf,
    /// Chunk-local → global language ID remap.
    pub lang_remap: Vec<u16>,
    /// Optional rdf:type sidecar used to rebuild the subject→class bitset table.
    pub types_map_path: Option<PathBuf>,
}

/// Configuration for the V3 build-from-commits pipeline.
#[derive(Clone)]
pub struct BuildConfig {
    /// Base directory for temporary run files.
    pub run_dir: PathBuf,
    /// Output directory for per-graph index artifacts.
    pub index_dir: PathBuf,
    /// Graph ID (builds are graph-scoped).
    pub g_id: u16,
    /// Target rows per leaflet.
    pub leaflet_target_rows: usize,
    /// Target rows per leaf.
    pub leaf_target_rows: usize,
    /// Zstd compression level.
    pub zstd_level: i32,
    /// Memory budget for run writers (bytes).
    pub run_budget_bytes: usize,
    /// Worker count for chunk-parallel secondary-order generation.
    pub worker_count: usize,
    /// Remap progress counter (optional).
    pub remap_progress: Option<Arc<AtomicU64>>,
    /// Merge/build progress counter (optional).
    pub build_progress: Option<Arc<AtomicU64>>,
    /// Shared stage marker for external progress reporting.
    pub stage_marker: Option<Arc<AtomicU8>>,
}

/// Result of the V3 build pipeline.
pub struct BuildResult {
    /// Per-order build results (each contains per-graph artifacts).
    pub order_results: Vec<(RunSortOrder, IndexBuildResult)>,
    /// Total rows across all orders (from the POST result for canonical count).
    pub total_rows: u64,
    /// Total records remapped across all chunks.
    pub total_remapped: u64,
    /// Time spent in the remap phase.
    pub remap_elapsed: std::time::Duration,
    /// Time spent in the build phase.
    pub build_elapsed: std::time::Duration,
}

pub struct ClassBitsetTable {
    pub bit_to_class: Vec<u64>,
    graph_bitsets: FxHashMap<u16, FxHashMap<u16, Vec<u64>>>,
}

impl ClassBitsetTable {
    pub fn get(&self, g_id: u16, sid: u64) -> u64 {
        let ns_code = (sid >> 48) as u16;
        let local_id = (sid & 0x0000_FFFF_FFFF_FFFF) as usize;
        self.graph_bitsets
            .get(&g_id)
            .and_then(|ns| ns.get(&ns_code))
            .and_then(|v| v.as_slice().get(local_id).copied())
            .unwrap_or(0)
    }

    fn build_from_commits(commits: &[CommitInput]) -> io::Result<Option<Self>> {
        use std::io::{BufReader, Read};

        let mut class_to_bit: FxHashMap<u64, u8> = FxHashMap::default();
        let mut bit_to_class: Vec<u64> = Vec::new();
        let mut graph_bitsets: FxHashMap<u16, FxHashMap<u16, Vec<u64>>> = FxHashMap::default();
        let mut overflow_classes: FxHashSet<u64> = FxHashSet::default();
        let mut overflow_entries = 0u64;
        let mut saw_sidecar = false;
        let mut buf = [0u8; 18];

        for commit in commits {
            let Some(types_map_path) = &commit.types_map_path else {
                continue;
            };
            saw_sidecar = true;
            let remap = MmapSubjectRemap::open(&commit.subject_remap_path)?;
            let file = std::fs::File::open(types_map_path)?;
            let entry_count = file.metadata()?.len() / 18;
            let mut reader = BufReader::new(file);

            for _ in 0..entry_count {
                reader.read_exact(&mut buf)?;
                let g_id = u16::from_le_bytes([buf[0], buf[1]]);
                let s_local = u64::from_le_bytes(buf[2..10].try_into().unwrap());
                let c_local = u64::from_le_bytes(buf[10..18].try_into().unwrap());
                let s_global = remap.get(s_local as usize)?;
                let c_global = remap.get(c_local as usize)?;

                let bit_idx = if let Some(&idx) = class_to_bit.get(&c_global) {
                    idx
                } else if bit_to_class.len() < 64 {
                    let idx = bit_to_class.len() as u8;
                    class_to_bit.insert(c_global, idx);
                    bit_to_class.push(c_global);
                    idx
                } else {
                    overflow_classes.insert(c_global);
                    overflow_entries += 1;
                    continue;
                };

                let ns_code = (s_global >> 48) as u16;
                let local_id = (s_global & 0x0000_FFFF_FFFF_FFFF) as usize;
                let ns_map = graph_bitsets.entry(g_id).or_default();
                let vec = ns_map.entry(ns_code).or_default();
                if local_id >= vec.len() {
                    vec.resize(local_id + 1, 0);
                }
                vec[local_id] |= 1u64 << bit_idx;
            }
        }

        if !saw_sidecar {
            return Ok(None);
        }

        tracing::info!(
            classes = bit_to_class.len(),
            graphs = graph_bitsets.len(),
            total_subjects = graph_bitsets
                .values()
                .flat_map(|ns| ns.values())
                .map(std::vec::Vec::len)
                .sum::<usize>(),
            "class bitset table built"
        );

        if !overflow_classes.is_empty() {
            tracing::warn!(
                retained_classes = bit_to_class.len(),
                skipped_classes = overflow_classes.len(),
                skipped_type_assertions = overflow_entries,
                "class ref stats truncated at 64 distinct classes; stats.classes[*].properties[*].ref-classes may be incomplete"
            );
        }

        Ok(Some(Self {
            bit_to_class,
            graph_bitsets,
        }))
    }

    /// Build from `.types` sidecar files that already contain **global** IDs.
    ///
    /// Used by the full rebuild path (`rebuild.rs`) where `.types` sidecars are
    /// written after chunk-local → global remapping (no `MmapSubjectRemap` needed).
    ///
    /// Wire format: 18 bytes per entry — `(g_id: u16 LE, s_id: u64 LE, class_sid64: u64 LE)`.
    pub fn build_from_global_types(types_paths: &[PathBuf]) -> io::Result<Option<Self>> {
        use std::io::{BufReader, Read};

        let mut class_to_bit: FxHashMap<u64, u8> = FxHashMap::default();
        let mut bit_to_class: Vec<u64> = Vec::new();
        let mut graph_bitsets: FxHashMap<u16, FxHashMap<u16, Vec<u64>>> = FxHashMap::default();
        let mut overflow_classes: FxHashSet<u64> = FxHashSet::default();
        let mut overflow_entries = 0u64;
        let mut saw_sidecar = false;
        let mut buf = [0u8; 18];

        for types_path in types_paths {
            if !types_path.exists() {
                continue;
            }
            saw_sidecar = true;
            let file = std::fs::File::open(types_path)?;
            let entry_count = file.metadata()?.len() / 18;
            let mut reader = BufReader::new(file);

            for _ in 0..entry_count {
                reader.read_exact(&mut buf)?;
                let g_id = u16::from_le_bytes([buf[0], buf[1]]);
                let s_global = u64::from_le_bytes(buf[2..10].try_into().unwrap());
                let c_global = u64::from_le_bytes(buf[10..18].try_into().unwrap());

                let bit_idx = if let Some(&idx) = class_to_bit.get(&c_global) {
                    idx
                } else if bit_to_class.len() < 64 {
                    let idx = bit_to_class.len() as u8;
                    class_to_bit.insert(c_global, idx);
                    bit_to_class.push(c_global);
                    idx
                } else {
                    overflow_classes.insert(c_global);
                    overflow_entries += 1;
                    continue;
                };

                let ns_code = (s_global >> 48) as u16;
                let local_id = (s_global & 0x0000_FFFF_FFFF_FFFF) as usize;
                let ns_map = graph_bitsets.entry(g_id).or_default();
                let vec = ns_map.entry(ns_code).or_default();
                if local_id >= vec.len() {
                    vec.resize(local_id + 1, 0);
                }
                vec[local_id] |= 1u64 << bit_idx;
            }
        }

        if !saw_sidecar {
            return Ok(None);
        }

        tracing::info!(
            classes = bit_to_class.len(),
            graphs = graph_bitsets.len(),
            total_subjects = graph_bitsets
                .values()
                .flat_map(|ns| ns.values())
                .map(std::vec::Vec::len)
                .sum::<usize>(),
            "class bitset table built (from global types)"
        );

        if !overflow_classes.is_empty() {
            tracing::warn!(
                retained_classes = bit_to_class.len(),
                skipped_classes = overflow_classes.len(),
                skipped_type_assertions = overflow_entries,
                "class ref stats truncated at 64 distinct classes; stats.classes[*].properties[*].ref-classes may be incomplete"
            );
        }

        Ok(Some(Self {
            bit_to_class,
            graph_bitsets,
        }))
    }
}

pub struct SpotClassStatsCollector {
    rdf_type_p_id: u32,
    current_s_id: Option<u64>,
    current_g_id: u16,
    classes: Vec<u64>,
    prop_dts: FxHashMap<(u32, u16), u64>,
    prop_langs: FxHashMap<(u32, u16), u64>,
    ref_targets: Vec<(u32, u64)>,
    class_bitset: Option<ClassBitsetTable>,
    result: SpotClassStats,
}

impl SpotClassStatsCollector {
    pub fn new(rdf_type_p_id: u32, class_bitset: Option<ClassBitsetTable>) -> Self {
        Self {
            rdf_type_p_id,
            current_s_id: None,
            current_g_id: 0,
            classes: Vec::new(),
            prop_dts: FxHashMap::default(),
            prop_langs: FxHashMap::default(),
            ref_targets: Vec::new(),
            class_bitset,
            result: SpotClassStats::default(),
        }
    }

    pub fn on_record(&mut self, rec: &fluree_db_binary_index::format::run_record_v2::RunRecordV2) {
        let sr = stats_record_from_v2(rec, 1);
        if self.current_s_id != Some(sr.s_id) || self.current_g_id != sr.g_id {
            self.flush_subject();
            self.current_s_id = Some(sr.s_id);
            self.current_g_id = sr.g_id;
        }

        if sr.p_id == self.rdf_type_p_id && sr.o_kind == 0x05 {
            self.classes.push(sr.o_key);
            return;
        }

        let ot = OType::from_u16(rec.o_type);
        let is_ref = ot == OType::IRI_REF;
        let dt = if is_ref {
            DT_REF_ID
        } else {
            sr.dt.as_u8() as u16
        };
        *self.prop_dts.entry((sr.p_id, dt)).or_insert(0) += 1;

        if sr.lang_id != 0 {
            *self.prop_langs.entry((sr.p_id, sr.lang_id)).or_insert(0) += 1;
        }

        if is_ref && self.class_bitset.is_some() {
            self.ref_targets.push((sr.p_id, sr.o_key));
        }
    }

    pub fn flush_subject(&mut self) {
        if self.classes.is_empty() {
            self.prop_dts.clear();
            self.prop_langs.clear();
            self.ref_targets.clear();
            return;
        }

        let g_id = self.current_g_id;
        for &class_sid in &self.classes {
            *self
                .result
                .class_counts
                .entry((g_id, class_sid))
                .or_insert(0) += 1;
            let class_entry = self
                .result
                .class_prop_dts
                .entry((g_id, class_sid))
                .or_default();
            for (&(p_id, dt), &count) in &self.prop_dts {
                *class_entry.entry(p_id).or_default().entry(dt).or_insert(0) += count;
            }
            if !self.prop_langs.is_empty() {
                let lang_entry = self
                    .result
                    .class_prop_langs
                    .entry((g_id, class_sid))
                    .or_default();
                for (&(p_id, lang_id), &count) in &self.prop_langs {
                    *lang_entry
                        .entry(p_id)
                        .or_default()
                        .entry(lang_id)
                        .or_insert(0) += count;
                }
            }
        }

        if let Some(ref bitset) = self.class_bitset {
            for &(p_id, target_sid) in &self.ref_targets {
                let target_bits = bitset.get(g_id, target_sid);
                if target_bits == 0 {
                    continue;
                }
                for &src_class in &self.classes {
                    let ref_entry = self
                        .result
                        .class_prop_refs
                        .entry((g_id, src_class))
                        .or_default()
                        .entry(p_id)
                        .or_default();
                    let mut bits = target_bits;
                    while bits != 0 {
                        let bit_idx = bits.trailing_zeros() as usize;
                        let target_class = bitset.bit_to_class[bit_idx];
                        *ref_entry.entry(target_class).or_insert(0) += 1;
                        bits &= bits - 1;
                    }
                }
            }
        }

        self.classes.clear();
        self.prop_dts.clear();
        self.prop_langs.clear();
        self.ref_targets.clear();
    }

    pub fn finish(mut self) -> SpotClassStats {
        self.flush_subject();
        self.result
    }
}

/// Build V3 indexes from V2-native sorted commit files.
///
/// Bulk import writes a V2-native sorted-commit artifact after chunk-local vocab
/// alignment. This function applies the global subject/string/language remaps to
/// that artifact, writes per-order run files, then k-way merges those runs into
/// final FLI3/FBR3 artifacts.
///
/// Steps:
/// 1. Build SPOT directly from V2 sorted commit artifacts
/// 2. Generate secondary-order run files in parallel
/// 3. K-way merge secondary runs → FLI3/FBR3
pub fn build_indexes_from_commits(
    commits: &[CommitInput],
    config: &BuildConfig,
    mut stats_hook: Option<&mut crate::stats::IdStatsHook>,
) -> io::Result<(BuildResult, Option<SpotClassStats>)> {
    let overall_start = Instant::now();
    tracing::info!(
        chunks = commits.len(),
        worker_count = config.worker_count,
        run_budget_bytes = config.run_budget_bytes,
        g_id = config.g_id,
        "starting build_indexes_from_commits"
    );
    if let Some(stage) = &config.stage_marker {
        stage.store(BUILD_STAGE_REMAP, Ordering::Relaxed);
    }

    let spot_commits = commits.to_vec();
    let spot_config = config.clone();
    let spot_rdf_type_p_id = stats_hook.as_ref().and_then(|hook| hook.rdf_type_p_id());
    let spot_class_bitset = if spot_rdf_type_p_id.is_some() {
        ClassBitsetTable::build_from_commits(commits)?
    } else {
        None
    };
    let spot_handle = std::thread::spawn(move || {
        build_spot_index_from_commits(
            &spot_commits,
            &spot_config,
            spot_rdf_type_p_id,
            spot_class_bitset,
        )
    });

    // Phase 1: Generate secondary-order runs in parallel.
    let remap_start = Instant::now();
    let worker_count = config.worker_count.max(1).min(commits.len().max(1));
    let per_thread_budget_bytes = (config.run_budget_bytes / worker_count).max(64 * 1024 * 1024);
    tracing::info!(
        chunks = commits.len(),
        worker_count,
        per_thread_budget_bytes,
        "starting secondary-order run generation"
    );
    let mut total_remapped = 0u64;
    let mut worker_hooks: Vec<crate::stats::IdStatsHook> = Vec::new();
    let next_chunk = Arc::new(AtomicUsize::new(0));
    std::thread::scope(|scope| -> io::Result<()> {
        let mut handles = Vec::with_capacity(worker_count);
        for _ in 0..worker_count {
            let commits_ref = commits;
            let next_chunk = Arc::clone(&next_chunk);
            let run_dir = config.run_dir.clone();
            let remap_progress = config.remap_progress.clone();
            let target_g_id = config.g_id;
            let collect_stats = stats_hook.is_some();

            handles.push(scope.spawn(
                move || -> io::Result<(u64, Option<crate::stats::IdStatsHook>)> {
                    let mut local_total = 0u64;
                    let mut worker_hook = collect_stats.then(crate::stats::IdStatsHook::new);
                    loop {
                        let pos = next_chunk.fetch_add(1, Ordering::Relaxed);
                        if pos >= commits_ref.len() {
                            break;
                        }

                        let commit = &commits_ref[pos];
                        let chunk_run_dir = run_dir.join(format!("chunk_{pos}"));
                        std::fs::create_dir_all(&chunk_run_dir)?;

                        let s_remap = MmapSubjectRemap::open(&commit.subject_remap_path)?;
                        let str_remap = MmapStringRemap::open(&commit.string_remap_path)?;
                        let mut writer = MultiOrderRunWriter::new(MultiOrderConfig {
                            total_budget_bytes: per_thread_budget_bytes,
                            orders: RunSortOrder::secondary_orders().to_vec(),
                            base_run_dir: chunk_run_dir,
                        })?;

                        let written = remap_sorted_commit_v2_to_runs(
                            &commit.commit_path,
                            commit.record_count,
                            &s_remap,
                            &str_remap,
                            &commit.lang_remap,
                            target_g_id,
                            &mut writer,
                            worker_hook.as_mut(),
                            remap_progress.as_deref(),
                        )?;
                        writer.finish()?;

                        local_total += written;
                    }
                    Ok((local_total, worker_hook))
                },
            ));
        }

        for handle in handles {
            let (written, hook) = handle
                .join()
                .map_err(|_| io::Error::other("secondary run generation thread panicked"))??;
            total_remapped += written;
            if let Some(hook) = hook {
                worker_hooks.push(hook);
            }
        }
        Ok(())
    })?;
    let remap_elapsed = remap_start.elapsed();
    tracing::info!(
        total_remapped,
        elapsed_ms = remap_elapsed.as_millis(),
        "secondary-order run generation complete"
    );

    if let Some(target_hook) = stats_hook.as_mut() {
        let stats_merge_start = Instant::now();
        tracing::info!(
            worker_hooks = worker_hooks.len(),
            "merging worker-local id stats hooks"
        );
        for hook in worker_hooks {
            target_hook.merge_from(hook);
        }
        tracing::info!(
            elapsed_ms = stats_merge_start.elapsed().as_millis(),
            "merged worker-local id stats hooks"
        );
    }

    // Phase 2: Build secondary indexes from run files.
    if let Some(stage) = &config.stage_marker {
        stage.store(BUILD_STAGE_LINK_RUNS, Ordering::Relaxed);
    }
    for &order in RunSortOrder::secondary_orders() {
        let link_start = Instant::now();
        let flat_dir = config.run_dir.join(order.dir_name());
        if flat_dir.exists() {
            std::fs::remove_dir_all(&flat_dir)?;
        }
        let linked = link_chunk_run_files_to_flat(&config.run_dir, order, &flat_dir)?;
        tracing::info!(
            order = order.dir_name(),
            linked_runs = linked,
            elapsed_ms = link_start.elapsed().as_millis(),
            "linked secondary-order run files"
        );
    }
    if let Some(stage) = &config.stage_marker {
        stage.store(BUILD_STAGE_MERGE, Ordering::Relaxed);
    }
    let build_start = Instant::now();
    tracing::info!("starting secondary index merge/build");

    let build_config = BuildAllConfig {
        base_run_dir: config.run_dir.clone(),
        index_dir: config.index_dir.clone(),
        leaflet_target_rows: config.leaflet_target_rows,
        leaf_target_rows: config.leaf_target_rows,
        zstd_level: config.zstd_level,
        skip_dedup: true,   // Fresh import: unique asserts.
        skip_history: true, // Append-only: no time-travel data.
        g_id: config.g_id,
        progress: config.build_progress.clone(),
    };

    let mut order_results = build_all_indexes(&build_config).map_err(io::Error::other)?;
    let (spot_result, spot_class_stats) = spot_handle
        .join()
        .map_err(|_| io::Error::other("SPOT direct build thread panicked"))??;
    order_results.push((RunSortOrder::Spot, spot_result));

    let _ = build_start;
    let build_elapsed = overall_start.elapsed().saturating_sub(remap_elapsed);
    tracing::info!(
        orders = order_results.len(),
        build_elapsed_ms = build_elapsed.as_millis(),
        total_elapsed_ms = overall_start.elapsed().as_millis(),
        "build_indexes_from_commits complete"
    );

    // Total rows from the POST result (canonical count — avoids double-counting).
    let total_rows = order_results
        .iter()
        .find(|(o, _)| *o == RunSortOrder::Post)
        .map(|(_, r)| r.total_rows)
        .unwrap_or_else(|| {
            order_results
                .first()
                .map(|(_, r)| r.total_rows)
                .unwrap_or(0)
        });

    Ok((
        BuildResult {
            order_results,
            total_rows,
            total_remapped,
            remap_elapsed,
            build_elapsed,
        },
        spot_class_stats,
    ))
}

fn build_spot_index_from_commits(
    commits: &[CommitInput],
    config: &BuildConfig,
    rdf_type_p_id: Option<u32>,
    class_bitset: Option<ClassBitsetTable>,
) -> io::Result<(IndexBuildResult, Option<SpotClassStats>)> {
    let g_id = config.g_id;
    let index_dir = &config.index_dir;
    let leaflet_target_rows = config.leaflet_target_rows;
    let leaf_target_rows = config.leaf_target_rows;
    let zstd_level = config.zstd_level;
    let progress = config.build_progress.clone();
    let t0 = Instant::now();
    tracing::info!(
        chunks = commits.len(),
        g_id,
        "starting direct SPOT build from sorted commits"
    );
    let streams: Vec<SortedCommitMergeReaderV2<MmapSubjectRemap, MmapStringRemap>> = commits
        .iter()
        .map(|commit| {
            let s_remap = MmapSubjectRemap::open(&commit.subject_remap_path)?;
            let str_remap = MmapStringRemap::open(&commit.string_remap_path)?;
            SortedCommitMergeReaderV2::open(
                &commit.commit_path,
                commit.record_count,
                s_remap,
                str_remap,
                commit.lang_remap.clone(),
                g_id,
            )
        })
        .collect::<io::Result<Vec<_>>>()?;

    if streams.is_empty() {
        return Ok((
            IndexBuildResult {
                graphs: Vec::new(),
                total_rows: 0,
                index_dir: index_dir.to_path_buf(),
                elapsed: t0.elapsed(),
            },
            None,
        ));
    }

    let mut merge = KWayMerge::new(streams, cmp_v2_spot)?;
    let order = RunSortOrder::Spot;
    let order_name = order.dir_name();
    std::fs::create_dir_all(index_dir.join(format!("graph_{g_id}/{order_name}")))?;

    let mut writer = LeafWriter::new(order, leaflet_target_rows, leaf_target_rows, zstd_level);
    writer.set_skip_history(true);

    let mut total_rows = 0u64;
    let mut progress_batch = 0u64;
    let mut class_stats_collector =
        rdf_type_p_id.map(|p_id| SpotClassStatsCollector::new(p_id, class_bitset));
    while let Some((record, op)) = merge.next_record()? {
        if op == 0 {
            continue;
        }
        if let Some(ref mut collector) = class_stats_collector {
            collector.on_record(&record);
        }
        writer.push_record(record)?;
        total_rows += 1;
        progress_batch += 1;
        if progress_batch >= PROGRESS_BATCH_SIZE {
            if let Some(ref ctr) = progress {
                ctr.fetch_add(progress_batch, Ordering::Relaxed);
            }
            progress_batch = 0;
        }
    }
    if progress_batch > 0 {
        if let Some(ref ctr) = progress {
            ctr.fetch_add(progress_batch, Ordering::Relaxed);
        }
    }

    let result = finish_graph_v2(g_id, order, writer, index_dir, order_name)?;
    tracing::info!(
        g_id,
        total_rows,
        elapsed_ms = t0.elapsed().as_millis(),
        "direct SPOT build complete"
    );
    Ok((
        IndexBuildResult {
            graphs: vec![result],
            total_rows,
            index_dir: index_dir.to_path_buf(),
            elapsed: t0.elapsed(),
        },
        class_stats_collector.map(SpotClassStatsCollector::finish),
    ))
}

/// Build V3 indexes from globally-remapped sorted commit files (rebuild path).
///
/// Unlike [`build_indexes_from_commits`], which takes `CommitInput` with
/// per-chunk remap files, this function takes `SortedCommitInfo` entries whose
/// sorted commit files already contain globally-remapped IDs. Uses
/// `IdentitySubjectRemap` / `IdentityStringRemap` since
/// no disk remap files exist.
///
/// Key differences from the import path:
/// - Input: `&[SortedCommitInfo]` (not `&[CommitInput]`)
/// - Remap: identity (global IDs already in place)
/// - `skip_dedup: false` (rebuild may have retractions)
/// - `skip_history: false` (produce history sidecars for time-travel)
pub fn build_indexes_from_remapped_commits(
    commit_infos: &[crate::run_index::runs::spool::SortedCommitInfo],
    registry: &OTypeRegistry,
    config: &BuildConfig,
) -> io::Result<BuildResult> {
    use crate::run_index::runs::spool::{IdentityStringRemap, IdentitySubjectRemap};

    if let Some(stage) = &config.stage_marker {
        stage.store(BUILD_STAGE_REMAP, Ordering::Relaxed);
    }

    // Phase 1: Remap sorted commits → V2 run files (with op) for all 4 orders.
    // Since records are already globally remapped, use identity remap tables.
    // The op byte (assert=1, retract=0) is preserved from V1 records so that
    // the merge/build phase can filter out retract-winners.
    let remap_start = Instant::now();

    let orders = RunSortOrder::all_build_orders().to_vec();
    let mut writer = MultiOrderRunWriterWithOp::new(MultiOrderConfig {
        total_budget_bytes: config.run_budget_bytes,
        orders: orders.clone(),
        base_run_dir: config.run_dir.clone(),
    })?;

    let s_remap = IdentitySubjectRemap;
    let str_remap = IdentityStringRemap;
    let lang_remap: &[u16] = &[]; // language IDs already global

    let mut total_remapped = 0u64;
    for info in commit_infos {
        let count = remap_commit_to_runs_with_op(
            &info.path,
            info.record_count,
            &s_remap,
            &str_remap,
            lang_remap,
            config.g_id,
            registry,
            &mut writer,
        )?;
        total_remapped += count;

        if let Some(ref ctr) = config.remap_progress {
            ctr.fetch_add(count, Ordering::Relaxed);
        }
    }

    let _run_results = writer.finish()?;
    let remap_elapsed = remap_start.elapsed();

    // Phase 2: Build V3 indexes from run files.
    if let Some(stage) = &config.stage_marker {
        stage.store(BUILD_STAGE_MERGE, Ordering::Relaxed);
    }
    let build_start = Instant::now();

    let build_config = BuildAllConfig {
        base_run_dir: config.run_dir.clone(),
        index_dir: config.index_dir.clone(),
        leaflet_target_rows: config.leaflet_target_rows,
        leaf_target_rows: config.leaf_target_rows,
        zstd_level: config.zstd_level,
        skip_dedup: false,   // Rebuild: must deduplicate (max-t wins).
        skip_history: false, // Produce history sidecars for time-travel.
        g_id: config.g_id,
        progress: config.build_progress.clone(),
    };

    let order_results = build_all_indexes(&build_config).map_err(io::Error::other)?;

    let build_elapsed = build_start.elapsed();

    // Total rows from the POST result (canonical count).
    let total_rows = order_results
        .iter()
        .find(|(o, _)| *o == RunSortOrder::Post)
        .map(|(_, r)| r.total_rows)
        .unwrap_or_else(|| {
            order_results
                .first()
                .map(|(_, r)| r.total_rows)
                .unwrap_or(0)
        });

    Ok(BuildResult {
        order_results,
        total_rows,
        total_remapped,
        remap_elapsed,
        build_elapsed,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::run_index::runs::spool::SortedCommitWriterV2;
    use fluree_db_binary_index::format::leaf::{decode_leaf_dir_v3, decode_leaf_header_v3};
    use fluree_db_binary_index::format::run_record::RunRecord;
    use fluree_db_binary_index::format::run_record_v2::RunRecordV2;

    use fluree_db_core::subject_id::SubjectId;
    use fluree_db_core::value_id::{ObjKey, ObjKind};

    #[test]
    fn end_to_end_v3_build() {
        let dir = std::env::temp_dir().join("fluree_test_v3_e2e");
        let _ = std::fs::remove_dir_all(&dir);
        let run_dir = dir.join("runs");
        let index_dir = dir.join("index");
        std::fs::create_dir_all(&run_dir).unwrap();
        std::fs::create_dir_all(&index_dir).unwrap();

        // Create a minimal V2-native sorted commit file with a few records.
        let commit_path = dir.join("commit_00000.fsv2");

        // For this test, we write records directly in sorted order.
        // We need identity remap files too (just identity: index → same value).
        let records = vec![
            RunRecord::new(
                0,
                SubjectId(1),
                1,
                ObjKind::NUM_INT,
                ObjKey::encode_i64(10),
                1,
                true,
                3,
                0,
                None,
            ),
            RunRecord::new(
                0,
                SubjectId(2),
                1,
                ObjKind::NUM_INT,
                ObjKey::encode_i64(20),
                2,
                true,
                3,
                0,
                None,
            ),
            RunRecord::new(
                0,
                SubjectId(3),
                2,
                ObjKind::LEX_ID,
                ObjKey::encode_u32_id(5),
                3,
                true,
                1,
                0,
                None,
            ),
        ];

        let registry = OTypeRegistry::builtin_only();

        // Write V2-native sorted commit file.
        let mut spool = SortedCommitWriterV2::new(&commit_path, 0).unwrap();
        for rec in &records {
            spool.push(&RunRecordV2::from_v1(rec, &registry)).unwrap();
        }
        let spool_info = spool.finish().unwrap();

        // Write identity remap files.
        // Subject remap: 4 entries (0-indexed), identity mapping.
        let subj_remap_path = dir.join("subjects_00000.rmp");
        let subj_data: Vec<u8> = (0u64..4).flat_map(u64::to_le_bytes).collect();
        std::fs::write(&subj_remap_path, &subj_data).unwrap();

        // String remap: 10 entries, identity mapping.
        let str_remap_path = dir.join("strings_00000.rmp");
        let str_data: Vec<u8> = (0u32..10).flat_map(u32::to_le_bytes).collect();
        std::fs::write(&str_remap_path, &str_data).unwrap();

        let commits = vec![CommitInput {
            commit_path,
            record_count: spool_info.record_count,
            subject_remap_path: subj_remap_path,
            string_remap_path: str_remap_path,
            lang_remap: vec![],
            types_map_path: None,
        }];

        let config = BuildConfig {
            run_dir,
            index_dir: index_dir.clone(),
            g_id: 0,
            leaflet_target_rows: 100,
            leaf_target_rows: 1000,
            zstd_level: 1,
            run_budget_bytes: 256 * 1024,
            worker_count: 1,
            remap_progress: None,
            build_progress: None,
            stage_marker: None,
        };

        let (result, _spot_class_stats) =
            build_indexes_from_commits(&commits, &config, None).unwrap();

        // Should have results for all 4 orders.
        assert_eq!(result.order_results.len(), 4);
        assert_eq!(result.total_rows, 3);
        assert_eq!(result.total_remapped, 3);

        // Verify POST has predicate-homogeneous leaflets.
        let post_result = result
            .order_results
            .iter()
            .find(|(o, _)| *o == RunSortOrder::Post)
            .unwrap();
        let post_graphs = &post_result.1.graphs;
        assert_eq!(post_graphs.len(), 1);
        assert_eq!(post_graphs[0].g_id, 0);

        // Check the POST leaf has 2 leaflets (p_id=1 and p_id=2).
        let post_leaf = &post_graphs[0].leaf_infos[0];
        let leaf_bytes = std::fs::read(&post_leaf.leaf_path).unwrap();
        let header = decode_leaf_header_v3(&leaf_bytes).unwrap();
        assert_eq!(header.order, RunSortOrder::Post);
        let leaf_dir = decode_leaf_dir_v3(&leaf_bytes, &header).unwrap();
        assert_eq!(leaf_dir.len(), 2); // p_id=1 and p_id=2
        assert_eq!(leaf_dir[0].p_const, Some(1));
        assert_eq!(leaf_dir[1].p_const, Some(2));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn class_bitset_build_from_global_types() {
        let dir = tempfile::tempdir().unwrap();
        let types_path = dir.path().join("chunk_00000.types");

        // Write .types sidecar entries: (g_id: u16, s_id: u64, class_sid64: u64)
        // Subject 100 is class A (sid=1000), subject 200 is class A and class B (sid=2000).
        let class_a: u64 = 1000;
        let class_b: u64 = 2000;
        let entries: Vec<(u16, u64, u64)> =
            vec![(0, 100, class_a), (0, 200, class_a), (0, 200, class_b)];

        {
            let mut file = std::fs::File::create(&types_path).unwrap();
            for (g_id, s_id, c_id) in &entries {
                use std::io::Write;
                file.write_all(&g_id.to_le_bytes()).unwrap();
                file.write_all(&s_id.to_le_bytes()).unwrap();
                file.write_all(&c_id.to_le_bytes()).unwrap();
            }
        }

        let table = ClassBitsetTable::build_from_global_types(&[types_path])
            .unwrap()
            .expect("should produce a table");

        // Both classes should be mapped.
        assert_eq!(table.bit_to_class.len(), 2);
        assert!(table.bit_to_class.contains(&class_a));
        assert!(table.bit_to_class.contains(&class_b));

        // Subject 100: only class A.
        let bits_100 = table.get(0, 100);
        assert_ne!(bits_100, 0);
        // Exactly one bit set.
        assert_eq!(bits_100.count_ones(), 1);

        // Subject 200: both class A and class B.
        let bits_200 = table.get(0, 200);
        assert_eq!(bits_200.count_ones(), 2);

        // Unknown subject returns 0.
        assert_eq!(table.get(0, 999), 0);
        // Unknown graph returns 0.
        assert_eq!(table.get(5, 100), 0);
    }

    #[test]
    fn class_bitset_overflow_at_64_classes() {
        let dir = tempfile::tempdir().unwrap();
        let types_path = dir.path().join("overflow.types");

        // Write 65 distinct classes — the 65th should be silently dropped.
        {
            let mut file = std::fs::File::create(&types_path).unwrap();
            for class_idx in 0u64..65 {
                use std::io::Write;
                let g_id: u16 = 0;
                let s_id: u64 = class_idx + 1; // unique subject per class
                let c_id: u64 = 10_000 + class_idx;
                file.write_all(&g_id.to_le_bytes()).unwrap();
                file.write_all(&s_id.to_le_bytes()).unwrap();
                file.write_all(&c_id.to_le_bytes()).unwrap();
            }
        }

        let table = ClassBitsetTable::build_from_global_types(&[types_path])
            .unwrap()
            .expect("should produce a table");

        // Only 64 classes should be retained.
        assert_eq!(table.bit_to_class.len(), 64);

        // First 64 subjects should have a bit set.
        for s_id in 1u64..=64 {
            assert_ne!(table.get(0, s_id), 0, "subject {s_id} should be mapped");
        }

        // Subject 65 has the 65th class which overflowed — no bit set.
        assert_eq!(table.get(0, 65), 0);
    }

    #[test]
    fn class_bitset_no_types_files_returns_none() {
        let result = ClassBitsetTable::build_from_global_types(&[]).unwrap();
        assert!(result.is_none());
    }
}
