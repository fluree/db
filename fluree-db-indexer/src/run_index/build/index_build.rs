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
use crate::run_index::runs::streaming_reader::StreamingRunReader;
use fluree_db_binary_index::format::branch::{build_branch_bytes, LeafEntry};
use fluree_db_binary_index::format::leaf::{LeafInfo, LeafWriter};
use fluree_db_binary_index::format::run_record::RunSortOrder;
use fluree_db_binary_index::format::run_record_v2::cmp_v2_for_order;
use fluree_db_core::ContentId;
use fluree_db_core::GraphId;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const PROGRESS_BATCH_SIZE: u64 = 4096;

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
    let run_paths = discover_run_files_v2(&config.run_dir)?;
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
    let order_name = order.dir_name();

    // V2 builds are graph-scoped: all records in the run directory belong
    // to config.g_id. No graph transition detection needed.
    let g_id = config.g_id;
    create_graph_dir(&config.index_dir, g_id, order_name)?;

    let mut writer = LeafWriter::new(
        order,
        config.leaflet_target_rows,
        config.leaf_target_rows,
        config.zstd_level,
    );
    writer.set_skip_history(config.skip_history);

    let mut total_rows: u64 = 0;
    let mut progress_batch: u64 = 0;

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
        } else if config.skip_history {
            // Rebuild without history: dedup but discard non-winners.
            let Some((record, op)) = merge.next_deduped()? else {
                break;
            };
            if op == 0 {
                continue;
            }
            writer.push_record(record)?;
        } else {
            // Rebuild with history: dedup and capture non-winners as sidecar entries.
            let Some((record, op, history)) = merge.next_deduped_with_history()? else {
                break;
            };
            // Push history entries for non-winners (these become sidecar segments).
            for (hist_rec, hist_op) in &history {
                writer.push_history_entry(
                    fluree_db_binary_index::format::history_sidecar::HistEntryV2 {
                        s_id: hist_rec.s_id,
                        p_id: hist_rec.p_id,
                        o_type: hist_rec.o_type,
                        o_key: hist_rec.o_key,
                        o_i: hist_rec.o_i,
                        t: hist_rec.t,
                        op: *hist_op,
                    },
                );
            }
            if op == 0 {
                // Retract-winner: don't push to latest-state, but history is already recorded.
                // Also push a history-only entry for the retract-winner itself.
                writer.push_history_entry(
                    fluree_db_binary_index::format::history_sidecar::HistEntryV2 {
                        s_id: record.s_id,
                        p_id: record.p_id,
                        o_type: record.o_type,
                        o_key: record.o_key,
                        o_i: record.o_i,
                        t: record.t,
                        op,
                    },
                );
                // Don't count retract-winners in total_rows (they're not in latest-state).
                // Only track progress for the UI.
                progress_batch += 1;
                if progress_batch >= PROGRESS_BATCH_SIZE {
                    if let Some(ref ctr) = config.progress {
                        ctr.fetch_add(progress_batch, Ordering::Relaxed);
                    }
                    progress_batch = 0;
                }
                continue;
            }
            writer.push_record(record)?;
        }

        total_rows += 1;
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

    let result = finish_graph_v2(g_id, order, writer, &config.index_dir, order_name)?;
    let graph_results = vec![result];

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

pub(crate) fn finish_graph_v2(
    g_id: GraphId,
    order: RunSortOrder,
    writer: LeafWriter,
    index_dir: &Path,
    order_name: &str,
) -> io::Result<GraphIndexResult> {
    let graph_dir = index_dir.join(format!("graph_{g_id}/{order_name}"));
    let leaf_infos = writer.finish()?;

    let total_rows: u64 = leaf_infos.iter().map(|l| l.total_rows).sum();

    // Write leaf + sidecar blobs to disk (content-addressed), then retain only
    // metadata + paths so the import build doesn't keep all FLI3/FHS1 bytes in RAM.
    let persisted_leaf_infos: Vec<PersistedLeafInfo> = leaf_infos
        .into_iter()
        .map(|info| -> io::Result<PersistedLeafInfo> {
            let LeafInfo {
                leaf_cid,
                leaf_bytes,
                sidecar_cid,
                sidecar_bytes,
                total_rows,
                first_key,
                last_key,
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
            })
        })
        .collect::<io::Result<Vec<_>>>()?;

    // Build branch manifest entries.
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
}

/// Build V2 indexes for all four orders from a base run directory.
///
/// Expects per-order subdirectories: `base_run_dir/{spot,psot,post,opst}/`.
/// Each subdirectory contains V2 run files sorted in that order.
pub fn build_all_indexes(
    config: &BuildAllConfig,
) -> Result<Vec<(RunSortOrder, IndexBuildResult)>, IndexBuildError> {
    let orders = RunSortOrder::all_build_orders();
    let mut results = Vec::with_capacity(orders.len());

    for &order in orders {
        let run_dir = config.base_run_dir.join(order.dir_name());
        if !run_dir.exists() {
            continue;
        }
        let order_start = Instant::now();
        let run_count = discover_run_files_v2(&run_dir)?.len();
        tracing::info!(
            order = order.dir_name(),
            run_count,
            run_dir = %run_dir.display(),
            "starting order index build"
        );

        // Import progress wants to reflect all order builds, not just a single
        // representative order, so attach the shared counter to every build.
        let order_progress = config.progress.clone();

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
            progress: order_progress,
        };

        let result = build_index(&order_config)?;
        tracing::info!(
            order = order.dir_name(),
            total_rows = result.total_rows,
            graphs = result.graphs.len(),
            elapsed_ms = order_start.elapsed().as_millis(),
            "completed order index build"
        );
        results.push((order, result));
    }

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
