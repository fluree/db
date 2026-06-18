//! Incremental branch update for V3 index format (FBR3).
//!
//! Given an existing V3 branch manifest + sorted novelty records, produces a
//! new FBR3 branch manifest with updated leaves.
//!
//! ## Strategy
//!
//! 1. Decode the existing FBR3 manifest.
//! 2. Slice novelty to leaves using half-open boundary intervals.
//! 3. For each touched leaf: fetch bytes, call `update_leaf`, collect results.
//! 4. Untouched leaves: carry forward existing `LeafEntry` unchanged.
//! 5. Assemble updated `LeafEntry` list and build new FBR3 manifest.

use std::io;

use fluree_db_binary_index::format::branch::{
    build_branch_bytes, read_branch_from_bytes, BranchManifest, LeafEntry,
};
use fluree_db_binary_index::format::leaf::decode_leaf_header_v3;
use fluree_db_binary_index::format::run_record::RunSortOrder;
use fluree_db_binary_index::format::run_record_v2::{
    cmp_v2_for_order, read_ordered_key_v2, RunRecordV2,
};
use fluree_db_core::ContentId;
use rayon::prelude::*;

use super::incremental_leaf::{update_leaf, LeafUpdateInput, LeafUpdateOutput, NewLeafBlob};

/// Minimum number of *touched* leaves before fanning the per-leaf CoW out across
/// the shared rayon pool. Below this the per-leaf zstd re-encode is cheap enough
/// that the fan-out overhead (work-unit cloning + window assembly) outweighs the
/// gain, so we run the byte-identical serial loop. Steady-state incremental folds
/// touch a handful of leaves and stay serial; large catch-up folds parallelize.
const PARALLEL_BRANCH_MIN_TOUCHED_LEAVES: usize = 4;

// ============================================================================
// Configuration and output types
// ============================================================================

/// Configuration for a branch update.
pub struct BranchUpdateConfig {
    /// Sort order.
    pub order: RunSortOrder,
    /// Graph id.
    pub g_id: u16,
    /// Zstd compression level for re-encoded leaflets.
    pub zstd_level: i32,
    /// Target rows per leaflet.
    pub leaflet_target_rows: usize,
    /// Target rows per leaf.
    pub leaf_target_rows: usize,
}

/// Result of a branch update.
pub struct BranchUpdateResult {
    /// Updated leaf entries for the new branch manifest.
    pub leaf_entries: Vec<LeafEntry>,
    /// New leaf blobs to upload to CAS.
    pub new_leaf_blobs: Vec<NewLeafBlob>,
    /// CIDs of replaced leaves (for GC).
    pub replaced_leaf_cids: Vec<ContentId>,
    /// CIDs of replaced sidecars (for GC).
    pub replaced_sidecar_cids: Vec<ContentId>,
    /// Encoded FBR3 branch manifest bytes.
    pub branch_bytes: Vec<u8>,
    /// CID of the new branch manifest.
    pub branch_cid: ContentId,
}

/// Branch-assembly metadata from a streaming update — everything the caller
/// needs for root/branch assembly *except* the leaf/sidecar byte buffers,
/// which are handed off to the sink as they are produced (see
/// [`update_branch_streaming`]) so they never accumulate in RAM.
pub struct BranchUpdateMeta {
    /// Updated leaf entries for the new branch manifest.
    pub leaf_entries: Vec<LeafEntry>,
    /// CIDs of replaced leaves (for GC).
    pub replaced_leaf_cids: Vec<ContentId>,
    /// CIDs of replaced sidecars (for GC).
    pub replaced_sidecar_cids: Vec<ContentId>,
    /// Encoded FBR3 branch manifest bytes.
    pub branch_bytes: Vec<u8>,
    /// CID of the new branch manifest.
    pub branch_cid: ContentId,
    /// Number of new leaf blobs emitted to the sink.
    pub new_leaf_count: usize,
}

// ============================================================================
// Main entry point
// ============================================================================

/// Update a V3 branch with sorted novelty records.
///
/// `fetch_leaf` fetches leaf bytes by CID (synchronous).
/// `fetch_sidecar` fetches sidecar bytes by CID (synchronous; returns None if absent).
///
/// Leaves are processed sequentially. The caller is responsible for prefetching
/// touched leaves if parallelism is desired.
pub fn update_branch<F, G>(
    existing_branch_bytes: &[u8],
    novelty: &[RunRecordV2],
    novelty_ops: &[u8],
    config: &BranchUpdateConfig,
    fetch_leaf: &F,
    fetch_sidecar: &G,
) -> io::Result<BranchUpdateResult>
where
    F: Fn(&ContentId) -> io::Result<Vec<u8>>,
    G: Fn(&ContentId) -> io::Result<Option<Vec<u8>>>,
{
    // Non-streaming convenience wrapper: collect the emitted blobs into a Vec.
    // The byte-for-byte output is identical to the streaming core, so CIDs and
    // branch assembly are unchanged. Used by direct callers/tests that want the
    // whole result in memory; the production pipeline uses the streaming core.
    let mut new_blobs: Vec<NewLeafBlob> = Vec::new();
    let meta = update_branch_streaming(
        existing_branch_bytes,
        novelty,
        novelty_ops,
        config,
        fetch_leaf,
        fetch_sidecar,
        &mut |blob| {
            new_blobs.push(blob);
            Ok(())
        },
    )?;
    Ok(BranchUpdateResult {
        leaf_entries: meta.leaf_entries,
        new_leaf_blobs: new_blobs,
        replaced_leaf_cids: meta.replaced_leaf_cids,
        replaced_sidecar_cids: meta.replaced_sidecar_cids,
        branch_bytes: meta.branch_bytes,
        branch_cid: meta.branch_cid,
    })
}

/// Streaming variant of [`update_branch`]: each produced [`NewLeafBlob`] is
/// handed to `sink` as soon as its branch-entry metadata has been read, so the
/// leaf/sidecar byte buffers never accumulate. Returns only the small
/// branch-assembly metadata ([`BranchUpdateMeta`]).
///
/// The leaf *encoding* is identical to [`update_branch`] — only when bytes are
/// released differs — so the resulting branch/root CIDs are bit-identical.
#[allow(clippy::too_many_arguments)]
pub fn update_branch_streaming<F, G, S>(
    existing_branch_bytes: &[u8],
    novelty: &[RunRecordV2],
    novelty_ops: &[u8],
    config: &BranchUpdateConfig,
    fetch_leaf: &F,
    fetch_sidecar: &G,
    sink: &mut S,
) -> io::Result<BranchUpdateMeta>
where
    F: Fn(&ContentId) -> io::Result<Vec<u8>>,
    G: Fn(&ContentId) -> io::Result<Option<Vec<u8>>>,
    S: FnMut(NewLeafBlob) -> io::Result<()>,
{
    let order = config.order;

    // Catch mis-sorted novelty early — silent mis-slicing is brutal to debug.
    debug_assert!(
        novelty
            .windows(2)
            .all(|w| cmp_v2_for_order(order)(&w[0], &w[1]) != std::cmp::Ordering::Greater),
        "novelty must be sorted by the branch's sort order ({order:?})"
    );

    let manifest = read_branch_from_bytes(existing_branch_bytes)?;
    let cmp = cmp_v2_for_order(order);

    // Slice novelty to leaves.
    let novelty_slices = slice_novelty_to_leaves(novelty, novelty_ops, &manifest, cmp);

    // Touched leaves: the per-leaf `update_leaf` (decode → merge → zstd re-encode)
    // is the CPU-bound work. On a multi-core box with enough touched leaves we fan
    // it out across the shared global rayon pool (sized to available_parallelism,
    // shared across all callers → no oversubscription on small boxes), otherwise
    // we run the byte-identical serial loop. Either path produces leaf_entries /
    // replaced-CID lists / sink emissions in ascending manifest index, so the
    // assembled branch bytes (and CID) are bit-identical to the serial version.
    let touched_count = novelty_slices.iter().filter(|(n, _)| !n.is_empty()).count();
    let ncpu = std::thread::available_parallelism()
        .map(std::num::NonZeroUsize::get)
        .unwrap_or(1);
    let mode = if ncpu < 2 || touched_count < PARALLEL_BRANCH_MIN_TOUCHED_LEAVES {
        DrainMode::Serial
    } else {
        DrainMode::Parallel { window: ncpu }
    };

    drain_branch(
        &manifest,
        &novelty_slices,
        config,
        fetch_leaf,
        fetch_sidecar,
        mode,
        sink,
    )
}

/// Which per-leaf CoW drain to run. Split out so tests can force either path and
/// assert byte-identical output regardless of the host's core count.
#[derive(Clone, Copy)]
enum DrainMode {
    Serial,
    Parallel { window: usize },
}

/// Run the chosen drain and assemble the branch manifest. Both drains append to a
/// shared `BranchAcc` in ascending manifest index, so the branch bytes/CID are
/// independent of the mode.
#[allow(clippy::too_many_arguments)]
fn drain_branch<F, G, S>(
    manifest: &BranchManifest,
    novelty_slices: &[(&[RunRecordV2], &[u8])],
    config: &BranchUpdateConfig,
    fetch_leaf: &F,
    fetch_sidecar: &G,
    mode: DrainMode,
    sink: &mut S,
) -> io::Result<BranchUpdateMeta>
where
    F: Fn(&ContentId) -> io::Result<Vec<u8>>,
    G: Fn(&ContentId) -> io::Result<Option<Vec<u8>>>,
    S: FnMut(NewLeafBlob) -> io::Result<()>,
{
    let mut acc = BranchAcc::with_capacity(manifest.leaves.len() + 4);

    match mode {
        DrainMode::Serial => update_branch_serial(
            manifest,
            novelty_slices,
            config,
            fetch_leaf,
            fetch_sidecar,
            &mut acc,
            sink,
        )?,
        DrainMode::Parallel { window } => update_branch_parallel(
            manifest,
            novelty_slices,
            config,
            fetch_leaf,
            fetch_sidecar,
            window,
            &mut acc,
            sink,
        )?,
    }

    let BranchAcc {
        leaf_entries,
        replaced_leaf_cids,
        replaced_sidecar_cids,
        new_leaf_count,
    } = acc;

    let branch_bytes = build_branch_bytes(config.order, config.g_id, &leaf_entries);
    let branch_cid = compute_branch_cid(&branch_bytes);

    Ok(BranchUpdateMeta {
        leaf_entries,
        replaced_leaf_cids,
        replaced_sidecar_cids,
        branch_bytes,
        branch_cid,
        new_leaf_count,
    })
}

// ============================================================================
// Per-leaf CoW drains (serial + windowed-parallel)
// ============================================================================

/// In-order accumulator for the per-leaf CoW results. Both the serial and the
/// parallel drain append to this strictly in ascending manifest index, so the
/// final `leaf_entries` / replaced-CID lists are identical regardless of path.
struct BranchAcc {
    leaf_entries: Vec<LeafEntry>,
    replaced_leaf_cids: Vec<ContentId>,
    replaced_sidecar_cids: Vec<ContentId>,
    new_leaf_count: usize,
}

impl BranchAcc {
    fn with_capacity(leaves: usize) -> Self {
        Self {
            leaf_entries: Vec::with_capacity(leaves),
            replaced_leaf_cids: Vec::new(),
            replaced_sidecar_cids: Vec::new(),
            new_leaf_count: 0,
        }
    }
}

/// Build the `LeafUpdateInput` for a touched leaf. The effective leaf target is
/// bumped so a touched leaf is never split into multiple new leaves — preserving
/// branch structure for CID stability (identical to the serial-loop logic).
fn make_leaf_update_input<'a>(
    leaf_bytes: &'a [u8],
    sidecar_bytes: Option<&'a [u8]>,
    nov_slice: &'a [RunRecordV2],
    ops_slice: &'a [u8],
    config: &BranchUpdateConfig,
) -> io::Result<LeafUpdateInput<'a>> {
    let existing_header = decode_leaf_header_v3(leaf_bytes)?;
    let effective_leaf_target_rows = (existing_header.total_rows as usize)
        .saturating_add(nov_slice.len())
        .saturating_add(1);
    Ok(LeafUpdateInput {
        leaf_bytes,
        novelty: nov_slice,
        novelty_ops: ops_slice,
        order: config.order,
        g_id: config.g_id,
        zstd_level: config.zstd_level,
        leaflet_target_rows: config.leaflet_target_rows,
        leaf_target_rows: config.leaf_target_rows.max(effective_leaf_target_rows),
        sidecar_bytes,
    })
}

/// Append one touched leaf's `update_leaf` output to `acc` and drain its blobs to
/// `sink`, in manifest order. `existing` supplies the replaced CIDs for GC. This
/// is the single in-order bookkeeping path shared by both drains, so the produced
/// branch bytes are byte-identical regardless of how `output` was computed.
fn drain_touched_output<S>(
    order: RunSortOrder,
    existing: &LeafEntry,
    output: LeafUpdateOutput,
    acc: &mut BranchAcc,
    sink: &mut S,
) -> io::Result<()>
where
    S: FnMut(NewLeafBlob) -> io::Result<()>,
{
    acc.replaced_leaf_cids.push(existing.leaf_cid.clone());
    if let Some(sc_cid) = &existing.sidecar_cid {
        acc.replaced_sidecar_cids.push(sc_cid.clone());
    }

    for new_leaf in output.leaves {
        let header = decode_leaf_header_v3(&new_leaf.info.leaf_bytes)?;
        let first_key = read_ordered_key_v2(order, &header.first_key);
        let last_key = read_ordered_key_v2(order, &header.last_key);

        acc.leaf_entries.push(LeafEntry {
            first_key,
            last_key,
            row_count: new_leaf.info.total_rows,
            leaf_cid: new_leaf.info.leaf_cid.clone(),
            sidecar_cid: new_leaf.info.sidecar_cid.clone(),
        });

        acc.new_leaf_count += 1;
        sink(new_leaf)?;
    }
    Ok(())
}

/// Serial per-leaf CoW: the original loop. Untouched leaves carry forward; touched
/// leaves are fetched, updated, and drained in manifest order.
#[allow(clippy::too_many_arguments)]
fn update_branch_serial<F, G, S>(
    manifest: &BranchManifest,
    novelty_slices: &[(&[RunRecordV2], &[u8])],
    config: &BranchUpdateConfig,
    fetch_leaf: &F,
    fetch_sidecar: &G,
    acc: &mut BranchAcc,
    sink: &mut S,
) -> io::Result<()>
where
    F: Fn(&ContentId) -> io::Result<Vec<u8>>,
    G: Fn(&ContentId) -> io::Result<Option<Vec<u8>>>,
    S: FnMut(NewLeafBlob) -> io::Result<()>,
{
    for (i, (nov_slice, ops_slice)) in novelty_slices.iter().enumerate() {
        let existing = &manifest.leaves[i];
        if nov_slice.is_empty() {
            acc.leaf_entries.push(existing.clone());
            continue;
        }

        let leaf_bytes = fetch_leaf(&existing.leaf_cid)?;
        let sidecar_bytes = match &existing.sidecar_cid {
            Some(cid) => fetch_sidecar(cid)?,
            None => None,
        };
        let input = make_leaf_update_input(
            &leaf_bytes,
            sidecar_bytes.as_deref(),
            nov_slice,
            ops_slice,
            config,
        )?;
        let output = update_leaf(&input)?;
        drain_touched_output(config.order, existing, output, acc, sink)?;
    }
    Ok(())
}

/// A touched-leaf work unit: owns the prefetched leaf/sidecar bytes so the rayon
/// closure can build a `LeafUpdateInput` over read-only data and call `update_leaf`
/// off-thread. `manifest_index` keeps the result reassemblable in branch order.
struct LeafWorkUnit<'a> {
    manifest_index: usize,
    leaf_bytes: Vec<u8>,
    sidecar_bytes: Option<Vec<u8>>,
    nov_slice: &'a [RunRecordV2],
    ops_slice: &'a [u8],
}

/// Windowed parallel per-leaf CoW. Iterates the novelty slices in manifest order
/// in windows of `ncpu`; within a window the touched leaves' `update_leaf` calls
/// run on the shared global rayon pool, then the window is drained strictly in
/// ascending manifest index (carry-forward untouched leaves, bookkeeping, sink).
///
/// - Order (INV1): per-unit outputs carry `manifest_index` and the window is
///   drained in index order via [`drain_touched_output`]; nothing is pushed from
///   workers. Untouched leaves are carried forward at their exact index.
/// - Bit-identity (INV2): each worker calls the same pure `update_leaf` with the
///   same input the serial loop builds (`make_leaf_update_input`), so blob bytes /
///   CIDs are identical; the in-order drain yields identical `leaf_entries`.
/// - Bounded memory (INV4): at most one window (`≤ ncpu` decoded+re-encoded leaf
///   sets) is in flight; the serial drain still calls `sink` (the bounded mpsc /
///   `blocking_send`) one blob at a time, so the uploader gates production rate.
/// - Shared CPU gate: `into_par_iter` runs on the process-global rayon pool sized
///   to available_parallelism and shared across all branch tasks, so the four
///   concurrent Phase 2 tasks cannot collectively exceed ~ncpu busy threads.
#[allow(clippy::too_many_arguments)]
fn update_branch_parallel<F, G, S>(
    manifest: &BranchManifest,
    novelty_slices: &[(&[RunRecordV2], &[u8])],
    config: &BranchUpdateConfig,
    fetch_leaf: &F,
    fetch_sidecar: &G,
    window: usize,
    acc: &mut BranchAcc,
    sink: &mut S,
) -> io::Result<()>
where
    F: Fn(&ContentId) -> io::Result<Vec<u8>>,
    G: Fn(&ContentId) -> io::Result<Option<Vec<u8>>>,
    S: FnMut(NewLeafBlob) -> io::Result<()>,
{
    let window = window.max(1);
    let span = tracing::Span::current();
    let n = novelty_slices.len();
    let mut i = 0;

    while i < n {
        let window_end = (i + window).min(n);

        // (1) Driver pre-pass: materialize a work unit (with its prefetched bytes)
        // for each touched leaf in this window. Fetches read the in-memory maps
        // only. Untouched leaves are handled in the post-pass to keep a single
        // ascending push order.
        let mut units: Vec<LeafWorkUnit<'_>> = Vec::with_capacity(window_end - i);
        for (offset, ((nov_slice, ops_slice), existing)) in novelty_slices[i..window_end]
            .iter()
            .zip(&manifest.leaves[i..window_end])
            .enumerate()
        {
            if nov_slice.is_empty() {
                continue;
            }
            let leaf_bytes = fetch_leaf(&existing.leaf_cid)?;
            let sidecar_bytes = match &existing.sidecar_cid {
                Some(cid) => fetch_sidecar(cid)?,
                None => None,
            };
            units.push(LeafWorkUnit {
                manifest_index: i + offset,
                leaf_bytes,
                sidecar_bytes,
                nov_slice,
                ops_slice,
            });
        }

        // (2) Parallel CPU/zstd-bound map over the window's touched leaves on the
        // shared rayon pool. Re-enter the parent span per worker so the CoW work
        // stays in the trace tree (mirrors fast_path_common::parallel_map_pooled).
        let outputs: Vec<io::Result<(usize, LeafUpdateOutput)>> = units
            .into_par_iter()
            .map(|unit| {
                let _guard = span.enter();
                let input = make_leaf_update_input(
                    &unit.leaf_bytes,
                    unit.sidecar_bytes.as_deref(),
                    unit.nov_slice,
                    unit.ops_slice,
                    config,
                )?;
                let output = update_leaf(&input)?;
                Ok((unit.manifest_index, output))
            })
            .collect();

        // Index the touched outputs by their window offset for an in-order drain.
        let mut outputs_by_offset: Vec<Option<LeafUpdateOutput>> =
            (i..window_end).map(|_| None).collect();
        for out in outputs {
            let (idx, output) = out?;
            outputs_by_offset[idx - i] = Some(output);
        }

        // (3) Driver post-pass: drain the window strictly in manifest order —
        // carry forward untouched leaves, drain touched outputs (bookkeeping +
        // sink) one at a time. This keeps the sink serial and positional.
        for (offset, output_slot) in outputs_by_offset.into_iter().enumerate() {
            let idx = i + offset;
            let existing = &manifest.leaves[idx];
            match output_slot {
                None => acc.leaf_entries.push(existing.clone()),
                Some(output) => drain_touched_output(config.order, existing, output, acc, sink)?,
            }
        }

        i = window_end;
    }
    Ok(())
}

/// The leaves — and their sidecars — that [`update_branch`] will fetch for the
/// given sorted novelty: exactly the leaves whose novelty slice is non-empty.
///
/// The caller prefetches these (asynchronously) so `update_branch`'s
/// `fetch_leaf`/`fetch_sidecar` closures can read them from memory instead of
/// blocking on CAS I/O from inside a `spawn_blocking` closure — on a
/// blocking-pool thread a `block_on(S3)` has no tokio worker to drive its
/// reactor, which wedges under slow remote storage. Uses the same slicing as
/// `update_branch`, so the prefetched set matches the leaves it requests
/// exactly.
pub fn touched_leaf_refs(
    existing_branch_bytes: &[u8],
    novelty: &[RunRecordV2],
    novelty_ops: &[u8],
    order: RunSortOrder,
) -> io::Result<Vec<(ContentId, Option<ContentId>)>> {
    let manifest = read_branch_from_bytes(existing_branch_bytes)?;
    let cmp = cmp_v2_for_order(order);
    let slices = slice_novelty_to_leaves(novelty, novelty_ops, &manifest, cmp);
    let mut refs = Vec::new();
    for (i, (nov_slice, _ops)) in slices.iter().enumerate() {
        if !nov_slice.is_empty() {
            let leaf = &manifest.leaves[i];
            refs.push((leaf.leaf_cid.clone(), leaf.sidecar_cid.clone()));
        }
    }
    Ok(refs)
}

// ============================================================================
// Novelty slicing
// ============================================================================

/// Slice novelty records to leaves using half-open boundary intervals.
///
/// Leaf 0: (-∞, leaf[1].first_key)
/// Leaf i: [leaf[i].first_key, leaf[i+1].first_key)
/// Last:   [leaf[last].first_key, +∞)
fn slice_novelty_to_leaves<'a>(
    novelty: &'a [RunRecordV2],
    ops: &'a [u8],
    manifest: &BranchManifest,
    cmp: fn(&RunRecordV2, &RunRecordV2) -> std::cmp::Ordering,
) -> Vec<(&'a [RunRecordV2], &'a [u8])> {
    let n_leaves = manifest.leaves.len();
    if n_leaves == 0 {
        return vec![];
    }
    if n_leaves == 1 {
        return vec![(novelty, ops)];
    }

    let mut result = Vec::with_capacity(n_leaves);
    let mut remaining_records = novelty;
    let mut remaining_ops = ops;

    for i in 0..n_leaves {
        if i + 1 < n_leaves {
            let next_first = &manifest.leaves[i + 1].first_key;
            let split_pos = remaining_records
                .partition_point(|rec| cmp(rec, next_first) == std::cmp::Ordering::Less);

            let (this_recs, rest_recs) = remaining_records.split_at(split_pos);
            let (this_ops, rest_ops) = remaining_ops.split_at(split_pos);
            result.push((this_recs, this_ops));
            remaining_records = rest_recs;
            remaining_ops = rest_ops;
        } else {
            result.push((remaining_records, remaining_ops));
        }
    }

    result
}

// ============================================================================
// Helpers
// ============================================================================

fn compute_branch_cid(bytes: &[u8]) -> ContentId {
    let hex_digest = fluree_db_core::sha256_hex(bytes);
    ContentId::from_hex_digest(
        fluree_db_core::content_kind::CODEC_FLUREE_INDEX_BRANCH,
        &hex_digest,
    )
    .expect("valid SHA-256 hex digest")
}

#[cfg(test)]
mod parity_tests {
    use super::*;
    use fluree_db_binary_index::format::leaf::LeafWriter;
    use fluree_db_core::o_type::OType;
    use fluree_db_core::subject_id::SubjectId;
    use fluree_db_core::value_id::ObjKey;
    use std::collections::HashMap;

    const OI_NONE: u32 = u32::MAX;

    fn rec(s_id: u64, p_id: u32, val: i64, t: u32) -> RunRecordV2 {
        RunRecordV2 {
            s_id: SubjectId(s_id),
            o_key: ObjKey::encode_i64(val).as_u64(),
            p_id,
            t,
            o_i: OI_NONE,
            o_type: OType::XSD_INTEGER.as_u16(),
            g_id: 0,
        }
    }

    /// Run a branch update through an explicitly-forced drain mode, collecting the
    /// emitted blobs in order so the whole result can be compared byte-for-byte.
    fn run_mode(
        branch_bytes: &[u8],
        novelty: &[RunRecordV2],
        ops: &[u8],
        config: &BranchUpdateConfig,
        leaf_map: &HashMap<ContentId, Vec<u8>>,
        sidecar_map: &HashMap<ContentId, Vec<u8>>,
        mode: DrainMode,
    ) -> (BranchUpdateMeta, Vec<NewLeafBlob>) {
        let manifest = read_branch_from_bytes(branch_bytes).unwrap();
        let cmp = cmp_v2_for_order(config.order);
        let slices = slice_novelty_to_leaves(novelty, ops, &manifest, cmp);
        let mut blobs: Vec<NewLeafBlob> = Vec::new();
        let meta = drain_branch(
            &manifest,
            &slices,
            config,
            &|cid: &ContentId| Ok(leaf_map.get(cid).cloned().unwrap()),
            &|cid: &ContentId| Ok(sidecar_map.get(cid).cloned()),
            mode,
            &mut |blob| {
                blobs.push(blob);
                Ok(())
            },
        )
        .unwrap();
        (meta, blobs)
    }

    /// Many touched leaves spanning several windows: the serial and the
    /// windowed-parallel drains MUST produce bit-identical branch bytes, CID,
    /// leaf_entries, replaced-CID lists, and emitted-blob bytes (INV1+INV2+INV3).
    #[test]
    fn serial_and_parallel_drains_are_bit_identical() {
        // Force many small leaves: 5 rows/leaflet, 25 rows/leaf, plenty of leaves.
        let mut writer = LeafWriter::new(RunSortOrder::Spot, 5, 25, 1);
        let base: Vec<RunRecordV2> = (0..600).map(|s| rec(s, 1, s as i64, 1)).collect();
        for r in &base {
            writer.push_record(*r).unwrap();
        }
        let infos = writer.finish().unwrap();
        assert!(infos.len() >= 8, "need many leaves to span windows");

        let mut leaf_map: HashMap<ContentId, Vec<u8>> = HashMap::new();
        let sidecar_map: HashMap<ContentId, Vec<u8>> = HashMap::new();
        let mut leaves: Vec<LeafEntry> = Vec::with_capacity(infos.len());
        for info in infos {
            leaf_map.insert(info.leaf_cid.clone(), info.leaf_bytes);
            leaves.push(LeafEntry {
                first_key: info.first_key,
                last_key: info.last_key,
                row_count: info.total_rows,
                leaf_cid: info.leaf_cid,
                sidecar_cid: None,
            });
        }
        let branch_bytes = build_branch_bytes(RunSortOrder::Spot, 0, &leaves);

        // Touch many leaves (one assert per ~50 subjects → spread across leaves),
        // interleaving touched and untouched leaves so carry-forward ordering is
        // exercised. Novelty must be sorted by SPOT order (subject id).
        let mut novelty: Vec<RunRecordV2> = Vec::new();
        let mut ops: Vec<u8> = Vec::new();
        for s in (0..600).step_by(37) {
            novelty.push(rec(s, 2, (s as i64) * 10 + 7, 2));
            ops.push(1u8);
        }
        assert!(novelty.len() >= PARALLEL_BRANCH_MIN_TOUCHED_LEAVES);

        let config = BranchUpdateConfig {
            order: RunSortOrder::Spot,
            g_id: 0,
            zstd_level: 1,
            leaflet_target_rows: 50,
            leaf_target_rows: 200,
        };

        let (serial_meta, serial_blobs) = run_mode(
            &branch_bytes,
            &novelty,
            &ops,
            &config,
            &leaf_map,
            &sidecar_map,
            DrainMode::Serial,
        );
        // Use a window smaller than the touched count so multiple windows run.
        let (par_meta, par_blobs) = run_mode(
            &branch_bytes,
            &novelty,
            &ops,
            &config,
            &leaf_map,
            &sidecar_map,
            DrainMode::Parallel { window: 3 },
        );

        assert_eq!(
            serial_meta.branch_cid, par_meta.branch_cid,
            "branch CID must be identical across drains"
        );
        assert_eq!(
            serial_meta.branch_bytes, par_meta.branch_bytes,
            "branch bytes must be byte-identical across drains"
        );
        assert_eq!(serial_meta.leaf_entries.len(), par_meta.leaf_entries.len());
        for (a, b) in serial_meta
            .leaf_entries
            .iter()
            .zip(par_meta.leaf_entries.iter())
        {
            assert_eq!(a.leaf_cid, b.leaf_cid, "leaf_entries order/CID must match");
            assert_eq!(a.first_key, b.first_key);
            assert_eq!(a.last_key, b.last_key);
            assert_eq!(a.row_count, b.row_count);
            assert_eq!(a.sidecar_cid, b.sidecar_cid);
        }
        assert_eq!(
            serial_meta.replaced_leaf_cids, par_meta.replaced_leaf_cids,
            "replaced leaf CID list must match in order"
        );
        assert_eq!(
            serial_meta.replaced_sidecar_cids, par_meta.replaced_sidecar_cids,
            "replaced sidecar CID list must match in order"
        );
        assert_eq!(serial_meta.new_leaf_count, par_meta.new_leaf_count);

        // Emitted blobs: same count, same order, same bytes/CIDs.
        assert_eq!(serial_blobs.len(), par_blobs.len());
        for (a, b) in serial_blobs.iter().zip(par_blobs.iter()) {
            assert_eq!(a.info.leaf_cid, b.info.leaf_cid);
            assert_eq!(a.info.leaf_bytes, b.info.leaf_bytes);
            assert_eq!(a.info.sidecar_cid, b.info.sidecar_cid);
            assert_eq!(a.info.sidecar_bytes, b.info.sidecar_bytes);
        }
    }

    /// A window of exactly 1 (degenerate small-box case) must still be identical
    /// to the serial drain and to a multi-leaf window.
    #[test]
    fn window_size_does_not_affect_output() {
        let mut writer = LeafWriter::new(RunSortOrder::Spot, 5, 25, 1);
        let base: Vec<RunRecordV2> = (0..300).map(|s| rec(s, 1, s as i64, 1)).collect();
        for r in &base {
            writer.push_record(*r).unwrap();
        }
        let infos = writer.finish().unwrap();
        let mut leaf_map: HashMap<ContentId, Vec<u8>> = HashMap::new();
        let sidecar_map: HashMap<ContentId, Vec<u8>> = HashMap::new();
        let mut leaves: Vec<LeafEntry> = Vec::new();
        for info in infos {
            leaf_map.insert(info.leaf_cid.clone(), info.leaf_bytes);
            leaves.push(LeafEntry {
                first_key: info.first_key,
                last_key: info.last_key,
                row_count: info.total_rows,
                leaf_cid: info.leaf_cid,
                sidecar_cid: None,
            });
        }
        let branch_bytes = build_branch_bytes(RunSortOrder::Spot, 0, &leaves);

        let mut novelty: Vec<RunRecordV2> = Vec::new();
        let mut ops: Vec<u8> = Vec::new();
        for s in (0..300).step_by(23) {
            novelty.push(rec(s, 3, (s as i64) + 1, 2));
            ops.push(1u8);
        }
        let config = BranchUpdateConfig {
            order: RunSortOrder::Spot,
            g_id: 0,
            zstd_level: 1,
            leaflet_target_rows: 50,
            leaf_target_rows: 200,
        };

        let serial = run_mode(
            &branch_bytes,
            &novelty,
            &ops,
            &config,
            &leaf_map,
            &sidecar_map,
            DrainMode::Serial,
        )
        .0;
        for w in [1usize, 2, 5, 64] {
            let par = run_mode(
                &branch_bytes,
                &novelty,
                &ops,
                &config,
                &leaf_map,
                &sidecar_map,
                DrainMode::Parallel { window: w },
            )
            .0;
            assert_eq!(
                serial.branch_cid, par.branch_cid,
                "window={w} must not change branch CID"
            );
            assert_eq!(serial.branch_bytes, par.branch_bytes, "window={w}");
            assert_eq!(serial.replaced_leaf_cids, par.replaced_leaf_cids);
        }
    }
}
