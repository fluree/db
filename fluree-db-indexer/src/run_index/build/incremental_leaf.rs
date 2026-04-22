//! Incremental leaf update for V3 index format (FLI3).
//!
//! Given an existing V3 leaf blob + sorted novelty records, produces one or more
//! new V3 leaf blobs with the novelty merged in.
//!
//! ## Strategy
//!
//! 1. Decode the FLI3 header and leaflet directory.
//! 2. Slice novelty to leaflets using half-open boundary intervals.
//! 3. Untouched leaflets: passthrough (raw `EncodedLeaflet` + payload bytes).
//! 4. Touched leaflets: decode columns → `merge_novelty` → re-encode via
//!    `encode_leaflet` (one encode per segmentation-safe chunk).
//! 5. Assemble all leaflets into new leaf blob(s) via `build_leaf_blob`.
//!
//! Empty-after-retract leaflets with remaining history are valid in V3 (unlike V5).
//! Their `row_count=0` but `history_offset/len/min_t/max_t` are preserved so
//! time-travel replay can discover them.

use std::io;

use fluree_db_binary_index::format::history_sidecar::{
    decode_history_segment, HistEntryV2, HistSidecarBuilder, HistorySegmentRef,
};
use fluree_db_binary_index::format::leaf::{
    build_leaf_blob_raw_keys, compute_cid_leaf, compute_cid_sidecar, decode_leaf_dir_v3_with_base,
    decode_leaf_header_v3, DecodedLeafDirV3, LeafInfo, LeafletDirEntryV3,
};
use fluree_db_binary_index::format::leaflet::encode_leaflet;
use fluree_db_binary_index::format::run_record::RunSortOrder;
use fluree_db_binary_index::format::run_record_v2::{
    write_ordered_key_v2, RunRecordV2, ORDERED_KEY_V2_SIZE,
};
use fluree_db_binary_index::read::column_loader::load_leaflet_columns;
use fluree_db_binary_index::read::column_types::ColumnProjection;

use super::novelty_merge::{merge_novelty, MergeInput, MergeOutput};

// ============================================================================
// Configuration and output types
// ============================================================================

/// Configuration for a single leaf update.
pub struct LeafUpdateInput<'a> {
    /// Raw FLI3 leaf bytes.
    pub leaf_bytes: &'a [u8],
    /// Sorted novelty records for this leaf's key range (by `order`).
    pub novelty: &'a [RunRecordV2],
    /// Parallel ops array (same length as `novelty`): 1=assert, 0=retract.
    pub novelty_ops: &'a [u8],
    /// Sort order.
    pub order: RunSortOrder,
    /// Graph id (for routing context; not encoded in leaf V3).
    pub g_id: u16,
    /// Zstd compression level for re-encoded leaflets.
    pub zstd_level: i32,
    /// Target rows per leaflet (for splitting oversized merged results).
    pub leaflet_target_rows: usize,
    /// Target rows per leaf (for splitting into multiple leaf blobs).
    pub leaf_target_rows: usize,
    /// Existing history sidecar bytes (None if no sidecar exists).
    /// Used to carry forward existing history for touched leaflets.
    pub sidecar_bytes: Option<&'a [u8]>,
}

/// A new leaf blob produced by the update.
#[derive(Debug)]
pub struct NewLeafBlob {
    pub info: LeafInfo,
}

/// Output of a leaf update: one or more new leaf blobs.
pub struct LeafUpdateOutput {
    /// New leaf blobs (1 in common case; 2+ if splits occurred).
    pub leaves: Vec<NewLeafBlob>,
}

// ============================================================================
// Processed leaflet (internal)
// ============================================================================

/// A leaflet after processing: either passthrough or re-encoded.
struct ProcessedLeafletV3 {
    /// Encoded leaflet (column blocks + metadata).
    encoded: EncodedLeafletInfo,
    /// History entries for this leaflet (merged: new + existing).
    history: Vec<HistEntryV2>,
}

/// Encoded leaflet representation — either raw passthrough bytes or newly encoded.
enum EncodedLeafletInfo {
    /// Passthrough: raw payload bytes + original directory entry metadata.
    Passthrough {
        dir_entry: LeafletDirEntryV3,
        payload: Vec<u8>,
    },
    /// Re-encoded leaflet(s) from merge result.
    Encoded(fluree_db_binary_index::format::leaflet::EncodedLeaflet),
}

// ============================================================================
// Main entry point
// ============================================================================

/// Update a V3 leaf file with sorted novelty records.
///
/// Returns one or more new leaf blobs (split when row count exceeds
/// `leaf_target_rows`).
pub fn update_leaf(input: &LeafUpdateInput<'_>) -> io::Result<LeafUpdateOutput> {
    debug_assert_eq!(input.novelty.len(), input.novelty_ops.len());

    if input.novelty.is_empty() {
        // No novelty — return the leaf unchanged.
        // The caller should detect this and skip the update entirely,
        // but we handle it gracefully.
        return passthrough_entire_leaf(input);
    }

    let header = decode_leaf_header_v3(input.leaf_bytes)?;
    let dir = decode_leaf_dir_v3_with_base(input.leaf_bytes, &header)?;

    // Slice novelty to leaflets.
    let novelty_slices =
        slice_novelty_to_leaflets(input.novelty, input.novelty_ops, &dir, input.order);

    // Process each leaflet.
    let mut processed: Vec<ProcessedLeafletV3> = Vec::with_capacity(
        header.leaflet_count as usize + input.novelty.len() / input.leaflet_target_rows,
    );

    for (i, (nov_slice, ops_slice)) in novelty_slices.iter().enumerate() {
        let entry = &dir.entries[i];

        if nov_slice.is_empty() {
            // No novelty overlaps this leaflet — passthrough.
            processed.push(passthrough_leaflet(
                input.leaf_bytes,
                entry,
                dir.payload_base,
                input.sidecar_bytes,
            )?);
        } else {
            // Decode, merge, re-encode.
            let mut merged =
                merge_and_encode_leaflet(input, entry, dir.payload_base, nov_slice, ops_slice)?;
            processed.append(&mut merged);
        }
    }

    // Assemble processed leaflets into leaf blob(s).
    assemble_output_leaves(processed, input)
}

// ============================================================================
// Novelty slicing
// ============================================================================

/// Slice novelty records and ops to leaflets using half-open boundary intervals.
///
/// Returns a parallel vec of `(&[RunRecordV2], &[u8])` per leaflet.
///
/// Boundary model:
/// - Leaflet 0: (-∞, boundary[1])
/// - Leaflet i: [boundary[i], boundary[i+1])
/// - Last:      [boundary[last], +∞)
fn slice_novelty_to_leaflets<'a>(
    novelty: &'a [RunRecordV2],
    ops: &'a [u8],
    dir: &DecodedLeafDirV3,
    order: RunSortOrder,
) -> Vec<(&'a [RunRecordV2], &'a [u8])> {
    let n_leaflets = dir.entries.len();
    if n_leaflets == 0 {
        return vec![];
    }
    if n_leaflets == 1 {
        // Single leaflet gets all novelty.
        return vec![(novelty, ops)];
    }

    // Build ordered keys from leaflet first_keys for comparison.
    let cmp_rec =
        |rec: &RunRecordV2, boundary_key: &[u8; ORDERED_KEY_V2_SIZE]| -> std::cmp::Ordering {
            let mut rec_key = [0u8; ORDERED_KEY_V2_SIZE];
            write_ordered_key_v2(order, rec, &mut rec_key);
            rec_key.cmp(boundary_key)
        };

    let mut result = Vec::with_capacity(n_leaflets);
    let mut remaining_records = novelty;
    let mut remaining_ops = ops;

    for i in 0..n_leaflets {
        if i + 1 < n_leaflets {
            // Find the split point: first record >= next leaflet's first_key.
            let next_boundary = &dir.entries[i + 1].first_key;
            let split_pos = remaining_records
                .partition_point(|rec| cmp_rec(rec, next_boundary) == std::cmp::Ordering::Less);

            let (this_recs, rest_recs) = remaining_records.split_at(split_pos);
            let (this_ops, rest_ops) = remaining_ops.split_at(split_pos);
            result.push((this_recs, this_ops));
            remaining_records = rest_recs;
            remaining_ops = rest_ops;
        } else {
            // Last leaflet gets everything remaining.
            result.push((remaining_records, remaining_ops));
        }
    }

    result
}

// ============================================================================
// Passthrough
// ============================================================================

/// Passthrough entire leaf when no novelty exists.
fn passthrough_entire_leaf(input: &LeafUpdateInput<'_>) -> io::Result<LeafUpdateOutput> {
    // Re-wrap existing bytes as a single leaf blob.
    let leaf_bytes = input.leaf_bytes.to_vec();
    let leaf_cid = compute_cid_leaf(&leaf_bytes);

    // Decode header for routing keys.
    let header = decode_leaf_header_v3(input.leaf_bytes)?;

    // Routing keys: placeholders (branch code reads leaf header directly).
    let first_key = zeroed_record();
    let last_key = zeroed_record();

    // Pass through existing sidecar unchanged.
    let (sidecar_cid, sidecar_bytes) = match input.sidecar_bytes {
        Some(bytes) => {
            let cid = compute_cid_sidecar(bytes);
            (Some(cid), Some(bytes.to_vec()))
        }
        None => (None, None),
    };

    Ok(LeafUpdateOutput {
        leaves: vec![NewLeafBlob {
            info: LeafInfo {
                leaf_cid,
                leaf_bytes,
                sidecar_cid,
                sidecar_bytes,
                total_rows: header.total_rows,
                first_key,
                last_key,
            },
        }],
    })
}

/// Passthrough a single untouched leaflet.
fn passthrough_leaflet(
    leaf_bytes: &[u8],
    entry: &LeafletDirEntryV3,
    payload_base: usize,
    sidecar_bytes: Option<&[u8]>,
) -> io::Result<ProcessedLeafletV3> {
    // Extract raw payload bytes.
    let start = payload_base + entry.payload_offset as usize;
    let end = start + entry.payload_len as usize;
    let payload = leaf_bytes[start..end].to_vec();

    // Carry forward existing history entries from sidecar.
    let history = load_existing_history(entry, sidecar_bytes)?;

    Ok(ProcessedLeafletV3 {
        encoded: EncodedLeafletInfo::Passthrough {
            dir_entry: clone_dir_entry(entry),
            payload,
        },
        history,
    })
}

// ============================================================================
// Merge and re-encode
// ============================================================================

/// Decode a leaflet's columns, merge novelty, and re-encode.
///
/// May produce multiple `ProcessedLeafletV3` if the merged result exceeds
/// `leaflet_target_rows` or requires segmentation splits.
fn merge_and_encode_leaflet(
    input: &LeafUpdateInput<'_>,
    entry: &LeafletDirEntryV3,
    payload_base: usize,
    novelty: &[RunRecordV2],
    novelty_ops: &[u8],
) -> io::Result<Vec<ProcessedLeafletV3>> {
    // 1. Load existing leaflet columns.
    let projection = ColumnProjection::all();
    let batch = load_leaflet_columns(
        input.leaf_bytes,
        entry,
        payload_base,
        &projection,
        input.order,
    )?;

    // 2. Load existing history from sidecar.
    let existing_history = load_existing_history(entry, input.sidecar_bytes)?;

    let order = input.order;
    let zstd_level = input.zstd_level;
    let leaflet_target_rows = input.leaflet_target_rows;

    // 3. Merge.
    let merge_input = MergeInput {
        batch: &batch,
        existing_history: &existing_history,
        novelty,
        novelty_ops,
        order,
    };
    let MergeOutput {
        records: merged,
        history,
    } = merge_novelty(&merge_input);

    // 4. Handle empty-after-retract: valid in V3 (history preserved in sidecar).
    //    Preserve the original leaflet's key range and constants so branch
    //    routing remains correct and time-travel replay can find this segment.
    if merged.is_empty() {
        return Ok(vec![ProcessedLeafletV3 {
            encoded: EncodedLeafletInfo::Encoded(empty_encoded_leaflet_with_keys(entry)),
            history,
        }]);
    }

    // 5. Split by segmentation + row count, then encode each chunk.
    let chunks = split_by_segmentation_and_size(&merged, order, leaflet_target_rows);
    let mut result = Vec::with_capacity(chunks.len());

    if chunks.len() == 1 {
        // Common case: no split, all history stays with the single chunk.
        let encoded = encode_leaflet(chunks[0], order, zstd_level)?;
        result.push(ProcessedLeafletV3 {
            encoded: EncodedLeafletInfo::Encoded(encoded),
            history,
        });
    } else {
        // Multiple chunks: partition history entries by chunk key boundaries.
        let partitioned = partition_history_to_chunks(&chunks, &history, order);
        for (chunk, chunk_history) in chunks.iter().zip(partitioned) {
            let encoded = encode_leaflet(chunk, order, zstd_level)?;
            result.push(ProcessedLeafletV3 {
                encoded: EncodedLeafletInfo::Encoded(encoded),
                history: chunk_history,
            });
        }
    }

    Ok(result)
}

/// Split merged records by segmentation constraints and row-count limits.
///
/// For POST/PSOT: split on `p_id` transitions.
/// For OPST: split on `o_type` transitions.
/// For SPOT: split by row count only.
///
/// Within each homogeneous segment, further split if rows > target.
fn split_by_segmentation_and_size(
    records: &[RunRecordV2],
    order: RunSortOrder,
    target_rows: usize,
) -> Vec<&[RunRecordV2]> {
    if records.is_empty() {
        return vec![];
    }

    let mut chunks = Vec::new();
    let mut start = 0;

    while start < records.len() {
        // Find the end of the current homogeneous segment.
        let seg_end = find_segment_end(records, start, order);

        // Split the segment into target_rows-sized chunks.
        let segment = &records[start..seg_end];
        for sub_chunk in segment.chunks(target_rows) {
            chunks.push(sub_chunk);
        }

        start = seg_end;
    }

    chunks
}

/// Find the end of the homogeneous segment starting at `start`.
fn find_segment_end(records: &[RunRecordV2], start: usize, order: RunSortOrder) -> usize {
    match order {
        RunSortOrder::Post | RunSortOrder::Psot => {
            let p_id = records[start].p_id;
            records[start..]
                .iter()
                .position(|r| r.p_id != p_id)
                .map_or(records.len(), |pos| start + pos)
        }
        RunSortOrder::Opst => {
            let o_type = records[start].o_type;
            records[start..]
                .iter()
                .position(|r| r.o_type != o_type)
                .map_or(records.len(), |pos| start + pos)
        }
        RunSortOrder::Spot => {
            // No segmentation — entire slice is one segment.
            records.len()
        }
    }
}

/// Partition history entries across chunks based on key boundaries.
///
/// Each history entry is assigned to the chunk whose key range contains its
/// identity. Uses the first record of each chunk (except the first) as boundary
/// markers, similar to the novelty slicing approach.
fn partition_history_to_chunks(
    chunks: &[&[RunRecordV2]],
    history: &[HistEntryV2],
    order: RunSortOrder,
) -> Vec<Vec<HistEntryV2>> {
    use fluree_db_binary_index::format::run_record_v2::cmp_v2_for_order;

    let n = chunks.len();
    let mut partitioned: Vec<Vec<HistEntryV2>> = (0..n).map(|_| Vec::new()).collect();

    if n == 0 || history.is_empty() {
        return partitioned;
    }

    let cmp = cmp_v2_for_order(order);

    // Build boundary keys: the first record of each chunk (starting from chunk 1).
    // A history entry belongs to chunk i if it's < boundary[i+1] (or is in the last chunk).
    let boundaries: Vec<&RunRecordV2> = chunks[1..].iter().map(|c| &c[0]).collect();

    for entry in history {
        // Convert history entry to a minimal RunRecordV2 for comparison.
        let entry_rec = RunRecordV2 {
            s_id: entry.s_id,
            o_key: entry.o_key,
            p_id: entry.p_id,
            t: 0,
            o_i: entry.o_i,
            o_type: entry.o_type,
            g_id: 0,
        };

        // Find the chunk this entry belongs to via binary search on boundaries.
        let chunk_idx = boundaries
            .partition_point(|boundary| cmp(&entry_rec, boundary) != std::cmp::Ordering::Less);

        partitioned[chunk_idx].push(*entry);
    }

    partitioned
}

// ============================================================================
// Assembly into leaf blobs
// ============================================================================

/// Assemble processed leaflets into one or more leaf blobs.
fn assemble_output_leaves(
    processed: Vec<ProcessedLeafletV3>,
    input: &LeafUpdateInput<'_>,
) -> io::Result<LeafUpdateOutput> {
    use fluree_db_binary_index::format::leaflet::EncodedLeaflet;

    if processed.is_empty() {
        return Ok(LeafUpdateOutput { leaves: vec![] });
    }

    // Convert ProcessedLeafletV3 into (EncodedLeaflet, Vec<HistEntryV2>) pairs
    // for assembly.
    let mut leaflet_data: Vec<(EncodedLeaflet, Vec<HistEntryV2>)> =
        Vec::with_capacity(processed.len());

    for p in processed {
        let encoded = match p.encoded {
            EncodedLeafletInfo::Passthrough { dir_entry, payload } => {
                reconstruct_encoded_leaflet(dir_entry, payload)
            }
            EncodedLeafletInfo::Encoded(e) => e,
        };
        leaflet_data.push((encoded, p.history));
    }

    // Group leaflets into leaves by row count.
    let mut leaves = Vec::new();
    let mut current_group: Vec<(EncodedLeaflet, Vec<HistEntryV2>)> = Vec::new();
    let mut current_rows: u64 = 0;

    for item in leaflet_data {
        let rows = item.0.row_count as u64;
        current_rows += rows;
        current_group.push(item);

        if current_rows >= input.leaf_target_rows as u64 {
            leaves.push(std::mem::take(&mut current_group));
            current_rows = 0;
        }
    }
    if !current_group.is_empty() {
        leaves.push(current_group);
    }

    // Build each leaf blob.
    let mut output = Vec::with_capacity(leaves.len());
    for group in leaves {
        let leaf_info = build_leaf_from_group(group, input.order)?;
        output.push(NewLeafBlob { info: leaf_info });
    }

    Ok(LeafUpdateOutput { leaves: output })
}

/// Build a single leaf blob from a group of (EncodedLeaflet, history) pairs.
fn build_leaf_from_group(
    group: Vec<(
        fluree_db_binary_index::format::leaflet::EncodedLeaflet,
        Vec<HistEntryV2>,
    )>,
    order: RunSortOrder,
) -> io::Result<LeafInfo> {
    use fluree_db_binary_index::format::leaflet::EncodedLeaflet;

    // Build sidecar from history entries.
    let mut sidecar_builder = HistSidecarBuilder::new();
    for (_, history) in &group {
        sidecar_builder.start_leaflet();
        for entry in history {
            sidecar_builder.push_entry(*entry);
        }
    }

    let (sidecar_cid, sidecar_bytes, seg_refs) = if sidecar_builder.has_history() {
        let (bytes, refs) = sidecar_builder.build();
        let cid = compute_cid_sidecar(&bytes);
        (Some(cid), Some(bytes), refs)
    } else {
        (None, None, Vec::new())
    };

    // Extract first/last routing keys (raw ordered key bytes).
    let first_key_bytes = group
        .first()
        .map(|(e, _)| e.first_key)
        .unwrap_or([0u8; ORDERED_KEY_V2_SIZE]);
    let last_key_bytes = group
        .last()
        .map(|(e, _)| e.last_key)
        .unwrap_or([0u8; ORDERED_KEY_V2_SIZE]);

    let owned_leaflets: Vec<EncodedLeaflet> = group.into_iter().map(|(e, _)| e).collect();

    let leaf_bytes = build_leaf_blob_raw_keys(
        order,
        &owned_leaflets,
        &seg_refs,
        &first_key_bytes,
        &last_key_bytes,
    );
    let leaf_cid = compute_cid_leaf(&leaf_bytes);
    let total_rows: u64 = owned_leaflets.iter().map(|l| l.row_count as u64).sum();

    // For LeafInfo, first_key/last_key are RunRecordV2 — but for incremental
    // we only need them for branch manifest routing. The branch code reads the
    // leaf header's raw key bytes directly, so these are placeholders.
    let first_key = zeroed_record();
    let last_key = zeroed_record();

    Ok(LeafInfo {
        leaf_cid,
        leaf_bytes,
        sidecar_cid,
        sidecar_bytes,
        total_rows,
        first_key,
        last_key,
    })
}

// ============================================================================
// Helpers
// ============================================================================

/// Load existing history entries for a leaflet from the sidecar.
///
/// Returns an error if the leaflet claims to have history (`history_len > 0`)
/// but no sidecar bytes are available — this indicates a missing sidecar that
/// would silently corrupt time-travel.
fn load_existing_history(
    entry: &LeafletDirEntryV3,
    sidecar_bytes: Option<&[u8]>,
) -> io::Result<Vec<HistEntryV2>> {
    if entry.history_len == 0 {
        return Ok(Vec::new());
    }
    let sb = sidecar_bytes.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            format!(
                "leaflet has history_len={} but no sidecar bytes available; \
                 cannot preserve time-travel history",
                entry.history_len,
            ),
        )
    })?;
    let seg_ref = HistorySegmentRef {
        offset: entry.history_offset,
        len: entry.history_len,
        min_t: entry.history_min_t,
        max_t: entry.history_max_t,
    };
    decode_history_segment(sb, &seg_ref)
}

/// Placeholder RunRecordV2 with all fields zeroed.
fn zeroed_record() -> RunRecordV2 {
    RunRecordV2 {
        s_id: fluree_db_core::subject_id::SubjectId(0),
        o_key: 0,
        p_id: 0,
        t: 0,
        o_i: u32::MAX,
        o_type: 0,
        g_id: 0,
    }
}

/// Reconstruct an `EncodedLeaflet` from a passthrough directory entry + payload.
fn reconstruct_encoded_leaflet(
    entry: LeafletDirEntryV3,
    payload: Vec<u8>,
) -> fluree_db_binary_index::format::leaflet::EncodedLeaflet {
    fluree_db_binary_index::format::leaflet::EncodedLeaflet {
        row_count: entry.row_count,
        lead_group_count: entry.lead_group_count,
        first_key: entry.first_key,
        last_key: entry.last_key,
        p_const: entry.p_const,
        o_type_const: entry.o_type_const,
        flags: entry.flags,
        column_refs: entry.column_refs,
        payload,
    }
}

/// Clone a `LeafletDirEntryV3`.
fn clone_dir_entry(entry: &LeafletDirEntryV3) -> LeafletDirEntryV3 {
    LeafletDirEntryV3 {
        row_count: entry.row_count,
        lead_group_count: entry.lead_group_count,
        first_key: entry.first_key,
        last_key: entry.last_key,
        p_const: entry.p_const,
        o_type_const: entry.o_type_const,
        flags: entry.flags,
        payload_offset: entry.payload_offset,
        payload_len: entry.payload_len,
        column_refs: entry.column_refs.clone(),
        history_offset: entry.history_offset,
        history_len: entry.history_len,
        history_min_t: entry.history_min_t,
        history_max_t: entry.history_max_t,
    }
}

/// Create an empty encoded leaflet (zero rows, no columns) that preserves
/// the original leaflet's key range and constants for correct branch routing.
fn empty_encoded_leaflet_with_keys(
    entry: &LeafletDirEntryV3,
) -> fluree_db_binary_index::format::leaflet::EncodedLeaflet {
    fluree_db_binary_index::format::leaflet::EncodedLeaflet {
        row_count: 0,
        lead_group_count: 0,
        first_key: entry.first_key,
        last_key: entry.last_key,
        p_const: entry.p_const,
        o_type_const: entry.o_type_const,
        flags: 0,
        column_refs: Vec::new(),
        payload: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_binary_index::format::leaf::LeafWriter;
    use fluree_db_binary_index::format::run_record_v2::RunRecordV2;
    use fluree_db_core::o_type::OType;
    use fluree_db_core::subject_id::SubjectId;
    use fluree_db_core::value_id::ObjKey;

    const OI_NONE: u32 = u32::MAX;

    fn rec2(s_id: u64, p_id: u32, val: i64, t: u32) -> RunRecordV2 {
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

    /// Build a V3 leaf from records using LeafWriter.
    fn build_test_leaf(records: &[RunRecordV2], order: RunSortOrder) -> (Vec<u8>, Option<Vec<u8>>) {
        let mut writer = LeafWriter::new(order, 100, 10000, 1);
        writer.set_skip_history(true);
        for rec in records {
            writer.push_record(*rec).unwrap();
        }
        let leaves = writer.finish().unwrap();
        assert_eq!(leaves.len(), 1, "test helper expects single leaf");
        let leaf = &leaves[0];
        (leaf.leaf_bytes.clone(), leaf.sidecar_bytes.clone())
    }

    #[test]
    fn test_update_no_novelty() {
        let records = vec![rec2(1, 1, 10, 1), rec2(2, 1, 20, 1)];
        let (leaf_bytes, sidecar) = build_test_leaf(&records, RunSortOrder::Spot);

        let input = LeafUpdateInput {
            leaf_bytes: &leaf_bytes,
            novelty: &[],
            novelty_ops: &[],
            order: RunSortOrder::Spot,
            g_id: 0,
            zstd_level: 1,
            leaflet_target_rows: 100,
            leaf_target_rows: 10000,
            sidecar_bytes: sidecar.as_deref(),
        };

        let output = update_leaf(&input).unwrap();
        assert_eq!(output.leaves.len(), 1);
        assert_eq!(output.leaves[0].info.total_rows, 2);
    }

    #[test]
    fn test_update_insert_new_fact() {
        let records = vec![rec2(1, 1, 10, 1), rec2(3, 1, 30, 1)];
        let (leaf_bytes, sidecar) = build_test_leaf(&records, RunSortOrder::Spot);

        let novelty = vec![rec2(2, 1, 20, 5)]; // insert between
        let ops = vec![1u8];

        let input = LeafUpdateInput {
            leaf_bytes: &leaf_bytes,
            novelty: &novelty,
            novelty_ops: &ops,
            order: RunSortOrder::Spot,
            g_id: 0,
            zstd_level: 1,
            leaflet_target_rows: 100,
            leaf_target_rows: 10000,
            sidecar_bytes: sidecar.as_deref(),
        };

        let output = update_leaf(&input).unwrap();
        assert_eq!(output.leaves.len(), 1);
        assert_eq!(output.leaves[0].info.total_rows, 3);

        // Verify sidecar was produced (history for the insert).
        assert!(output.leaves[0].info.sidecar_bytes.is_some());
    }

    #[test]
    fn test_update_retract_fact() {
        let records = vec![rec2(1, 1, 10, 1), rec2(2, 1, 20, 1), rec2(3, 1, 30, 1)];
        let (leaf_bytes, sidecar) = build_test_leaf(&records, RunSortOrder::Spot);

        let novelty = vec![rec2(2, 1, 20, 5)]; // retract s=2
        let ops = vec![0u8];

        let input = LeafUpdateInput {
            leaf_bytes: &leaf_bytes,
            novelty: &novelty,
            novelty_ops: &ops,
            order: RunSortOrder::Spot,
            g_id: 0,
            zstd_level: 1,
            leaflet_target_rows: 100,
            leaf_target_rows: 10000,
            sidecar_bytes: sidecar.as_deref(),
        };

        let output = update_leaf(&input).unwrap();
        assert_eq!(output.leaves.len(), 1);
        assert_eq!(output.leaves[0].info.total_rows, 2); // s=2 removed
    }

    #[test]
    fn test_update_retract_all_preserves_history() {
        let records = vec![rec2(1, 1, 10, 1)];
        let (leaf_bytes, sidecar) = build_test_leaf(&records, RunSortOrder::Spot);

        let novelty = vec![rec2(1, 1, 10, 5)]; // retract
        let ops = vec![0u8];

        let input = LeafUpdateInput {
            leaf_bytes: &leaf_bytes,
            novelty: &novelty,
            novelty_ops: &ops,
            order: RunSortOrder::Spot,
            g_id: 0,
            zstd_level: 1,
            leaflet_target_rows: 100,
            leaf_target_rows: 10000,
            sidecar_bytes: sidecar.as_deref(),
        };

        let output = update_leaf(&input).unwrap();
        assert_eq!(output.leaves.len(), 1);
        // Empty latest-state is valid; total_rows=0
        assert_eq!(output.leaves[0].info.total_rows, 0);
        // But sidecar should have history
        assert!(output.leaves[0].info.sidecar_bytes.is_some());
    }

    /// Regression: empty-after-retract leaflet must preserve the original key range
    /// so branch routing stays correct.
    #[test]
    fn test_retract_all_preserves_key_range() {
        let records = vec![rec2(100, 1, 10, 1), rec2(200, 1, 20, 1)];
        let (leaf_bytes, sidecar) = build_test_leaf(&records, RunSortOrder::Spot);

        // Read original leaf header to capture key range.
        let orig_header = decode_leaf_header_v3(&leaf_bytes).unwrap();

        // Retract all facts.
        let novelty = vec![rec2(100, 1, 10, 5), rec2(200, 1, 20, 5)];
        let ops = vec![0u8, 0];

        let input = LeafUpdateInput {
            leaf_bytes: &leaf_bytes,
            novelty: &novelty,
            novelty_ops: &ops,
            order: RunSortOrder::Spot,
            g_id: 0,
            zstd_level: 1,
            leaflet_target_rows: 100,
            leaf_target_rows: 10000,
            sidecar_bytes: sidecar.as_deref(),
        };

        let output = update_leaf(&input).unwrap();
        assert_eq!(output.leaves.len(), 1);
        assert_eq!(output.leaves[0].info.total_rows, 0);

        // Verify the new leaf's directory preserves the original key range (not zeroed).
        let new_header = decode_leaf_header_v3(&output.leaves[0].info.leaf_bytes).unwrap();
        // The leaflet directory first_key should be non-zero (preserved from original).
        let dir =
            decode_leaf_dir_v3_with_base(&output.leaves[0].info.leaf_bytes, &new_header).unwrap();
        assert!(!dir.entries.is_empty());
        // Keys should be the same as the original leaflet's keys.
        assert_eq!(
            dir.entries[0].first_key, orig_header.first_key,
            "empty leaflet must preserve original first_key for routing"
        );
    }

    /// Regression: when a leaflet splits into multiple chunks, history entries
    /// must be partitioned to the correct chunk (not all in chunk 0).
    #[test]
    fn test_leaflet_split_partitions_history() -> Result<(), Box<dyn std::error::Error>> {
        // Create a leaf with a few existing records.
        let existing = vec![rec2(1, 1, 10, 1), rec2(2, 1, 20, 1), rec2(3, 1, 30, 1)];
        let (leaf_bytes, sidecar) = build_test_leaf(&existing, RunSortOrder::Spot);

        // Add enough novelty to force a split (target_rows=3).
        let novelty = vec![
            rec2(4, 1, 40, 5),
            rec2(5, 1, 50, 5),
            rec2(6, 1, 60, 5),
            rec2(7, 1, 70, 5),
        ];
        let ops = vec![1u8, 1, 1, 1];

        let input = LeafUpdateInput {
            leaf_bytes: &leaf_bytes,
            novelty: &novelty,
            novelty_ops: &ops,
            order: RunSortOrder::Spot,
            g_id: 0,
            zstd_level: 1,
            leaflet_target_rows: 3, // Force split: 7 rows / 3 = 3 leaflets
            leaf_target_rows: 10000,
            sidecar_bytes: sidecar.as_deref(),
        };

        let output = update_leaf(&input).unwrap();
        assert_eq!(output.leaves.len(), 1);

        // The leaf should contain multiple leaflets due to the split.
        let header = decode_leaf_header_v3(&output.leaves[0].info.leaf_bytes).unwrap();
        assert!(
            header.leaflet_count >= 2,
            "expected split into 2+ leaflets, got {}",
            header.leaflet_count
        );

        // Verify sidecar exists (novelty creates history entries).
        assert!(output.leaves[0].info.sidecar_bytes.is_some());

        // Decode sidecar and verify history entries are distributed
        // (not all concentrated in segment 0).
        let sc_bytes = output.leaves[0].info.sidecar_bytes.as_ref().unwrap();
        let dir = decode_leaf_dir_v3_with_base(&output.leaves[0].info.leaf_bytes, &header).unwrap();

        let mut total_history = 0;
        for entry in &dir.entries {
            if entry.history_len > 0 {
                let seg = HistorySegmentRef {
                    offset: entry.history_offset,
                    len: entry.history_len,
                    min_t: entry.history_min_t,
                    max_t: entry.history_max_t,
                };
                let entries = decode_history_segment(sc_bytes, &seg)?;
                total_history += entries.len();
            }
        }

        // All 4 novelty records should produce history entries.
        assert_eq!(
            total_history, 4,
            "expected 4 history entries total, got {total_history}"
        );
        Ok(())
    }
}
