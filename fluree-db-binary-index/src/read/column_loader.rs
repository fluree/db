//! Column block loader: decodes individual columns from FLI3 leaf bytes.
//!
//! Given a `LeafletDirEntryV3` and the leaf blob, loads only the columns
//! requested by a `ColumnProjection`. Handles constant columns (`p_const`,
//! `o_type_const`) and absent optional columns (`o_i` when `HAS_O_I` is unset).

use std::io;
use std::sync::Arc;

use super::column_types::{ColumnBatch, ColumnData, ColumnProjection};
use crate::format::column_block::{
    decode_column_u16, decode_column_u32, decode_column_u64, ColumnBlockRef, ColumnId,
};
use crate::format::leaf::LeafletDirEntryV3;
use crate::format::leaflet::flags::{
    HAS_O_I as FLAG_HAS_O_I, HAS_O_TYPE_COL as FLAG_HAS_O_TYPE_COL,
};
use crate::format::run_record::RunSortOrder;

/// Load columns from a single leaflet within a V3 leaf blob.
///
/// # Arguments
///
/// - `leaf_bytes`: the complete FLI3 leaf blob
/// - `entry`: the leaflet's directory entry (from `decode_leaf_dir_v3`)
/// - `payload_base`: byte offset in `leaf_bytes` where the payload section starts
///   (i.e. `LEAF_V3_HEADER_SIZE + total_directory_size`). Obtained from
///   `decode_leaf_dir_v3_with_payload_base()`.
/// - `projection`: which columns to load
/// - `_order`: sort order (reserved for future order-specific optimizations)
pub fn load_leaflet_columns(
    leaf_bytes: &[u8],
    entry: &LeafletDirEntryV3,
    payload_base: usize,
    projection: &ColumnProjection,
    _order: RunSortOrder,
) -> io::Result<ColumnBatch> {
    let row_count = entry.row_count as usize;
    let eff = projection.effective();

    // The leaflet's column block data starts at this absolute offset in the leaf blob.
    let leaflet_data_start = payload_base + entry.payload_offset as usize;

    // Helper: find a column block ref by column ID.
    let find_ref = |col_id: ColumnId| -> Option<&ColumnBlockRef> {
        entry.column_refs.iter().find(|r| r.col_id == col_id as u16)
    };

    // Adjust a block ref's offset to be absolute within leaf_bytes, then decode.
    // Validates that the decoded length matches row_count.
    let decode_u64 = |col_id: ColumnId| -> io::Result<Vec<u64>> {
        let block_ref = find_ref(col_id).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("missing column block for {col_id:?}"),
            )
        })?;
        let mut adjusted = *block_ref;
        adjusted.offset += leaflet_data_start as u32;
        let decoded = decode_column_u64(leaf_bytes, &adjusted)?;
        if decoded.len() != row_count {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "{:?} column: decoded {} values but leaflet has {} rows",
                    col_id,
                    decoded.len(),
                    row_count
                ),
            ));
        }
        Ok(decoded)
    };

    let decode_u32 = |col_id: ColumnId| -> io::Result<Vec<u32>> {
        let block_ref = find_ref(col_id).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("missing column block for {col_id:?}"),
            )
        })?;
        let mut adjusted = *block_ref;
        adjusted.offset += leaflet_data_start as u32;
        let decoded = decode_column_u32(leaf_bytes, &adjusted)?;
        if decoded.len() != row_count {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "{:?} column: decoded {} values but leaflet has {} rows",
                    col_id,
                    decoded.len(),
                    row_count
                ),
            ));
        }
        Ok(decoded)
    };

    let decode_u16_col = |col_id: ColumnId| -> io::Result<Vec<u16>> {
        let block_ref = find_ref(col_id).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("missing column block for {col_id:?}"),
            )
        })?;
        let mut adjusted = *block_ref;
        adjusted.offset += leaflet_data_start as u32;
        let decoded = decode_column_u16(leaf_bytes, &adjusted)?;
        if decoded.len() != row_count {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "{:?} column: decoded {} values but leaflet has {} rows",
                    col_id,
                    decoded.len(),
                    row_count
                ),
            ));
        }
        Ok(decoded)
    };

    // ── s_id ──
    let s_id = if eff.contains(ColumnId::SId) {
        ColumnData::Block(Arc::from(decode_u64(ColumnId::SId)?))
    } else {
        ColumnData::AbsentDefault
    };

    // ── o_key ──
    let o_key = if eff.contains(ColumnId::OKey) {
        ColumnData::Block(Arc::from(decode_u64(ColumnId::OKey)?))
    } else {
        ColumnData::AbsentDefault
    };

    // ── p_id ── (may be constant via p_const)
    let p_id = if eff.contains(ColumnId::PId) {
        if let Some(p) = entry.p_const {
            ColumnData::Const(p)
        } else {
            ColumnData::Block(Arc::from(decode_u32(ColumnId::PId)?))
        }
    } else {
        ColumnData::AbsentDefault
    };

    // ── o_type ── (may be constant via o_type_const)
    let o_type = if eff.contains(ColumnId::OType) {
        if let Some(ot) = entry.o_type_const {
            ColumnData::Const(ot)
        } else if entry.flags & FLAG_HAS_O_TYPE_COL != 0 {
            ColumnData::Block(Arc::from(decode_u16_col(ColumnId::OType)?))
        } else {
            // No o_type column and no o_type_const — shouldn't happen in a valid leaf.
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "leaflet has neither o_type_const nor o_type column",
            ));
        }
    } else {
        ColumnData::AbsentDefault
    };

    // ── o_i ── (optional: only present when FLAG_HAS_O_I is set)
    let o_i = if eff.contains(ColumnId::OI) {
        if entry.flags & FLAG_HAS_O_I != 0 {
            ColumnData::Block(Arc::from(decode_u32(ColumnId::OI)?))
        } else {
            // All values are the sentinel — no column block exists.
            ColumnData::Const(u32::MAX)
        }
    } else {
        ColumnData::AbsentDefault
    };

    // ── t ──
    let t = if eff.contains(ColumnId::T) {
        ColumnData::Block(Arc::from(decode_u32(ColumnId::T)?))
    } else {
        ColumnData::AbsentDefault
    };

    Ok(ColumnBatch {
        row_count,
        s_id,
        o_key,
        p_id,
        o_type,
        o_i,
        t,
    })
}

/// Load columns from a V3 leaflet with `LeafletCache` support.
///
/// On cache hit, returns the previously decoded `ColumnBatch` directly (zero
/// decompress cost). On miss, calls `load_leaflet_columns`, inserts the result
/// into the cache, and returns it.
///
/// The cache key uses `(leaf_id, leaflet_idx)` — base columns are immutable
/// (content-addressed leaf CID), so no `to_t`/`epoch` dimension is needed.
/// Overlay merge and time-travel replay are applied downstream.
///
/// **Important**: this always decodes ALL columns (projection=all) so the cached
/// batch can serve any subsequent projection. The per-column `ColumnData::Block`
/// values are `Arc<[T]>` and cheap to clone.
pub fn load_leaflet_columns_cached(
    leaf_bytes: &[u8],
    entry: &LeafletDirEntryV3,
    payload_base: usize,
    order: RunSortOrder,
    cache: &super::leaflet_cache::LeafletCache,
    leaf_id: u128,
    leaflet_idx: u32,
) -> io::Result<ColumnBatch> {
    let key = super::leaflet_cache::V3BatchCacheKey {
        leaf_id,
        leaflet_idx,
    };

    cache.try_get_or_decode_v3_batch(key, || {
        // Cache miss: decode ALL columns so the cached batch serves any projection.
        let all = ColumnProjection::all();
        load_leaflet_columns(leaf_bytes, entry, payload_base, &all, order)
    })
}

/// Load columns from a V3 leaflet with `LeafletCache` support, via a [`LeafHandle`].
///
/// Same caching semantics as [`load_leaflet_columns_cached`], but delegates to
/// [`LeafHandle::load_columns()`] on cache miss. This works transparently for
/// both local (`FullBlobLeafHandle`) and remote (`RangeReadLeafHandle`) access.
///
/// Always decodes ALL columns on miss so the cached batch serves any projection.
pub fn load_columns_cached_via_handle(
    handle: &dyn super::leaf_access::LeafHandle,
    leaflet_idx: usize,
    order: RunSortOrder,
    cache: &super::leaflet_cache::LeafletCache,
    leaf_id: u128,
    leaflet_idx_u32: u32,
) -> io::Result<ColumnBatch> {
    let key = super::leaflet_cache::V3BatchCacheKey {
        leaf_id,
        leaflet_idx: leaflet_idx_u32,
    };
    let batch = cache.try_get_or_decode_v3_batch(key, || {
        let all = ColumnProjection::all();
        handle.load_columns(leaflet_idx, &all, order)
    })?;
    Ok(batch)
}

// Re-export for convenience: callers use decode_leaf_dir_v3_with_base to get
// the authoritative payload_base alongside the directory entries.
pub use crate::format::leaf::{decode_leaf_dir_v3_with_base, DecodedLeafDirV3};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::leaf::{decode_leaf_header_v3, LeafWriter};
    use crate::format::run_record::RunSortOrder;
    use crate::format::run_record_v2::RunRecordV2;
    use crate::read::column_types::{ColumnProjection, ColumnSet};
    use crate::read::leaf_access::FullBlobLeafHandle;
    use crate::read::leaflet_cache::LeafletCache;
    use fluree_db_core::o_type::OType;
    use fluree_db_core::subject_id::SubjectId;

    fn make_rec(s_id: u64, p_id: u32, o_type: u16, o_key: u64, t: u32) -> RunRecordV2 {
        RunRecordV2 {
            s_id: SubjectId(s_id),
            o_key,
            p_id,
            t,
            o_i: u32::MAX,
            o_type,
            g_id: 0,
        }
    }

    /// Round-trip: LeafWriter → leaf bytes → decode dir → load columns → verify.
    #[test]
    fn round_trip_post_leaf_columns() {
        // Build a POST leaf with a single predicate (p_id=1), single type (XSD_INTEGER).
        let mut writer = LeafWriter::new(RunSortOrder::Post, 100, 1000, 1);
        writer.set_skip_history(true);

        let ot = OType::XSD_INTEGER.as_u16();
        for i in 0..5u64 {
            writer
                .push_record(make_rec(i + 1, 1, ot, i * 10, 1))
                .unwrap();
        }
        let infos = writer.finish().unwrap();
        assert_eq!(infos.len(), 1);
        let leaf_bytes = &infos[0].leaf_bytes;

        // Decode header + directory.
        let header = decode_leaf_header_v3(leaf_bytes).unwrap();
        assert_eq!(header.leaflet_count, 1);

        let decoded_dir = decode_leaf_dir_v3_with_base(leaf_bytes, &header).unwrap();
        assert_eq!(decoded_dir.entries.len(), 1);
        let entry = &decoded_dir.entries[0];

        // Verify directory metadata.
        assert_eq!(entry.row_count, 5);
        assert_eq!(entry.p_const, Some(1)); // POST → predicate-homogeneous
        assert_eq!(entry.o_type_const, Some(ot)); // single type → o_type_const set

        // Load all columns.
        let proj = ColumnProjection::all();
        let batch = load_leaflet_columns(
            leaf_bytes,
            entry,
            decoded_dir.payload_base,
            &proj,
            RunSortOrder::Post,
        )
        .unwrap();

        assert_eq!(batch.row_count, 5);

        // p_id should be Const(1) because p_const is set.
        assert!(batch.p_id.is_const());
        assert_eq!(batch.p_id.get(0), 1);
        assert_eq!(batch.p_id.get(4), 1);

        // o_type should be Const(XSD_INTEGER) because o_type_const is set.
        assert!(batch.o_type.is_const());
        assert_eq!(batch.o_type.get(0), ot);

        // o_i should be Const(u32::MAX) — HAS_O_I flag is not set (all sentinel).
        assert!(batch.o_i.is_const());
        assert_eq!(batch.o_i.get(0), u32::MAX);

        // s_id should be Block with values 1..=5.
        assert!(matches!(batch.s_id, ColumnData::Block(_)));
        for i in 0..5 {
            assert_eq!(batch.s_id.get(i), (i + 1) as u64);
        }

        // o_key should be Block with values 0, 10, 20, 30, 40.
        assert!(matches!(batch.o_key, ColumnData::Block(_)));
        for i in 0..5 {
            assert_eq!(batch.o_key.get(i), i as u64 * 10);
        }

        // t should be Block with all 1s.
        assert!(matches!(batch.t, ColumnData::Block(_)));
        for i in 0..5 {
            assert_eq!(batch.t.get(i), 1);
        }
    }

    /// SPOT leaf: no p_const, o_type should be a real column when types are mixed.
    #[test]
    fn round_trip_spot_leaf_mixed_types() {
        let mut writer = LeafWriter::new(RunSortOrder::Spot, 100, 1000, 1);
        writer.set_skip_history(true);

        // Two records with different o_type → no o_type_const.
        writer
            .push_record(make_rec(1, 1, OType::XSD_INTEGER.as_u16(), 10, 1))
            .unwrap();
        writer
            .push_record(make_rec(1, 2, OType::XSD_STRING.as_u16(), 20, 1))
            .unwrap();
        let infos = writer.finish().unwrap();
        let leaf_bytes = &infos[0].leaf_bytes;

        let header = decode_leaf_header_v3(leaf_bytes).unwrap();
        let decoded_dir = decode_leaf_dir_v3_with_base(leaf_bytes, &header).unwrap();
        let entry = &decoded_dir.entries[0];

        // SPOT has no p_const.
        assert!(entry.p_const.is_none());
        // Mixed types → no o_type_const.
        assert!(entry.o_type_const.is_none());

        let proj = ColumnProjection::all();
        let batch = load_leaflet_columns(
            leaf_bytes,
            entry,
            decoded_dir.payload_base,
            &proj,
            RunSortOrder::Spot,
        )
        .unwrap();

        assert_eq!(batch.row_count, 2);

        // p_id should be a Block (not const).
        assert!(matches!(batch.p_id, ColumnData::Block(_)));
        assert_eq!(batch.p_id.get(0), 1);
        assert_eq!(batch.p_id.get(1), 2);

        // o_type should be a Block (not const).
        assert!(matches!(batch.o_type, ColumnData::Block(_)));
        assert_eq!(batch.o_type.get(0), OType::XSD_INTEGER.as_u16());
        assert_eq!(batch.o_type.get(1), OType::XSD_STRING.as_u16());
    }

    /// Selective projection: only load s_id and o_key, skip everything else.
    #[test]
    fn selective_projection() {
        let mut writer = LeafWriter::new(RunSortOrder::Post, 100, 1000, 1);
        writer.set_skip_history(true);

        let ot = OType::XSD_INTEGER.as_u16();
        for i in 0..3u64 {
            writer
                .push_record(make_rec(i + 1, 1, ot, i * 5, 1))
                .unwrap();
        }
        let infos = writer.finish().unwrap();
        let leaf_bytes = &infos[0].leaf_bytes;

        let header = decode_leaf_header_v3(leaf_bytes).unwrap();
        let decoded_dir = decode_leaf_dir_v3_with_base(leaf_bytes, &header).unwrap();
        let entry = &decoded_dir.entries[0];

        // Request only s_id and o_key.
        let proj = ColumnProjection {
            output: ColumnSet::single(ColumnId::SId).union(ColumnSet::single(ColumnId::OKey)),
            internal: ColumnSet::EMPTY,
        };
        let batch = load_leaflet_columns(
            leaf_bytes,
            entry,
            decoded_dir.payload_base,
            &proj,
            RunSortOrder::Post,
        )
        .unwrap();

        assert_eq!(batch.row_count, 3);
        // Requested columns are loaded.
        assert!(matches!(batch.s_id, ColumnData::Block(_)));
        assert!(matches!(batch.o_key, ColumnData::Block(_)));
        // Non-requested columns are AbsentDefault.
        assert!(batch.p_id.is_absent());
        assert!(batch.o_type.is_absent());
        assert!(batch.t.is_absent());
    }

    #[test]
    fn cached_loader_supports_leaflet_index_above_255() {
        let mut writer = LeafWriter::new(RunSortOrder::Post, 10, 10_000, 1);
        writer.set_skip_history(true);

        let ot = OType::XSD_INTEGER.as_u16();
        for i in 0..257u32 {
            // POST flushes on p_id transitions, so changing p_id per row guarantees
            // one-row leaflets and exercises the >255 leaflet index path.
            writer
                .push_record(make_rec(i as u64 + 1, i + 1, ot, i as u64, 1))
                .unwrap();
        }

        let infos = writer.finish().unwrap();
        assert_eq!(infos.len(), 1);
        let leaf_bytes = infos.into_iter().next().unwrap().leaf_bytes;

        let header = decode_leaf_header_v3(&leaf_bytes).unwrap();
        assert_eq!(header.leaflet_count, 257);

        let handle = FullBlobLeafHandle::new(leaf_bytes, None, 123).unwrap();
        let cache = LeafletCache::with_max_mb(16);

        let batch =
            load_columns_cached_via_handle(&handle, 256, RunSortOrder::Post, &cache, 123, 256)
                .unwrap();

        assert_eq!(batch.row_count, 1);
        assert_eq!(batch.s_id.get(0), 257);
        assert_eq!(batch.p_id.get(0), 257);
        assert_eq!(batch.o_key.get(0), 256);

        let cached = cache
            .get_v3_batch(&crate::read::leaflet_cache::V3BatchCacheKey {
                leaf_id: 123,
                leaflet_idx: 256,
            })
            .expect("cached batch for leaflet 256");
        assert_eq!(cached.s_id.get(0), 257);
        assert_eq!(cached.p_id.get(0), 257);
    }
}
