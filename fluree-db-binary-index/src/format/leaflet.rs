//! V3 leaflet encoder for the FLI3 columnar index format.
//!
//! A leaflet is the unit of routing + selective decode + caching. Each leaflet
//! contains independently compressed column blocks. The column set depends on
//! the index sort order and whether constant-column optimizations apply.
//!
//! ## Column selection by order
//!
//! | Order | Constants | Core columns | Optional |
//! |-------|-----------|-------------|----------|
//! | POST  | `p_const` | `o_key, s_id, t` | `o_type` (when mixed), `o_i` |
//! | PSOT  | `p_const` | `s_id, o_key, t` | `o_type` (when mixed), `o_i` |
//! | SPOT  | (none)    | `s_id, p_id, o_key, t` | `o_type` (when mixed), `o_i` |
//! | OPST  | `o_type_const` | `o_key, p_id, s_id, t` | `o_i` |
//!
//! When a leaflet has a single `o_type` value for all rows (common for
//! predicate-homogeneous POST/PSOT leaflets), the `o_type` column is omitted
//! and `o_type_const` is set in the directory entry. Readers use the constant
//! instead of decoding a column block.

use super::column_block::{
    encode_column_u16, encode_column_u32, encode_column_u64, ColumnBlockRef, ColumnId,
};
use super::run_record::RunSortOrder;
use super::run_record::LIST_INDEX_NONE;
use super::run_record_v2::{write_ordered_key_v2, RunRecordV2, ORDERED_KEY_V2_SIZE};

/// Bitflags for leaflet directory entries.
pub mod flags {
    /// The leaflet contains an `o_i` column (at least one row has o_i != sentinel).
    pub const HAS_O_I: u32 = 1 << 0;
    /// The leaflet contains a per-row `o_type` column (multiple types present).
    pub const HAS_O_TYPE_COL: u32 = 1 << 1;
}

/// Result of encoding a single leaflet's column blocks.
pub struct EncodedLeaflet {
    /// Number of rows in this leaflet.
    pub row_count: u32,
    /// Number of distinct values of the leading sort key.
    pub lead_group_count: u32,
    /// Routing key for the first row (order-specific, 26 bytes).
    pub first_key: [u8; ORDERED_KEY_V2_SIZE],
    /// Routing key for the last row (order-specific, 26 bytes).
    pub last_key: [u8; ORDERED_KEY_V2_SIZE],
    /// Constant `p_id` for POST/PSOT leaflets (always `Some` for those orders).
    pub p_const: Option<u32>,
    /// Constant `o_type` when all rows share the same type. Set for:
    /// - OPST (always, by segmentation design)
    /// - POST/PSOT/SPOT (when single-type predicate or leaflet — optimization)
    pub o_type_const: Option<u16>,
    /// Bitflags (HAS_O_I, HAS_O_TYPE_COL, etc.).
    pub flags: u32,
    /// Per-column block references (offset is relative to payload start).
    pub column_refs: Vec<ColumnBlockRef>,
    /// Concatenated compressed column block bytes.
    pub payload: Vec<u8>,
}

/// Encode a buffer of V2 records as a V3 leaflet for the given sort order.
///
/// Records MUST be pre-sorted in the given order and (for POST/PSOT)
/// predicate-homogeneous, (for OPST) type-homogeneous.
pub fn encode_leaflet(
    records: &[RunRecordV2],
    order: RunSortOrder,
    zstd_level: i32,
) -> std::io::Result<EncodedLeaflet> {
    if records.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "cannot encode empty leaflet",
        ));
    }

    // Validate segmentation invariants.
    let first = &records[0];
    match order {
        RunSortOrder::Post | RunSortOrder::Psot => {
            debug_assert!(
                records.iter().all(|r| r.p_id == first.p_id),
                "POST/PSOT leaflet must be predicate-homogeneous (got mixed p_id)"
            );
        }
        RunSortOrder::Opst => {
            debug_assert!(
                records.iter().all(|r| r.o_type == first.o_type),
                "OPST leaflet must be type-homogeneous (got mixed o_type)"
            );
        }
        RunSortOrder::Spot => {}
    }

    let row_count = records.len() as u32;

    // Compute first/last routing keys.
    let mut first_key = [0u8; ORDERED_KEY_V2_SIZE];
    let mut last_key = [0u8; ORDERED_KEY_V2_SIZE];
    write_ordered_key_v2(order, first, &mut first_key);
    write_ordered_key_v2(order, records.last().unwrap(), &mut last_key);

    // Determine constants and flags.
    let p_const = match order {
        RunSortOrder::Post | RunSortOrder::Psot => Some(first.p_id),
        _ => None,
    };

    // o_type_const: always set for OPST (type-homogeneous by design).
    // For POST/PSOT: set when the predicate has a single datatype (common case).
    // For SPOT: never set — SPOT leaflets are almost always mixed-type and
    // the check would be wasted work.
    let o_type_const = match order {
        RunSortOrder::Opst => Some(first.o_type),
        RunSortOrder::Post | RunSortOrder::Psot => {
            let first_o_type = first.o_type;
            if records.iter().all(|r| r.o_type == first_o_type) {
                Some(first_o_type)
            } else {
                None
            }
        }
        RunSortOrder::Spot => None,
    };

    // Check if any row has a list index.
    let has_o_i = records.iter().any(|r| r.o_i != LIST_INDEX_NONE);

    let mut fl = 0u32;
    if has_o_i {
        fl |= flags::HAS_O_I;
    }
    if o_type_const.is_none() {
        fl |= flags::HAS_O_TYPE_COL;
    }

    // Compute lead_group_count.
    let lead_group_count = compute_lead_group_count(records, order);

    // Extract column vectors and encode.
    let s_ids: Vec<u64> = records.iter().map(|r| r.s_id.as_u64()).collect();
    let o_keys: Vec<u64> = records.iter().map(|r| r.o_key).collect();
    let t_vals: Vec<u32> = records.iter().map(|r| r.t).collect();

    let mut payload = Vec::new();
    let mut column_refs = Vec::new();

    // Encode columns in a canonical order that matches typical access patterns.
    // The order within the payload doesn't affect correctness (readers use
    // ColumnBlockRef offsets), but putting hot columns first may improve
    // prefetch locality for sequential reads.

    match order {
        RunSortOrder::Post => {
            // p_const: p_id omitted. Hot path: o_key for aggregates.
            // o_type column only if mixed types.
            if o_type_const.is_none() {
                let o_types: Vec<u16> = records.iter().map(|r| r.o_type).collect();
                append_column_u16(
                    &mut payload,
                    &mut column_refs,
                    ColumnId::OType,
                    &o_types,
                    zstd_level,
                )?;
            }
            append_column_u64(
                &mut payload,
                &mut column_refs,
                ColumnId::OKey,
                &o_keys,
                zstd_level,
            )?;
            append_column_u64(
                &mut payload,
                &mut column_refs,
                ColumnId::SId,
                &s_ids,
                zstd_level,
            )?;
            append_column_u32(
                &mut payload,
                &mut column_refs,
                ColumnId::T,
                &t_vals,
                zstd_level,
            )?;
        }
        RunSortOrder::Psot => {
            // p_const: p_id omitted. Hot path: s_id for subject scans.
            append_column_u64(
                &mut payload,
                &mut column_refs,
                ColumnId::SId,
                &s_ids,
                zstd_level,
            )?;
            if o_type_const.is_none() {
                let o_types: Vec<u16> = records.iter().map(|r| r.o_type).collect();
                append_column_u16(
                    &mut payload,
                    &mut column_refs,
                    ColumnId::OType,
                    &o_types,
                    zstd_level,
                )?;
            }
            append_column_u64(
                &mut payload,
                &mut column_refs,
                ColumnId::OKey,
                &o_keys,
                zstd_level,
            )?;
            append_column_u32(
                &mut payload,
                &mut column_refs,
                ColumnId::T,
                &t_vals,
                zstd_level,
            )?;
        }
        RunSortOrder::Spot => {
            // No constant columns. Full set.
            append_column_u64(
                &mut payload,
                &mut column_refs,
                ColumnId::SId,
                &s_ids,
                zstd_level,
            )?;
            let p_ids: Vec<u32> = records.iter().map(|r| r.p_id).collect();
            append_column_u32(
                &mut payload,
                &mut column_refs,
                ColumnId::PId,
                &p_ids,
                zstd_level,
            )?;
            if o_type_const.is_none() {
                let o_types: Vec<u16> = records.iter().map(|r| r.o_type).collect();
                append_column_u16(
                    &mut payload,
                    &mut column_refs,
                    ColumnId::OType,
                    &o_types,
                    zstd_level,
                )?;
            }
            append_column_u64(
                &mut payload,
                &mut column_refs,
                ColumnId::OKey,
                &o_keys,
                zstd_level,
            )?;
            append_column_u32(
                &mut payload,
                &mut column_refs,
                ColumnId::T,
                &t_vals,
                zstd_level,
            )?;
        }
        RunSortOrder::Opst => {
            // o_type_const always set (type-homogeneous by segmentation).
            // p_id and s_id are columns.
            append_column_u64(
                &mut payload,
                &mut column_refs,
                ColumnId::OKey,
                &o_keys,
                zstd_level,
            )?;
            let p_ids: Vec<u32> = records.iter().map(|r| r.p_id).collect();
            append_column_u32(
                &mut payload,
                &mut column_refs,
                ColumnId::PId,
                &p_ids,
                zstd_level,
            )?;
            append_column_u64(
                &mut payload,
                &mut column_refs,
                ColumnId::SId,
                &s_ids,
                zstd_level,
            )?;
            append_column_u32(
                &mut payload,
                &mut column_refs,
                ColumnId::T,
                &t_vals,
                zstd_level,
            )?;
        }
    }

    // Optional o_i column (only if any row has a list index).
    if has_o_i {
        let o_i_vals: Vec<u32> = records.iter().map(|r| r.o_i).collect();
        append_column_u32(
            &mut payload,
            &mut column_refs,
            ColumnId::OI,
            &o_i_vals,
            zstd_level,
        )?;
    }

    Ok(EncodedLeaflet {
        row_count,
        lead_group_count,
        first_key,
        last_key,
        p_const,
        o_type_const,
        flags: fl,
        column_refs,
        payload,
    })
}

/// Compute `lead_group_count`: the number of distinct leading-key values.
///
/// - POST: distinct `(o_type, o_key)` (p is constant, O is leading after P)
/// - PSOT: distinct `s_id` (P is constant, S is leading after P)
/// - SPOT: distinct `s_id`
/// - OPST: distinct `(o_type, o_key)` (o_type is constant, but conceptually O is leading)
///
/// Note: `o_i` does NOT participate in the distinct count per proposal §3.2.
fn compute_lead_group_count(records: &[RunRecordV2], order: RunSortOrder) -> u32 {
    if records.is_empty() {
        return 0;
    }

    match order {
        RunSortOrder::Post | RunSortOrder::Opst => {
            // Count distinct (o_type, o_key).
            let mut count = 1u32;
            for i in 1..records.len() {
                if records[i].o_type != records[i - 1].o_type
                    || records[i].o_key != records[i - 1].o_key
                {
                    count += 1;
                }
            }
            count
        }
        RunSortOrder::Psot | RunSortOrder::Spot => {
            // Count distinct s_id.
            let mut count = 1u32;
            for i in 1..records.len() {
                if records[i].s_id != records[i - 1].s_id {
                    count += 1;
                }
            }
            count
        }
    }
}

// ── Helpers ────────────────────────────────────────────────────────────

fn append_column_u64(
    payload: &mut Vec<u8>,
    refs: &mut Vec<ColumnBlockRef>,
    col_id: ColumnId,
    values: &[u64],
    zstd_level: i32,
) -> std::io::Result<()> {
    let (compressed, mut block_ref) = encode_column_u64(col_id, values, zstd_level)?;
    block_ref.offset = payload.len() as u32;
    payload.extend_from_slice(&compressed);
    refs.push(block_ref);
    Ok(())
}

fn append_column_u32(
    payload: &mut Vec<u8>,
    refs: &mut Vec<ColumnBlockRef>,
    col_id: ColumnId,
    values: &[u32],
    zstd_level: i32,
) -> std::io::Result<()> {
    let (compressed, mut block_ref) = encode_column_u32(col_id, values, zstd_level)?;
    block_ref.offset = payload.len() as u32;
    payload.extend_from_slice(&compressed);
    refs.push(block_ref);
    Ok(())
}

fn append_column_u16(
    payload: &mut Vec<u8>,
    refs: &mut Vec<ColumnBlockRef>,
    col_id: ColumnId,
    values: &[u16],
    zstd_level: i32,
) -> std::io::Result<()> {
    let (compressed, mut block_ref) = encode_column_u16(col_id, values, zstd_level)?;
    block_ref.offset = payload.len() as u32;
    payload.extend_from_slice(&compressed);
    refs.push(block_ref);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::column_block::{decode_column_u16, decode_column_u32, decode_column_u64};
    use crate::format::run_record_v2::RunRecordV2;
    use fluree_db_core::o_type::OType;
    use fluree_db_core::subject_id::SubjectId;

    fn make_rec(s_id: u64, p_id: u32, o_type: u16, o_key: u64, o_i: u32, t: u32) -> RunRecordV2 {
        RunRecordV2 {
            s_id: SubjectId(s_id),
            o_key,
            p_id,
            t,
            o_i,
            o_type,
            g_id: 0,
        }
    }

    #[test]
    fn post_single_type_omits_o_type_column() {
        let records = vec![
            make_rec(10, 1, OType::XSD_INTEGER.as_u16(), 100, LIST_INDEX_NONE, 1),
            make_rec(20, 1, OType::XSD_INTEGER.as_u16(), 200, LIST_INDEX_NONE, 2),
            make_rec(30, 1, OType::XSD_INTEGER.as_u16(), 300, LIST_INDEX_NONE, 3),
        ];
        let enc = encode_leaflet(&records, RunSortOrder::Post, 1).unwrap();
        assert_eq!(enc.row_count, 3);
        assert_eq!(enc.p_const, Some(1));
        assert_eq!(enc.o_type_const, Some(OType::XSD_INTEGER.as_u16()));
        assert_eq!(enc.flags & flags::HAS_O_TYPE_COL, 0); // no o_type column
        assert_eq!(enc.flags & flags::HAS_O_I, 0); // no o_i column
                                                   // Columns: o_key, s_id, t (3 columns, no p_id, no o_type)
        assert_eq!(enc.column_refs.len(), 3);
    }

    #[test]
    fn post_mixed_type_includes_o_type_column() {
        let records = vec![
            make_rec(10, 1, OType::XSD_INTEGER.as_u16(), 100, LIST_INDEX_NONE, 1),
            make_rec(20, 1, OType::XSD_STRING.as_u16(), 200, LIST_INDEX_NONE, 2),
        ];
        let enc = encode_leaflet(&records, RunSortOrder::Post, 1).unwrap();
        assert_eq!(enc.o_type_const, None);
        assert_ne!(enc.flags & flags::HAS_O_TYPE_COL, 0);
        // Columns: o_type, o_key, s_id, t (4 columns)
        assert_eq!(enc.column_refs.len(), 4);
    }

    #[test]
    fn spot_includes_all_columns() {
        let records = vec![
            make_rec(10, 1, OType::XSD_INTEGER.as_u16(), 100, LIST_INDEX_NONE, 1),
            make_rec(10, 2, OType::XSD_STRING.as_u16(), 200, LIST_INDEX_NONE, 2),
        ];
        let enc = encode_leaflet(&records, RunSortOrder::Spot, 1).unwrap();
        assert_eq!(enc.p_const, None);
        assert_eq!(enc.o_type_const, None);
        // Columns: s_id, p_id, o_type, o_key, t (5 columns)
        assert_eq!(enc.column_refs.len(), 5);
    }

    #[test]
    fn opst_omits_o_type_column() {
        let records = vec![
            make_rec(10, 1, OType::XSD_INTEGER.as_u16(), 100, LIST_INDEX_NONE, 1),
            make_rec(20, 2, OType::XSD_INTEGER.as_u16(), 200, LIST_INDEX_NONE, 2),
        ];
        let enc = encode_leaflet(&records, RunSortOrder::Opst, 1).unwrap();
        assert_eq!(enc.o_type_const, Some(OType::XSD_INTEGER.as_u16()));
        assert_eq!(enc.flags & flags::HAS_O_TYPE_COL, 0);
        // Columns: o_key, p_id, s_id, t (4 columns)
        assert_eq!(enc.column_refs.len(), 4);
    }

    #[test]
    fn o_i_column_present_when_needed() {
        let records = vec![
            make_rec(10, 1, OType::XSD_STRING.as_u16(), 100, 0, 1),
            make_rec(10, 1, OType::XSD_STRING.as_u16(), 100, 1, 2),
            make_rec(10, 1, OType::XSD_STRING.as_u16(), 100, LIST_INDEX_NONE, 3),
        ];
        let enc = encode_leaflet(&records, RunSortOrder::Post, 1).unwrap();
        assert_ne!(enc.flags & flags::HAS_O_I, 0);
        // o_type_const present (all same type), so: o_key, s_id, t, o_i = 4 columns
        assert_eq!(enc.column_refs.len(), 4);
    }

    #[test]
    fn lead_group_count_post() {
        // POST with 3 distinct (o_type, o_key) pairs.
        let records = vec![
            make_rec(10, 1, OType::XSD_INTEGER.as_u16(), 100, LIST_INDEX_NONE, 1),
            make_rec(20, 1, OType::XSD_INTEGER.as_u16(), 100, LIST_INDEX_NONE, 2),
            make_rec(30, 1, OType::XSD_INTEGER.as_u16(), 200, LIST_INDEX_NONE, 3),
            make_rec(40, 1, OType::XSD_INTEGER.as_u16(), 300, LIST_INDEX_NONE, 4),
        ];
        let enc = encode_leaflet(&records, RunSortOrder::Post, 1).unwrap();
        assert_eq!(enc.lead_group_count, 3); // 100, 200, 300
    }

    #[test]
    fn lead_group_count_psot() {
        // PSOT: count distinct s_id.
        let records = vec![
            make_rec(10, 1, OType::XSD_INTEGER.as_u16(), 100, LIST_INDEX_NONE, 1),
            make_rec(10, 1, OType::XSD_INTEGER.as_u16(), 200, LIST_INDEX_NONE, 2),
            make_rec(20, 1, OType::XSD_INTEGER.as_u16(), 300, LIST_INDEX_NONE, 3),
        ];
        let enc = encode_leaflet(&records, RunSortOrder::Psot, 1).unwrap();
        assert_eq!(enc.lead_group_count, 2); // s_id 10, 20
    }

    #[test]
    fn column_data_roundtrip() {
        let records = vec![
            make_rec(100, 5, OType::XSD_INTEGER.as_u16(), 42, LIST_INDEX_NONE, 7),
            make_rec(200, 5, OType::XSD_INTEGER.as_u16(), 43, LIST_INDEX_NONE, 8),
        ];
        let enc = encode_leaflet(&records, RunSortOrder::Post, 1).unwrap();

        // Verify we can decode the o_key column.
        let o_key_ref = enc
            .column_refs
            .iter()
            .find(|r| r.col_id == ColumnId::OKey.to_u16())
            .unwrap();
        let o_keys = decode_column_u64(&enc.payload, o_key_ref).unwrap();
        assert_eq!(o_keys, vec![42, 43]);

        // Verify s_id column.
        let s_id_ref = enc
            .column_refs
            .iter()
            .find(|r| r.col_id == ColumnId::SId.to_u16())
            .unwrap();
        let s_ids = decode_column_u64(&enc.payload, s_id_ref).unwrap();
        assert_eq!(s_ids, vec![100, 200]);

        // Verify t column.
        let t_ref = enc
            .column_refs
            .iter()
            .find(|r| r.col_id == ColumnId::T.to_u16())
            .unwrap();
        let t_vals = decode_column_u32(&enc.payload, t_ref).unwrap();
        assert_eq!(t_vals, vec![7, 8]);
    }

    #[test]
    fn spot_always_includes_o_type_column() {
        // SPOT never sets o_type_const — SPOT leaflets are almost always mixed-type
        // and the check would be wasted work in the hot path.
        let records = vec![
            make_rec(1, 1, OType::XSD_STRING.as_u16(), 10, LIST_INDEX_NONE, 1),
            make_rec(1, 2, OType::XSD_STRING.as_u16(), 20, LIST_INDEX_NONE, 2),
        ];
        let enc = encode_leaflet(&records, RunSortOrder::Spot, 1).unwrap();
        assert_eq!(enc.o_type_const, None);
        assert_ne!(enc.flags & flags::HAS_O_TYPE_COL, 0);
        // s_id, p_id, o_type, o_key, t = 5 columns
        assert_eq!(enc.column_refs.len(), 5);
    }

    #[test]
    fn mixed_o_type_produces_decodable_column() {
        let records = vec![
            make_rec(10, 1, OType::XSD_INTEGER.as_u16(), 42, LIST_INDEX_NONE, 1),
            make_rec(20, 1, OType::XSD_DOUBLE.as_u16(), 43, LIST_INDEX_NONE, 2),
        ];
        let enc = encode_leaflet(&records, RunSortOrder::Post, 1).unwrap();
        assert!(enc.o_type_const.is_none());

        let ot_ref = enc
            .column_refs
            .iter()
            .find(|r| r.col_id == ColumnId::OType.to_u16())
            .unwrap();
        let o_types = decode_column_u16(&enc.payload, ot_ref).unwrap();
        assert_eq!(
            o_types,
            vec![OType::XSD_INTEGER.as_u16(), OType::XSD_DOUBLE.as_u16()]
        );
    }
}
