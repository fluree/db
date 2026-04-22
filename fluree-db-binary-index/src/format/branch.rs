//! V3 branch manifest (`FBR3`) — single-level routing for the FLI3 index format.
//!
//! Uses V2 record keys (30 bytes) and carries
//! `sidecar_cid` for each leaf (the single source of truth for locating
//! a leaf's history sidecar).
//!
//! ## Binary format
//!
//! ```text
//! [Header: 16 bytes]
//!   magic: "FBR3" (4B)
//!   version: u8 (= 1)
//!   order_id: u8
//!   g_id: u16 (LE)
//!   leaf_count: u32 (LE)
//!   _reserved: u32 (= 0)
//! [Leaf entries: leaf_count × variable]
//!   first_key: RunRecordV2 (30B, LE, graphless)
//!   last_key: RunRecordV2 (30B, LE, graphless)
//!   row_count: u64 (LE)
//!   leaf_cid_len: u16 (LE)
//!   leaf_cid_bytes: [u8; leaf_cid_len]
//!   has_sidecar: u8 (0 = no, 1 = yes)
//!   [if has_sidecar]
//!     sidecar_cid_len: u16 (LE)
//!     sidecar_cid_bytes: [u8; sidecar_cid_len]
//! ```

use super::run_record::RunSortOrder;
use super::run_record_v2::{cmp_v2_for_order, RunRecordV2, RECORD_V2_WIRE_SIZE};
use fluree_db_core::ContentId;
use std::cmp::Ordering;
use std::io;
use std::ops::Range;

/// Magic bytes for a V3 branch manifest.
const BRANCH_V3_MAGIC: [u8; 4] = *b"FBR3";

/// Current V3 branch manifest format version.
const BRANCH_V3_VERSION: u8 = 1;

/// Size of the branch header in bytes.
const BRANCH_V3_HEADER_LEN: usize = 16;

// ============================================================================
// BranchManifest (in-memory)
// ============================================================================

/// In-memory V3 branch manifest for query routing.
#[derive(Debug, Clone)]
pub struct BranchManifest {
    pub leaves: Vec<LeafEntry>,
}

/// A single leaf entry in the V3 branch manifest.
#[derive(Debug, Clone)]
pub struct LeafEntry {
    pub first_key: RunRecordV2,
    pub last_key: RunRecordV2,
    pub row_count: u64,
    /// Content identifier for the leaf blob.
    pub leaf_cid: ContentId,
    /// Content identifier for the per-leaf history sidecar (None if no history).
    /// This is the **single source of truth** for locating the sidecar.
    pub sidecar_cid: Option<ContentId>,
}

impl BranchManifest {
    /// Binary search for the leaf containing the given key using the given order.
    pub fn find_leaf(&self, key: &RunRecordV2, order: RunSortOrder) -> Option<&LeafEntry> {
        let cmp = cmp_v2_for_order(order);
        self.find_leaf_with_cmp(key, cmp)
    }

    /// Binary search using an explicit comparator.
    pub fn find_leaf_with_cmp(
        &self,
        key: &RunRecordV2,
        cmp: fn(&RunRecordV2, &RunRecordV2) -> Ordering,
    ) -> Option<&LeafEntry> {
        if self.leaves.is_empty() {
            return None;
        }
        let idx = self
            .leaves
            .partition_point(|entry| cmp(&entry.first_key, key) != Ordering::Greater);
        if idx == 0 {
            return None;
        }
        let candidate = &self.leaves[idx - 1];
        if cmp(key, &candidate.last_key) != Ordering::Greater {
            Some(candidate)
        } else {
            None
        }
    }

    /// Find all leaf indices whose key range overlaps [min_key, max_key].
    pub fn find_leaves_in_range(
        &self,
        min_key: &RunRecordV2,
        max_key: &RunRecordV2,
        cmp: fn(&RunRecordV2, &RunRecordV2) -> Ordering,
    ) -> Range<usize> {
        if self.leaves.is_empty() {
            return 0..0;
        }
        let start = self
            .leaves
            .partition_point(|entry| cmp(&entry.last_key, min_key) == Ordering::Less);
        let end = self
            .leaves
            .partition_point(|entry| cmp(&entry.first_key, max_key) != Ordering::Greater);
        start..end
    }

    /// Find leaves that may contain records for a given subject (SPOT order).
    pub fn find_leaves_for_subject(&self, s_id: u64) -> Range<usize> {
        if self.leaves.is_empty() {
            return 0..0;
        }
        let start = self
            .leaves
            .partition_point(|entry| entry.last_key.s_id.as_u64() < s_id);
        let end = self
            .leaves
            .partition_point(|entry| entry.first_key.s_id.as_u64() <= s_id);
        start..end
    }
}

// ============================================================================
// Encode (FBR3)
// ============================================================================

/// Build FBR3 branch manifest bytes in memory.
pub fn build_branch_bytes(order: RunSortOrder, g_id: u16, leaves: &[LeafEntry]) -> Vec<u8> {
    let estimated =
        BRANCH_V3_HEADER_LEN + leaves.len() * (2 * RECORD_V2_WIRE_SIZE + 8 + 2 + 40 + 1 + 2 + 40);
    let mut buf = Vec::with_capacity(estimated);

    // Header (16 bytes).
    buf.extend_from_slice(&BRANCH_V3_MAGIC);
    buf.push(BRANCH_V3_VERSION);
    buf.push(order.to_wire_id());
    buf.extend_from_slice(&g_id.to_le_bytes());
    buf.extend_from_slice(&(leaves.len() as u32).to_le_bytes());
    buf.extend_from_slice(&0u32.to_le_bytes()); // reserved

    // Leaf entries.
    let mut rec_buf = [0u8; RECORD_V2_WIRE_SIZE];
    for leaf in leaves {
        leaf.first_key.write_run_le(&mut rec_buf);
        buf.extend_from_slice(&rec_buf);

        leaf.last_key.write_run_le(&mut rec_buf);
        buf.extend_from_slice(&rec_buf);

        buf.extend_from_slice(&leaf.row_count.to_le_bytes());

        let cid_bytes = leaf.leaf_cid.to_bytes();
        buf.extend_from_slice(&(cid_bytes.len() as u16).to_le_bytes());
        buf.extend_from_slice(&cid_bytes);

        match &leaf.sidecar_cid {
            Some(sc) => {
                buf.push(1);
                let sc_bytes = sc.to_bytes();
                buf.extend_from_slice(&(sc_bytes.len() as u16).to_le_bytes());
                buf.extend_from_slice(&sc_bytes);
            }
            None => {
                buf.push(0);
            }
        }
    }

    buf
}

// ============================================================================
// Decode (FBR3)
// ============================================================================

/// Decode an FBR3 branch manifest from bytes.
pub fn read_branch_from_bytes(data: &[u8]) -> io::Result<BranchManifest> {
    if data.len() < BRANCH_V3_HEADER_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "branch v3 manifest too small for header",
        ));
    }
    if data[0..4] != BRANCH_V3_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("branch v3: expected magic FBR3, got {:?}", &data[0..4]),
        ));
    }
    let version = data[4];
    if version != BRANCH_V3_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("branch v3: unsupported version {version}"),
        ));
    }

    let leaf_count = u32::from_le_bytes(data[8..12].try_into().unwrap()) as usize;
    let mut leaves = Vec::with_capacity(leaf_count);
    let mut pos = BRANCH_V3_HEADER_LEN;

    for _ in 0..leaf_count {
        // first_key: 30 bytes
        if pos + RECORD_V2_WIRE_SIZE > data.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "FBR3: truncated at first_key",
            ));
        }
        let first_key =
            RunRecordV2::read_run_le(data[pos..pos + RECORD_V2_WIRE_SIZE].try_into().unwrap());
        pos += RECORD_V2_WIRE_SIZE;

        // last_key: 30 bytes
        if pos + RECORD_V2_WIRE_SIZE > data.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "FBR3: truncated at last_key",
            ));
        }
        let last_key =
            RunRecordV2::read_run_le(data[pos..pos + RECORD_V2_WIRE_SIZE].try_into().unwrap());
        pos += RECORD_V2_WIRE_SIZE;

        // row_count: 8 bytes
        if pos + 8 > data.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "FBR3: truncated at row_count",
            ));
        }
        let row_count = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
        pos += 8;

        // leaf_cid
        if pos + 2 > data.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "FBR3: truncated at cid_len",
            ));
        }
        let cid_len = u16::from_le_bytes(data[pos..pos + 2].try_into().unwrap()) as usize;
        pos += 2;
        if pos + cid_len > data.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "FBR3: CID bytes truncated",
            ));
        }
        let leaf_cid = ContentId::from_bytes(&data[pos..pos + cid_len]).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("FBR3: invalid CID: {e}"),
            )
        })?;
        pos += cid_len;

        // sidecar_cid
        if pos >= data.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "FBR3: truncated at has_sidecar",
            ));
        }
        let has_sidecar = data[pos];
        pos += 1;
        let sidecar_cid = if has_sidecar != 0 {
            if pos + 2 > data.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "FBR3: truncated at sidecar_cid_len",
                ));
            }
            let sc_len = u16::from_le_bytes(data[pos..pos + 2].try_into().unwrap()) as usize;
            pos += 2;
            if pos + sc_len > data.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "FBR3: sidecar CID truncated",
                ));
            }
            let sc = ContentId::from_bytes(&data[pos..pos + sc_len]).map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("FBR3: invalid sidecar CID: {e}"),
                )
            })?;
            pos += sc_len;
            Some(sc)
        } else {
            None
        };

        leaves.push(LeafEntry {
            first_key,
            last_key,
            row_count,
            leaf_cid,
            sidecar_cid,
        });
    }

    Ok(BranchManifest { leaves })
}

/// Read the sort order from an FBR3 header without full decode.
pub fn read_branch_order(data: &[u8]) -> io::Result<RunSortOrder> {
    if data.len() < BRANCH_V3_HEADER_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "FBR3: too small for header",
        ));
    }
    if data[0..4] != BRANCH_V3_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "FBR3: expected magic FBR3",
        ));
    }
    RunSortOrder::from_wire_id(data[5]).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("FBR3: invalid order_id {}", data[5]),
        )
    })
}

/// Read g_id from an FBR3 header without full decode.
pub fn read_branch_g_id(data: &[u8]) -> io::Result<u16> {
    if data.len() < BRANCH_V3_HEADER_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "FBR3: too small for header",
        ));
    }
    if data[0..4] != BRANCH_V3_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "FBR3: expected magic FBR3",
        ));
    }
    Ok(u16::from_le_bytes(data[6..8].try_into().unwrap()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::run_record::LIST_INDEX_NONE;
    use fluree_db_core::content_kind::{CODEC_FLUREE_INDEX_BRANCH, CODEC_FLUREE_INDEX_LEAF};
    use fluree_db_core::o_type::OType;
    use fluree_db_core::subject_id::SubjectId;

    fn make_rec(s_id: u64, p_id: u32, o_type: u16, o_key: u64, t: u32) -> RunRecordV2 {
        RunRecordV2 {
            s_id: SubjectId(s_id),
            o_key,
            p_id,
            t,
            o_i: LIST_INDEX_NONE,
            o_type,
            g_id: 0,
        }
    }

    fn make_leaf_cid(index: u32) -> ContentId {
        let data = format!("v3-leaf-{index}");
        ContentId::from_hex_digest(
            CODEC_FLUREE_INDEX_LEAF,
            &fluree_db_core::sha256_hex(data.as_bytes()),
        )
        .unwrap()
    }

    fn make_sidecar_cid(index: u32) -> ContentId {
        let data = format!("v3-sidecar-{index}");
        ContentId::from_hex_digest(
            CODEC_FLUREE_INDEX_BRANCH,
            &fluree_db_core::sha256_hex(data.as_bytes()),
        )
        .unwrap()
    }

    #[test]
    fn round_trip_no_sidecar() {
        let leaves = vec![
            LeafEntry {
                first_key: make_rec(1, 1, OType::XSD_INTEGER.as_u16(), 0, 1),
                last_key: make_rec(100, 5, OType::XSD_INTEGER.as_u16(), 99, 1),
                row_count: 5000,
                leaf_cid: make_leaf_cid(0),
                sidecar_cid: None,
            },
            LeafEntry {
                first_key: make_rec(101, 1, OType::XSD_STRING.as_u16(), 0, 1),
                last_key: make_rec(200, 10, OType::XSD_STRING.as_u16(), 50, 1),
                row_count: 3000,
                leaf_cid: make_leaf_cid(1),
                sidecar_cid: None,
            },
        ];

        let bytes = build_branch_bytes(RunSortOrder::Spot, 0, &leaves);
        let manifest = read_branch_from_bytes(&bytes).unwrap();

        assert_eq!(manifest.leaves.len(), 2);
        assert_eq!(manifest.leaves[0].row_count, 5000);
        assert_eq!(manifest.leaves[0].first_key.s_id.as_u64(), 1);
        assert_eq!(manifest.leaves[0].sidecar_cid, None);
        assert_eq!(manifest.leaves[1].row_count, 3000);
        assert_eq!(manifest.leaves[1].leaf_cid, make_leaf_cid(1));
    }

    #[test]
    fn round_trip_with_sidecar() {
        let leaves = vec![LeafEntry {
            first_key: make_rec(1, 1, OType::XSD_INTEGER.as_u16(), 0, 1),
            last_key: make_rec(100, 5, OType::XSD_INTEGER.as_u16(), 99, 5),
            row_count: 5000,
            leaf_cid: make_leaf_cid(0),
            sidecar_cid: Some(make_sidecar_cid(0)),
        }];

        let bytes = build_branch_bytes(RunSortOrder::Post, 2, &leaves);
        let manifest = read_branch_from_bytes(&bytes).unwrap();

        assert_eq!(manifest.leaves.len(), 1);
        assert_eq!(manifest.leaves[0].sidecar_cid, Some(make_sidecar_cid(0)));
    }

    #[test]
    fn header_metadata() {
        let bytes = build_branch_bytes(RunSortOrder::Opst, 42, &[]);
        assert_eq!(&bytes[0..4], b"FBR3");
        assert_eq!(bytes[4], 1);
        assert_eq!(read_branch_order(&bytes).unwrap(), RunSortOrder::Opst);
        assert_eq!(read_branch_g_id(&bytes).unwrap(), 42);
    }

    #[test]
    fn find_leaf_spot() {
        let leaves = vec![
            LeafEntry {
                first_key: make_rec(1, 1, OType::XSD_INTEGER.as_u16(), 0, 1),
                last_key: make_rec(100, 5, OType::XSD_INTEGER.as_u16(), 99, 1),
                row_count: 5000,
                leaf_cid: make_leaf_cid(0),
                sidecar_cid: None,
            },
            LeafEntry {
                first_key: make_rec(101, 1, OType::XSD_INTEGER.as_u16(), 0, 1),
                last_key: make_rec(200, 10, OType::XSD_INTEGER.as_u16(), 50, 1),
                row_count: 5000,
                leaf_cid: make_leaf_cid(1),
                sidecar_cid: None,
            },
        ];

        let bytes = build_branch_bytes(RunSortOrder::Spot, 0, &leaves);
        let manifest = read_branch_from_bytes(&bytes).unwrap();

        // Key in leaf 0.
        let key = make_rec(50, 1, OType::XSD_INTEGER.as_u16(), 0, 1);
        let found = manifest.find_leaf(&key, RunSortOrder::Spot).unwrap();
        assert_eq!(found.first_key.s_id.as_u64(), 1);

        // Key in leaf 1.
        let key = make_rec(150, 1, OType::XSD_INTEGER.as_u16(), 0, 1);
        let found = manifest.find_leaf(&key, RunSortOrder::Spot).unwrap();
        assert_eq!(found.first_key.s_id.as_u64(), 101);

        // Key before all.
        let key = make_rec(0, 0, OType::RESERVED.as_u16(), 0, 0);
        assert!(manifest.find_leaf(&key, RunSortOrder::Spot).is_none());
    }

    #[test]
    fn deterministic() {
        let leaves = vec![LeafEntry {
            first_key: make_rec(1, 1, OType::XSD_INTEGER.as_u16(), 0, 1),
            last_key: make_rec(100, 5, OType::XSD_INTEGER.as_u16(), 99, 1),
            row_count: 5000,
            leaf_cid: make_leaf_cid(0),
            sidecar_cid: Some(make_sidecar_cid(0)),
        }];

        let b1 = build_branch_bytes(RunSortOrder::Post, 0, &leaves);
        let b2 = build_branch_bytes(RunSortOrder::Post, 0, &leaves);
        assert_eq!(b1, b2);
    }
}
