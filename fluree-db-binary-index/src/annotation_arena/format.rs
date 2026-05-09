//! Wire format for the edge-annotation forward/reverse arenas.
//!
//! ## Blob shape (forward leaf, reverse leaf, forward branch, reverse branch)
//!
//! ```text
//! [magic: 4B][version: u8][flags: u8][reserved: u16 = 0][body_len: u32 LE]
//! [body: postcard-encoded payload, body_len bytes]
//! ```
//!
//! Header is fixed at 12 bytes. The payload is a CBOR-encoded
//! [`AnnotationForwardLeaf`] / [`AnnotationReverseLeaf`] /
//! [`AnnotationForwardBranch`] / [`AnnotationReverseBranch`] respectively.
//!
//! CBOR (via `ciborium`) was chosen over postcard because [`FlakeValue`]
//! uses `#[serde(untagged)]` for the polymorphic object value, which
//! non-self-describing formats like postcard cannot deserialize.
//!
//! ## Sort orders
//!
//! - **Forward leaf rows**: `(EdgeKey, ann_sid, t, op)` — ascending.
//!   The leaf's first/last keys (held in the branch entry) are
//!   `(EdgeKey, ann_sid)` projections.
//! - **Reverse leaf rows**: `(ann_sid, EdgeKey, t, op)` — ascending.
//!   The branch routes on `ann_sid`.
//!
//! ## Empty blobs
//!
//! An empty leaf encodes to `body = postcard(empty Vec)` and is valid.
//! An empty branch likewise. Builders that produce no rows should still
//! emit an empty branch + leaf rather than skipping the section, so
//! `IndexRoot.annotation_index = None` keeps its "zero attachments"
//! correctness guarantee.
//!
//! See `EDGE_ANNOTATIONS.md` (Forward/Reverse Attachment Index) for the
//! design contract.

use fluree_db_core::{ContentId, EdgeKey, Sid};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Magic bytes for an edge-annotation forward branch (`EAFB1`).
pub const FORWARD_BRANCH_MAGIC: [u8; 4] = *b"EAFB";

/// Magic bytes for an edge-annotation forward leaf (`EAFL1`).
pub const FORWARD_LEAF_MAGIC: [u8; 4] = *b"EAFL";

/// Magic bytes for an edge-annotation reverse branch (`EARB1`).
pub const REVERSE_BRANCH_MAGIC: [u8; 4] = *b"EARB";

/// Magic bytes for an edge-annotation reverse leaf (`EARL1`).
pub const REVERSE_LEAF_MAGIC: [u8; 4] = *b"EARL";

/// Wire-format version for all four arena blob kinds.
pub const ARENA_VERSION: u8 = 1;

/// Header length (magic + version + flags + reserved + body_len).
pub const ARENA_HEADER_LEN: usize = 12;

/// Aggregate counters populated at build time, surfaced for cost-based
/// planning (M3) and storage inspection.
///
/// Counts are `u64` everywhere — the arena is content-addressed and can
/// in principle hold history-scale row counts, so 32-bit fields would be
/// risky on long-lived ledgers.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AnnotationStats {
    /// Total forward-arena rows (one per asserted/retracted attachment event).
    pub forward_rows: u64,
    /// Total reverse-arena rows (mirror of `forward_rows` after compaction).
    pub reverse_rows: u64,
    /// Distinct edges with at least one current (live) attachment.
    pub distinct_edges: u64,
    /// Distinct annotation subjects.
    pub distinct_annotations: u64,
}

/// Inline section in [`crate::format::index_root::IndexRoot`].
///
/// Absent (`None`) means the indexed snapshot has zero `f:reifies*`
/// flakes — a positive correctness guarantee, not a "don't know yet"
/// signal. Builders that are uncertain emit `Some(empty)` and let
/// reads no-op cheaply.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnnotationIndexRoot {
    /// Format version of the on-disk arena artifacts. Independent of
    /// `IndexRoot`'s own version so the arena format can roll forward
    /// without a full FIR6 bump.
    pub version: u8,
    /// Highest commit `t` reflected in either arena. Reads with `as_of_t`
    /// above this fall back to novelty for any newer attachments.
    pub max_t: i64,
    /// Forward-arena branch CID (`EAFB1`). Always present, even for an
    /// empty arena, so absence-of-section in the parent root keeps its
    /// "zero attachments" meaning.
    pub forward_branch_cid: ContentId,
    /// Reverse-arena branch CID (`EARB1`).
    pub reverse_branch_cid: ContentId,
    /// Build-time stats. Always present (zero-valued for empty arenas).
    pub stats: AnnotationStats,
}

// ── Forward arena ───────────────────────────────────────────────────────────

/// One row in a forward-arena leaf.
///
/// `(edge, ann)` are the routing key; `(t, op)` records the event so
/// history queries can replay the timeline. The on-disk sort order is
/// `(edge, ann, t, op)`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AnnotationForwardRow {
    pub edge: EdgeKey,
    pub ann: Sid,
    pub t: i64,
    /// `true` = assertion (attach), `false` = retraction (detach).
    pub op: bool,
}

/// Forward-arena leaf body. Rows are sorted by `(edge, ann, t, op)`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AnnotationForwardLeaf {
    pub rows: Vec<AnnotationForwardRow>,
}

/// One leaf entry in a forward-arena branch.
///
/// `first_key` / `last_key` are the inclusive `(edge, ann)` bounds for
/// the leaf, stored explicitly so the branch can binary-search on
/// `EdgeKey` without loading the leaf body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AnnotationForwardBranchEntry {
    pub first_edge: EdgeKey,
    pub first_ann: Sid,
    pub last_edge: EdgeKey,
    pub last_ann: Sid,
    pub row_count: u64,
    pub leaf_cid: ContentId,
}

/// Forward-arena branch body. Entries are sorted by `(first_edge, first_ann)`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AnnotationForwardBranch {
    pub leaves: Vec<AnnotationForwardBranchEntry>,
}

// ── Reverse arena ───────────────────────────────────────────────────────────

/// One row in a reverse-arena leaf.
///
/// On-disk sort order is `(ann, edge, t, op)`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AnnotationReverseRow {
    pub ann: Sid,
    pub edge: EdgeKey,
    pub t: i64,
    pub op: bool,
}

/// Reverse-arena leaf body. Rows are sorted by `(ann, edge, t, op)`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AnnotationReverseLeaf {
    pub rows: Vec<AnnotationReverseRow>,
}

/// One leaf entry in a reverse-arena branch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AnnotationReverseBranchEntry {
    pub first_ann: Sid,
    pub first_edge: EdgeKey,
    pub last_ann: Sid,
    pub last_edge: EdgeKey,
    pub row_count: u64,
    pub leaf_cid: ContentId,
}

/// Reverse-arena branch body. Entries are sorted by `(first_ann, first_edge)`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AnnotationReverseBranch {
    pub leaves: Vec<AnnotationReverseBranchEntry>,
}

// ── Wire encoding ───────────────────────────────────────────────────────────

#[derive(Debug, Error)]
pub enum DecodeError {
    #[error("blob shorter than {ARENA_HEADER_LEN}-byte header (got {0})")]
    TruncatedHeader(usize),
    #[error("magic mismatch: expected {expected:?}, got {got:?}")]
    BadMagic { expected: [u8; 4], got: [u8; 4] },
    #[error("unsupported version {0} (this build expects {ARENA_VERSION})")]
    UnsupportedVersion(u8),
    #[error("body length {declared} exceeds available bytes {available}")]
    TruncatedBody { declared: usize, available: usize },
    #[error("cbor decode error: {0}")]
    Cbor(String),
    #[error("non-zero reserved bytes: {0:#06x}")]
    NonZeroReserved(u16),
}

fn encode_blob<T: Serialize>(magic: [u8; 4], payload: &T) -> Vec<u8> {
    let mut body = Vec::new();
    ciborium::ser::into_writer(payload, &mut body)
        .expect("ciborium serialization to Vec<u8> is infallible");
    let mut out = Vec::with_capacity(ARENA_HEADER_LEN + body.len());
    out.extend_from_slice(&magic);
    out.push(ARENA_VERSION);
    out.push(0u8); // flags
    out.extend_from_slice(&0u16.to_le_bytes()); // reserved
    out.extend_from_slice(&(body.len() as u32).to_le_bytes());
    out.extend_from_slice(&body);
    out
}

fn decode_blob<T: for<'de> Deserialize<'de>>(
    bytes: &[u8],
    expected_magic: [u8; 4],
) -> Result<T, DecodeError> {
    if bytes.len() < ARENA_HEADER_LEN {
        return Err(DecodeError::TruncatedHeader(bytes.len()));
    }
    let magic: [u8; 4] = bytes[0..4].try_into().unwrap();
    if magic != expected_magic {
        return Err(DecodeError::BadMagic {
            expected: expected_magic,
            got: magic,
        });
    }
    let version = bytes[4];
    if version != ARENA_VERSION {
        return Err(DecodeError::UnsupportedVersion(version));
    }
    let _flags = bytes[5];
    let reserved = u16::from_le_bytes(bytes[6..8].try_into().unwrap());
    if reserved != 0 {
        return Err(DecodeError::NonZeroReserved(reserved));
    }
    let body_len = u32::from_le_bytes(bytes[8..12].try_into().unwrap()) as usize;
    let body_end = ARENA_HEADER_LEN
        .checked_add(body_len)
        .ok_or(DecodeError::TruncatedBody {
            declared: body_len,
            available: bytes.len().saturating_sub(ARENA_HEADER_LEN),
        })?;
    if body_end > bytes.len() {
        return Err(DecodeError::TruncatedBody {
            declared: body_len,
            available: bytes.len().saturating_sub(ARENA_HEADER_LEN),
        });
    }
    let payload = ciborium::de::from_reader::<T, _>(&bytes[ARENA_HEADER_LEN..body_end])
        .map_err(|e| DecodeError::Cbor(e.to_string()))?;
    Ok(payload)
}

impl AnnotationForwardLeaf {
    pub fn encode(&self) -> Vec<u8> {
        encode_blob(FORWARD_LEAF_MAGIC, self)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        decode_blob(bytes, FORWARD_LEAF_MAGIC)
    }
}

impl AnnotationForwardBranch {
    pub fn encode(&self) -> Vec<u8> {
        encode_blob(FORWARD_BRANCH_MAGIC, self)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        decode_blob(bytes, FORWARD_BRANCH_MAGIC)
    }
}

impl AnnotationReverseLeaf {
    pub fn encode(&self) -> Vec<u8> {
        encode_blob(REVERSE_LEAF_MAGIC, self)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        decode_blob(bytes, REVERSE_LEAF_MAGIC)
    }
}

impl AnnotationReverseBranch {
    pub fn encode(&self) -> Vec<u8> {
        encode_blob(REVERSE_BRANCH_MAGIC, self)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        decode_blob(bytes, REVERSE_BRANCH_MAGIC)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::{ContentId, ContentKind, FlakeValue, Sid};
    use fluree_vocab::xsd;

    fn sid(ns: u16, name: &str) -> Sid {
        Sid::new(ns, name)
    }

    fn cid(seed: &str) -> ContentId {
        ContentId::new(ContentKind::AnnotationForwardLeaf, seed.as_bytes())
    }

    fn sample_edge(idx: u8) -> EdgeKey {
        EdgeKey {
            g: if idx % 2 == 0 {
                None
            } else {
                Some(sid(10, &format!("g{idx}")))
            },
            s: sid(11, &format!("s{idx}")),
            p: sid(12, &format!("p{idx}")),
            o: FlakeValue::Ref(sid(11, &format!("o{idx}"))),
            dt: Sid::new(0, xsd::ANY_URI),
            lang: None,
            list_i: None,
        }
    }

    #[test]
    fn forward_leaf_roundtrip_empty() {
        let leaf = AnnotationForwardLeaf::default();
        let bytes = leaf.encode();
        // header-only bodies still carry a CBOR-encoded empty Vec.
        assert!(bytes.len() >= ARENA_HEADER_LEN);
        assert_eq!(&bytes[..4], &FORWARD_LEAF_MAGIC);
        let decoded = AnnotationForwardLeaf::decode(&bytes).unwrap();
        assert_eq!(decoded, leaf);
    }

    #[test]
    fn forward_leaf_roundtrip_multi() {
        let leaf = AnnotationForwardLeaf {
            rows: vec![
                AnnotationForwardRow {
                    edge: sample_edge(0),
                    ann: sid(20, "ann0"),
                    t: 1,
                    op: true,
                },
                AnnotationForwardRow {
                    edge: sample_edge(0),
                    ann: sid(20, "ann0"),
                    t: 2,
                    op: false,
                },
                AnnotationForwardRow {
                    edge: sample_edge(1),
                    ann: sid(20, "ann1"),
                    t: 3,
                    op: true,
                },
            ],
        };
        let bytes = leaf.encode();
        let decoded = AnnotationForwardLeaf::decode(&bytes).unwrap();
        assert_eq!(decoded, leaf);
    }

    #[test]
    fn forward_branch_roundtrip() {
        let branch = AnnotationForwardBranch {
            leaves: vec![AnnotationForwardBranchEntry {
                first_edge: sample_edge(0),
                first_ann: sid(20, "ann0"),
                last_edge: sample_edge(1),
                last_ann: sid(20, "ann1"),
                row_count: 3,
                leaf_cid: cid("forward-leaf-0"),
            }],
        };
        let bytes = branch.encode();
        assert_eq!(&bytes[..4], &FORWARD_BRANCH_MAGIC);
        let decoded = AnnotationForwardBranch::decode(&bytes).unwrap();
        assert_eq!(decoded, branch);
    }

    #[test]
    fn reverse_leaf_roundtrip() {
        let leaf = AnnotationReverseLeaf {
            rows: vec![
                AnnotationReverseRow {
                    ann: sid(20, "ann0"),
                    edge: sample_edge(0),
                    t: 1,
                    op: true,
                },
                AnnotationReverseRow {
                    ann: sid(20, "ann1"),
                    edge: sample_edge(1),
                    t: 2,
                    op: true,
                },
            ],
        };
        let bytes = leaf.encode();
        assert_eq!(&bytes[..4], &REVERSE_LEAF_MAGIC);
        let decoded = AnnotationReverseLeaf::decode(&bytes).unwrap();
        assert_eq!(decoded, leaf);
    }

    #[test]
    fn reverse_branch_roundtrip() {
        let branch = AnnotationReverseBranch {
            leaves: vec![AnnotationReverseBranchEntry {
                first_ann: sid(20, "ann0"),
                first_edge: sample_edge(0),
                last_ann: sid(20, "ann1"),
                last_edge: sample_edge(1),
                row_count: 2,
                leaf_cid: cid("reverse-leaf-0"),
            }],
        };
        let bytes = branch.encode();
        let decoded = AnnotationReverseBranch::decode(&bytes).unwrap();
        assert_eq!(decoded, branch);
    }

    #[test]
    fn rejects_truncated_header() {
        let err = AnnotationForwardLeaf::decode(&[0u8; 4]).unwrap_err();
        assert!(matches!(err, DecodeError::TruncatedHeader(4)));
    }

    #[test]
    fn rejects_wrong_magic() {
        let bytes = AnnotationForwardLeaf::default().encode();
        // Decoding forward bytes as reverse must fail loudly.
        let err = AnnotationReverseLeaf::decode(&bytes).unwrap_err();
        assert!(matches!(err, DecodeError::BadMagic { .. }));
    }

    #[test]
    fn rejects_unsupported_version() {
        let mut bytes = AnnotationForwardLeaf::default().encode();
        bytes[4] = ARENA_VERSION + 1;
        let err = AnnotationForwardLeaf::decode(&bytes).unwrap_err();
        assert!(matches!(err, DecodeError::UnsupportedVersion(_)));
    }

    #[test]
    fn rejects_truncated_body() {
        let bytes = AnnotationForwardLeaf {
            rows: vec![AnnotationForwardRow {
                edge: sample_edge(0),
                ann: sid(20, "ann0"),
                t: 1,
                op: true,
            }],
        }
        .encode();
        // Lop off the last 3 bytes of the body.
        let truncated = &bytes[..bytes.len() - 3];
        let err = AnnotationForwardLeaf::decode(truncated).unwrap_err();
        assert!(matches!(err, DecodeError::TruncatedBody { .. }));
    }

    #[test]
    fn rejects_non_zero_reserved() {
        let mut bytes = AnnotationForwardLeaf::default().encode();
        bytes[6] = 0xff;
        let err = AnnotationForwardLeaf::decode(&bytes).unwrap_err();
        assert!(matches!(err, DecodeError::NonZeroReserved(_)));
    }

    #[test]
    fn annotation_index_root_construct_and_clone() {
        // No on-disk format yet for AnnotationIndexRoot itself — it lives
        // inline in IndexRoot. Slice 2 wires it into FIR6 encoding. This
        // test pins the struct contract so accidental field renames here
        // surface immediately.
        let root = AnnotationIndexRoot {
            version: ARENA_VERSION,
            max_t: 42,
            forward_branch_cid: cid("fwd"),
            reverse_branch_cid: cid("rev"),
            stats: AnnotationStats {
                forward_rows: 10,
                reverse_rows: 10,
                distinct_edges: 4,
                distinct_annotations: 6,
            },
        };
        let cloned = root.clone();
        assert_eq!(root, cloned);
    }
}
