//! Namespace-structured 64-bit subject ID.
//!
//! A `SubjectId` encodes both a namespace code and a per-namespace local ID into
//! a single `u64`:
//!
//! ```text
//! bits 63..48  namespace code (u16)
//! bits 47..0   local ID (u48, max 2^48 - 1 ≈ 281 trillion)
//! ```
//!
//! ## Physical Encoding Modes
//!
//! The index root declares a `SubjectIdEncoding` that determines how `SubjectId` values
//! are stored in leaflet columns:
//!
//! - **Narrow** (`ns16_local16`): stored as `u32 = (ns_code << 16) | local_id`.
//!   Valid only when every namespace's local_id fits in `u16`.
//! - **Wide** (`ns16_local48`): stored as the full `u64`.
//!
//! New databases start in narrow mode. Once any namespace exceeds `u16::MAX`
//! local IDs, the index transitions to wide mode permanently.

use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::Arc;

/// Maximum local ID that fits in narrow (u32) encoding.
const NARROW_LOCAL_MAX: u64 = u16::MAX as u64;

/// Canonical 64-bit subject ID: `(ns_code_u16 << 48) | local_id_u48`.
///
/// Provides namespace-aware ordering: subjects in the same namespace sort
/// together, enabling efficient prefix scans by namespace.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[repr(transparent)]
pub struct SubjectId(pub u64);

impl SubjectId {
    /// Create a new `SubjectId` from namespace code and local ID.
    ///
    /// # Panics
    ///
    /// Panics if `local_id` exceeds 48 bits (> 0xFFFF_FFFF_FFFF).
    #[inline]
    pub fn new(ns_code: u16, local_id: u64) -> Self {
        debug_assert!(
            local_id <= 0x0000_FFFF_FFFF_FFFF,
            "local_id exceeds 48 bits: {local_id}"
        );
        Self((ns_code as u64) << 48 | (local_id & 0x0000_FFFF_FFFF_FFFF))
    }

    /// Extract the namespace code (upper 16 bits).
    #[inline]
    pub fn ns_code(self) -> u16 {
        (self.0 >> 48) as u16
    }

    /// Extract the local ID (lower 48 bits).
    #[inline]
    pub fn local_id(self) -> u64 {
        self.0 & 0x0000_FFFF_FFFF_FFFF
    }

    /// Raw `u64` value.
    #[inline]
    pub fn as_u64(self) -> u64 {
        self.0
    }

    /// Construct from a raw `u64`.
    #[inline]
    pub fn from_u64(v: u64) -> Self {
        Self(v)
    }

    /// Try to down-convert to a narrow `u32` (`ns16_local16`).
    ///
    /// Returns `Some(sid32)` if `local_id <= u16::MAX`, `None` otherwise.
    #[inline]
    pub fn to_sid32(self) -> Option<u32> {
        let local = self.local_id();
        if local <= NARROW_LOCAL_MAX {
            Some(((self.ns_code() as u32) << 16) | local as u32)
        } else {
            None
        }
    }

    /// Promote a narrow `u32` (`ns16_local16`) to a full `SubjectId`.
    #[inline]
    pub fn from_sid32(sid32: u32) -> Self {
        let ns_code = (sid32 >> 16) as u16;
        let local_id = (sid32 & 0xFFFF) as u64;
        Self::new(ns_code, local_id)
    }

    /// Minimum possible `SubjectId` (namespace 0, local 0).
    #[inline]
    pub fn min() -> Self {
        Self(0)
    }

    /// Maximum possible `SubjectId`.
    #[inline]
    pub fn max() -> Self {
        Self(u64::MAX)
    }
}

impl fmt::Debug for SubjectId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "SubjectId(ns={}, local={})",
            self.ns_code(),
            self.local_id()
        )
    }
}

impl fmt::Display for SubjectId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.ns_code(), self.local_id())
    }
}

impl From<u64> for SubjectId {
    #[inline]
    fn from(v: u64) -> Self {
        Self(v)
    }
}

impl From<SubjectId> for u64 {
    #[inline]
    fn from(s: SubjectId) -> Self {
        s.0
    }
}

// === Serde: serialize as u64 ===

impl Serialize for SubjectId {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for SubjectId {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        u64::deserialize(deserializer).map(Self)
    }
}

// === Physical Encoding Mode ===

/// Physical encoding mode for subject IDs in leaflet columns.
///
/// Declared in the index root; determines how `SubjectId` values are
/// read from and written to persistent storage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub enum SubjectIdEncoding {
    /// Narrow: `u32 = (ns_code_u16 << 16) | local_id_u16`.
    /// Valid only when all local IDs fit in `u16`.
    #[default]
    Narrow,
    /// Wide: full `u64 = (ns_code_u16 << 48) | local_id_u48`.
    Wide,
}

// === SubjectIdColumn: compact array of subject IDs ===

/// Compact array of subject IDs, stored as either narrow (`u32`) or wide (`SubjectId`).
///
/// In narrow mode, each element is a `u32` in Sid32 format:
/// `(ns_code_u16 << 16) | local_id_u16`. In wide mode, each element is a
/// full `SubjectId`.
///
/// This enum exists to save memory in the LRU cache: narrow mode uses 4 bytes
/// per element instead of 8, yielding ~20% more cache capacity for index data.
///
/// Accessing an element always returns [`SubjectId`] — narrow values are promoted
/// via [`SubjectId::from_sid32()`].
#[derive(Clone, Debug)]
pub enum SubjectIdColumn {
    /// Narrow: 4 bytes per element. `u32 = (ns_code << 16) | local_id`.
    Narrow(Arc<[u32]>),
    /// Wide: 8 bytes per element. Full SubjectId values.
    Wide(Arc<[SubjectId]>),
}

impl SubjectIdColumn {
    /// Number of elements in this column.
    #[inline]
    pub fn len(&self) -> usize {
        match self {
            Self::Narrow(v) => v.len(),
            Self::Wide(v) => v.len(),
        }
    }

    /// True if the column is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get the SubjectId at the given index.
    ///
    /// Narrow values are promoted via `SubjectId::from_sid32()`.
    #[inline]
    pub fn get(&self, i: usize) -> SubjectId {
        match self {
            Self::Narrow(v) => SubjectId::from_sid32(v[i]),
            Self::Wide(v) => v[i],
        }
    }

    /// Byte size of the column data (for cache weighing).
    #[inline]
    pub fn byte_size(&self) -> usize {
        match self {
            Self::Narrow(v) => v.len() * 4,
            Self::Wide(v) => v.len() * 8,
        }
    }

    /// Construct from decoded `u64` values (raw SubjectId bit patterns).
    ///
    /// If `encoding` is `Narrow`, values are downconverted to u32 (Sid32 format).
    /// Panics in debug mode if any value doesn't fit in narrow format.
    pub fn from_u64_vec(values: Vec<u64>, encoding: SubjectIdEncoding) -> Self {
        match encoding {
            SubjectIdEncoding::Narrow => {
                let narrow: Vec<u32> = values
                    .iter()
                    .map(|&v| {
                        let sid = SubjectId::from_u64(v);
                        sid.to_sid32()
                            .expect("narrow mode but local_id exceeds u16")
                    })
                    .collect();
                Self::Narrow(narrow.into())
            }
            SubjectIdEncoding::Wide => {
                let wide: Vec<SubjectId> = values.into_iter().map(SubjectId::from_u64).collect();
                Self::Wide(wide.into())
            }
        }
    }

    /// Construct a narrow column directly from u32 values.
    pub fn from_narrow(values: Vec<u32>) -> Self {
        Self::Narrow(values.into())
    }

    /// Construct a wide column directly from SubjectId values.
    pub fn from_wide(values: Vec<SubjectId>) -> Self {
        Self::Wide(values.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_round_trip() {
        let sid = SubjectId::new(7, 42);
        assert_eq!(sid.ns_code(), 7);
        assert_eq!(sid.local_id(), 42);
    }

    #[test]
    fn test_max_local_id() {
        let max_local = 0x0000_FFFF_FFFF_FFFF_u64;
        let sid = SubjectId::new(0xFFFF, max_local);
        assert_eq!(sid.ns_code(), 0xFFFF);
        assert_eq!(sid.local_id(), max_local);
        assert_eq!(sid.as_u64(), u64::MAX);
    }

    #[test]
    fn test_ordering_namespace_first() {
        let a = SubjectId::new(1, 1000);
        let b = SubjectId::new(2, 0);
        assert!(a < b, "namespace 1 < namespace 2 regardless of local_id");
    }

    #[test]
    fn test_ordering_local_within_namespace() {
        let a = SubjectId::new(5, 10);
        let b = SubjectId::new(5, 20);
        assert!(a < b);
    }

    #[test]
    fn test_sid32_round_trip() {
        let sid = SubjectId::new(7, 1000);
        let sid32 = sid.to_sid32().expect("should fit in narrow");
        let restored = SubjectId::from_sid32(sid32);
        assert_eq!(restored, sid);
    }

    #[test]
    fn test_sid32_overflow() {
        let sid = SubjectId::new(1, 0x1_0000); // local_id = 65536, exceeds u16
        assert!(sid.to_sid32().is_none());
    }

    #[test]
    fn test_sid32_max_narrow() {
        let sid = SubjectId::new(0xFFFF, 0xFFFF);
        let sid32 = sid.to_sid32().unwrap();
        assert_eq!(sid32, u32::MAX);
        assert_eq!(SubjectId::from_sid32(sid32), sid);
    }

    #[test]
    fn test_min_max() {
        assert_eq!(SubjectId::min().as_u64(), 0);
        assert_eq!(SubjectId::max().as_u64(), u64::MAX);
        assert!(SubjectId::min() < SubjectId::new(0, 1));
        assert!(SubjectId::new(u16::MAX, 0x0000_FFFF_FFFF_FFFF) <= SubjectId::max());
    }

    #[test]
    fn test_serde_round_trip() {
        let sid = SubjectId::new(7, 42);
        let json = serde_json::to_string(&sid).unwrap();
        let parsed: SubjectId = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, sid);
    }

    #[test]
    fn test_sid_column_narrow() {
        let col = SubjectIdColumn::from_u64_vec(
            vec![
                SubjectId::new(2, 10).as_u64(),
                SubjectId::new(2, 20).as_u64(),
                SubjectId::new(3, 5).as_u64(),
            ],
            SubjectIdEncoding::Narrow,
        );
        assert_eq!(col.len(), 3);
        assert_eq!(col.byte_size(), 12); // 3 × 4
        assert_eq!(col.get(0), SubjectId::new(2, 10));
        assert_eq!(col.get(1), SubjectId::new(2, 20));
        assert_eq!(col.get(2), SubjectId::new(3, 5));
        assert!(matches!(col, SubjectIdColumn::Narrow(_)));
    }

    #[test]
    fn test_sid_column_wide() {
        let col = SubjectIdColumn::from_u64_vec(
            vec![
                SubjectId::new(2, 0x1_0000).as_u64(), // exceeds u16
                SubjectId::new(3, 5).as_u64(),
            ],
            SubjectIdEncoding::Wide,
        );
        assert_eq!(col.len(), 2);
        assert_eq!(col.byte_size(), 16); // 2 × 8
        assert_eq!(col.get(0), SubjectId::new(2, 0x1_0000));
        assert_eq!(col.get(1), SubjectId::new(3, 5));
        assert!(matches!(col, SubjectIdColumn::Wide(_)));
    }

    #[test]
    fn test_sid_encoding_serde() {
        let narrow = SubjectIdEncoding::Narrow;
        let json = serde_json::to_string(&narrow).unwrap();
        assert_eq!(json, "\"narrow\"");
        let parsed: SubjectIdEncoding = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, SubjectIdEncoding::Narrow);

        let wide = SubjectIdEncoding::Wide;
        let json = serde_json::to_string(&wide).unwrap();
        assert_eq!(json, "\"wide\"");
    }
}
