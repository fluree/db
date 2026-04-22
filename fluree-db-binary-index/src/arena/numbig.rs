//! Per-predicate equality-only arena for overflow BigInt and BigDecimal values.
//!
//! Values that don't fit `NumInt` (i64) or `NumF64` (finite f64) are stored
//! here with sequential `u32` handles. The handle is zero-extended to `u64`
//! and placed in `ObjKey` with `ObjKind::NUM_BIG`.
//!
//! **No numeric ordering is maintained** -- range queries on `NumBig` values
//! fall back to scan + post-filter using `FlakeValue::numeric_cmp`.

use bigdecimal::BigDecimal;
use num_bigint::BigInt;
use std::collections::HashMap;
use std::io;

// ============================================================================
// NumBigRepr -- canonical byte representation for dedup
// ============================================================================

/// Canonical byte representation used as the dedup key.
#[derive(Clone, Debug)]
pub enum NumBigRepr {
    /// BigInt canonical signed LE bytes.
    BigIntBytes(Vec<u8>),
    /// BigDecimal: unscaled BigInt LE bytes + scale.
    BigDecBytes { unscaled: Vec<u8>, scale: i64 },
}

impl PartialEq for NumBigRepr {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::BigIntBytes(a), Self::BigIntBytes(b)) => a == b,
            (
                Self::BigDecBytes {
                    unscaled: a_u,
                    scale: a_s,
                },
                Self::BigDecBytes {
                    unscaled: b_u,
                    scale: b_s,
                },
            ) => a_u == b_u && a_s == b_s,
            _ => false,
        }
    }
}

impl Eq for NumBigRepr {}

impl std::hash::Hash for NumBigRepr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            Self::BigIntBytes(bytes) => bytes.hash(state),
            Self::BigDecBytes { unscaled, scale } => {
                unscaled.hash(state);
                scale.hash(state);
            }
        }
    }
}

// ============================================================================
// StoredBigValue -- forward lookup from handle -> value
// ============================================================================

/// Stored value for forward lookup (handle -> FlakeValue reconstruction).
#[derive(Clone, Debug)]
pub enum StoredBigValue {
    BigInt(Vec<u8>),
    BigDec { unscaled: Vec<u8>, scale: i64 },
}

impl StoredBigValue {
    /// Reconstruct a `FlakeValue` from the stored bytes.
    pub fn to_flake_value(&self) -> fluree_db_core::value::FlakeValue {
        use fluree_db_core::value::FlakeValue;
        match self {
            Self::BigInt(bytes) => {
                let bi = BigInt::from_signed_bytes_le(bytes);
                if let Some(v) = num_traits::ToPrimitive::to_i64(&bi) {
                    FlakeValue::Long(v)
                } else {
                    FlakeValue::BigInt(Box::new(bi))
                }
            }
            Self::BigDec { unscaled, scale } => {
                let bi = BigInt::from_signed_bytes_le(unscaled);
                FlakeValue::Decimal(Box::new(BigDecimal::new(bi, *scale)))
            }
        }
    }
}

// ============================================================================
// NumBigArena -- per-predicate equality-only arena
// ============================================================================

/// Per-predicate equality-only arena for overflow numeric values.
///
/// Handles are sequential `u32` values (0, 1, 2, ...) assigned in insertion
/// order. **Not** numerically ordered -- range queries post-filter.
#[derive(Debug)]
pub struct NumBigArena {
    /// Dedup: repr -> handle.
    dedup: HashMap<NumBigRepr, u32>,
    /// Forward: handle -> stored value.
    values: Vec<StoredBigValue>,
}

impl NumBigArena {
    pub fn new() -> Self {
        Self {
            dedup: HashMap::new(),
            values: Vec::new(),
        }
    }

    /// Look up or insert a BigInt, returning its handle.
    pub fn get_or_insert_bigint(&mut self, bi: &BigInt) -> u32 {
        let bytes = bi.to_signed_bytes_le();
        let repr = NumBigRepr::BigIntBytes(bytes.clone());
        if let Some(&handle) = self.dedup.get(&repr) {
            return handle;
        }
        let handle = self.values.len() as u32;
        self.values.push(StoredBigValue::BigInt(bytes));
        self.dedup.insert(repr, handle);
        handle
    }

    /// Look up or insert a BigDecimal, returning its handle.
    pub fn get_or_insert_bigdec(&mut self, bd: &BigDecimal) -> u32 {
        let (unscaled_bi, scale) = bd.as_bigint_and_exponent();
        let unscaled = unscaled_bi.to_signed_bytes_le();
        let repr = NumBigRepr::BigDecBytes {
            unscaled: unscaled.clone(),
            scale,
        };
        if let Some(&handle) = self.dedup.get(&repr) {
            return handle;
        }
        let handle = self.values.len() as u32;
        self.values.push(StoredBigValue::BigDec { unscaled, scale });
        self.dedup.insert(repr, handle);
        handle
    }

    /// Look up a stored value by handle.
    pub fn get_by_handle(&self, handle: u32) -> Option<&StoredBigValue> {
        self.values.get(handle as usize)
    }

    /// Find a BigInt's handle without inserting (read-only lookup for query path).
    pub fn find_bigint(&self, bi: &BigInt) -> Option<u32> {
        let bytes = bi.to_signed_bytes_le();
        let repr = NumBigRepr::BigIntBytes(bytes);
        self.dedup.get(&repr).copied()
    }

    /// Find a BigDecimal's handle without inserting (read-only lookup for query path).
    pub fn find_bigdec(&self, bd: &BigDecimal) -> Option<u32> {
        let (unscaled_bi, scale) = bd.as_bigint_and_exponent();
        let unscaled = unscaled_bi.to_signed_bytes_le();
        let repr = NumBigRepr::BigDecBytes { unscaled, scale };
        self.dedup.get(&repr).copied()
    }

    /// Number of entries.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// All stored values (for persistence, in handle order).
    pub fn values(&self) -> &[StoredBigValue] {
        &self.values
    }
}

impl Default for NumBigArena {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// NBA1 binary persistence
// ============================================================================

/// Magic bytes for a numbig arena file.
const NBA_MAGIC: [u8; 4] = *b"NBA1";

const REPR_TAG_BIGINT: u8 = 0;
const REPR_TAG_BIGDEC: u8 = 1;

/// Serialize a NumBigArena to an in-memory NBA1 byte buffer.
///
/// Format:
/// ```text
/// magic: "NBA1" (4B)
/// entry_count: u32 LE
/// Per entry (in handle order):
///   repr_tag: u8
///   repr_len: u32 LE
///   repr_bytes: [u8; repr_len]
/// ```
pub fn write_numbig_arena_to_bytes(arena: &NumBigArena) -> io::Result<Vec<u8>> {
    use std::io::Write;

    let mut buf = Vec::new();
    buf.write_all(&NBA_MAGIC)?;
    buf.write_all(&(arena.values().len() as u32).to_le_bytes())?;

    for entry in arena.values() {
        match entry {
            StoredBigValue::BigInt(bytes) => {
                buf.write_all(&[REPR_TAG_BIGINT])?;
                buf.write_all(&(bytes.len() as u32).to_le_bytes())?;
                buf.write_all(bytes)?;
            }
            StoredBigValue::BigDec { unscaled, scale } => {
                buf.write_all(&[REPR_TAG_BIGDEC])?;
                let repr_len = 4 + unscaled.len() + 8;
                buf.write_all(&(repr_len as u32).to_le_bytes())?;
                buf.write_all(&(unscaled.len() as u32).to_le_bytes())?;
                buf.write_all(unscaled)?;
                buf.write_all(&scale.to_le_bytes())?;
            }
        }
    }

    Ok(buf)
}

/// Write a NumBigArena to a binary NBA1 file.
pub fn write_numbig_arena(path: &std::path::Path, arena: &NumBigArena) -> io::Result<()> {
    let bytes = write_numbig_arena_to_bytes(arena)?;
    std::fs::write(path, &bytes)
}

/// Parse a NumBigArena from a byte buffer (NBA1 format).
///
/// Entries are inserted in file order, so handles are preserved
/// (entry 0 = handle 0, etc.).
pub fn read_numbig_arena_from_bytes(data: &[u8]) -> io::Result<NumBigArena> {
    if data.len() < 8 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "numbig arena too small",
        ));
    }
    if data[0..4] != NBA_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("numbig arena: invalid magic {:?}", &data[0..4]),
        ));
    }

    let count = u32::from_le_bytes(data[4..8].try_into().unwrap()) as usize;
    let mut arena = NumBigArena::new();
    let mut pos = 8;

    for _ in 0..count {
        if pos + 1 > data.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "numbig entry truncated (tag)",
            ));
        }
        let tag = data[pos];
        pos += 1;

        if pos + 4 > data.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "numbig entry truncated (len)",
            ));
        }
        let repr_len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;

        if pos + repr_len > data.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "numbig entry truncated (data)",
            ));
        }
        let repr_bytes = &data[pos..pos + repr_len];
        pos += repr_len;

        match tag {
            REPR_TAG_BIGINT => {
                let bi = BigInt::from_signed_bytes_le(repr_bytes);
                arena.get_or_insert_bigint(&bi);
            }
            REPR_TAG_BIGDEC => {
                if repr_bytes.len() < 12 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "bigdec repr too short",
                    ));
                }
                let unscaled_len =
                    u32::from_le_bytes(repr_bytes[0..4].try_into().unwrap()) as usize;
                if 4 + unscaled_len + 8 != repr_bytes.len() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "bigdec repr size mismatch: unscaled_len={}, total={}",
                            unscaled_len,
                            repr_bytes.len()
                        ),
                    ));
                }
                let unscaled_bytes = &repr_bytes[4..4 + unscaled_len];
                let scale_bytes = &repr_bytes[4 + unscaled_len..];
                let unscaled = BigInt::from_signed_bytes_le(unscaled_bytes);
                let scale = i64::from_le_bytes(scale_bytes.try_into().unwrap());
                let bd = BigDecimal::new(unscaled, scale);
                arena.get_or_insert_bigdec(&bd);
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("unknown numbig tag: {tag}"),
                ));
            }
        }
    }

    Ok(arena)
}

/// Read a NumBigArena from a binary NBA1 file.
///
/// Entries are inserted in file order, so handles are preserved
/// (entry 0 = handle 0, etc.).
pub fn read_numbig_arena(path: &std::path::Path) -> io::Result<NumBigArena> {
    read_numbig_arena_from_bytes(&std::fs::read(path)?)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_arena() {
        let arena = NumBigArena::new();
        assert_eq!(arena.len(), 0);
        assert!(arena.is_empty());
        assert!(arena.get_by_handle(0).is_none());
    }

    #[test]
    fn test_bigint_insert_and_dedup() {
        let mut arena = NumBigArena::new();
        let bi = BigInt::from(999_999_999_999_999_999i64) * BigInt::from(1000i64);
        let h1 = arena.get_or_insert_bigint(&bi);
        let h2 = arena.get_or_insert_bigint(&bi);
        assert_eq!(h1, 0);
        assert_eq!(h1, h2);
        assert_eq!(arena.len(), 1);
    }

    #[test]
    fn test_bigdec_insert_and_dedup() {
        let mut arena = NumBigArena::new();
        let bd: BigDecimal = "123456789.123456789".parse().unwrap();
        let h1 = arena.get_or_insert_bigdec(&bd);
        let h2 = arena.get_or_insert_bigdec(&bd);
        assert_eq!(h1, 0);
        assert_eq!(h1, h2);
        assert_eq!(arena.len(), 1);
    }

    #[test]
    fn test_sequential_handles() {
        let mut arena = NumBigArena::new();
        let bi1 = BigInt::from(i64::MAX) + BigInt::from(1);
        let bi2 = BigInt::from(i64::MIN) - BigInt::from(1);
        let bd1: BigDecimal = "1.23".parse().unwrap();

        let h1 = arena.get_or_insert_bigint(&bi1);
        let h2 = arena.get_or_insert_bigint(&bi2);
        let h3 = arena.get_or_insert_bigdec(&bd1);

        assert_eq!(h1, 0);
        assert_eq!(h2, 1);
        assert_eq!(h3, 2);
        assert_eq!(arena.len(), 3);
    }

    #[test]
    fn test_forward_lookup() {
        let mut arena = NumBigArena::new();
        let bi = BigInt::from(i64::MAX) + BigInt::from(100);
        let handle = arena.get_or_insert_bigint(&bi);

        let stored = arena.get_by_handle(handle).unwrap();
        let val = stored.to_flake_value();
        match val {
            fluree_db_core::value::FlakeValue::BigInt(v) => {
                assert_eq!(*v, bi);
            }
            _ => panic!("expected BigInt"),
        }
    }

    #[test]
    fn test_bigdec_forward_lookup() {
        let mut arena = NumBigArena::new();
        let bd: BigDecimal = "123456789.987654321".parse().unwrap();
        let handle = arena.get_or_insert_bigdec(&bd);

        let stored = arena.get_by_handle(handle).unwrap();
        let val = stored.to_flake_value();
        match val {
            fluree_db_core::value::FlakeValue::Decimal(v) => {
                assert_eq!(*v, bd);
            }
            _ => panic!("expected Decimal"),
        }
    }

    #[test]
    fn test_nba1_round_trip() {
        let mut arena = NumBigArena::new();
        let bi = BigInt::from(i64::MAX) + BigInt::from(42);
        let bd: BigDecimal = "3.14159265358979323846".parse().unwrap();
        arena.get_or_insert_bigint(&bi);
        arena.get_or_insert_bigdec(&bd);

        let dir = std::env::temp_dir().join("fluree_nba1_test");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("test.nba");

        write_numbig_arena(&path, &arena).unwrap();
        let loaded = read_numbig_arena(&path).unwrap();

        assert_eq!(loaded.len(), arena.len());

        // Verify BigInt round-trip
        let val0 = loaded.get_by_handle(0).unwrap().to_flake_value();
        match val0 {
            fluree_db_core::value::FlakeValue::BigInt(v) => assert_eq!(*v, bi),
            _ => panic!("expected BigInt"),
        }

        // Verify BigDecimal round-trip
        let val1 = loaded.get_by_handle(1).unwrap().to_flake_value();
        match val1 {
            fluree_db_core::value::FlakeValue::Decimal(v) => assert_eq!(*v, bd),
            _ => panic!("expected Decimal"),
        }

        let _ = std::fs::remove_dir_all(&dir);
    }
}
