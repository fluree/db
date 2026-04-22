//! Domain-separated hashing for HLL registers.

use xxhash_rust::xxh64::xxh64;

/// Domain separator for object value hashing.
const OBJ_HASH_DOMAIN: &[u8] = b"fluree:obj:";

/// Domain separator for subject HLL hashing.
const SUBJ_HASH_DOMAIN: &[u8] = b"fluree:subj:";

/// Compute a stable, endian-invariant hash of an object value.
///
/// Domain-separated by `o_kind` to prevent cross-kind collisions
/// (e.g., `NumInt(3)` vs `RefId(3)` both have `o_key=3`).
pub fn value_hash(o_kind: u8, o_key: u64) -> u64 {
    // domain(11) + kind(1) + key(8) = 20 bytes
    let mut buf = [0u8; 20];
    buf[..11].copy_from_slice(OBJ_HASH_DOMAIN);
    buf[11] = o_kind;
    buf[12..20].copy_from_slice(&o_key.to_le_bytes());
    xxh64(&buf, 0)
}

/// V2-compatible value hash using `o_type` (u16) instead of `o_kind` (u8).
///
/// Domain-separated by `o_type` to prevent cross-type collisions.
/// Compatible with V1 hashing in the sense that HLL sketches built from
/// V2 records can be merged with those from V1 records as long as
/// the full rebuild produces a consistent sketch.
pub fn value_hash_v2(o_type: u16, o_key: u64) -> u64 {
    // domain(11) + type(2) + key(8) = 21 bytes
    let mut buf = [0u8; 21];
    buf[..11].copy_from_slice(OBJ_HASH_DOMAIN);
    buf[11..13].copy_from_slice(&o_type.to_le_bytes());
    buf[13..21].copy_from_slice(&o_key.to_le_bytes());
    xxh64(&buf, 0)
}

/// Compute a stable hash of a subject ID for HLL insertion.
///
/// Hashes `s_id` rather than using it directly to ensure uniform bit
/// distribution across HLL registers.
pub fn subject_hash(s_id: u64) -> u64 {
    // domain(12) + s_id(8) = 20 bytes
    let mut buf = [0u8; 20];
    buf[..12].copy_from_slice(SUBJ_HASH_DOMAIN);
    buf[12..20].copy_from_slice(&s_id.to_le_bytes());
    xxh64(&buf, 0)
}
