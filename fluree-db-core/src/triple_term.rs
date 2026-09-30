//! Triple-term dictionary handles and encoded keys.
//!
//! A reification link `_:r rdf:reifies <<( s p o )>>` is one main-index flake
//! whose object is a term **handle** (`OType::TRIPLE_TERM`, `o_key`). The
//! handle is partitioned by the term's inner predicate so that every term
//! under one predicate occupies a contiguous `o_key` interval: a predicate
//! restriction on a reified-triple pattern becomes one `POST` range on
//! `rdf:reifies`. The precedent is `SubjectId`, `(ns_code << 48) | local`.
//!
//! The split is a deliberate constant: 32 bits of sequence per predicate,
//! 32 bits of predicate id. Both limits are enforced at allocation.

use crate::o_type::OType;

/// Bits of per-predicate sequence in a handle.
pub const TERM_SEQ_BITS: u32 = 32;
/// Mask for the sequence part of a handle.
pub const TERM_SEQ_MASK: u64 = (1u64 << TERM_SEQ_BITS) - 1;

/// Compose a handle from the inner predicate id and a per-predicate sequence.
#[inline]
pub const fn term_handle(inner_p_id: u32, seq: u32) -> u64 {
    ((inner_p_id as u64) << TERM_SEQ_BITS) | seq as u64
}

/// The inner predicate id a handle was allocated under.
#[inline]
pub const fn term_handle_p_id(handle: u64) -> u32 {
    (handle >> TERM_SEQ_BITS) as u32
}

/// The per-predicate sequence of a handle.
#[inline]
pub const fn term_handle_seq(handle: u64) -> u32 {
    (handle & TERM_SEQ_MASK) as u32
}

/// Inclusive `o_key` interval holding every term under `inner_p_id`.
#[inline]
pub const fn term_handle_range(inner_p_id: u32) -> (u64, u64) {
    (
        term_handle(inner_p_id, 0),
        term_handle(inner_p_id, u32::MAX),
    )
}

/// The encoded identity of a triple term: the base edge\'s `(s_id, p_id,
/// o_type, o_key)` as the main index stores it. No graph, no list index.
///
/// Its big-endian byte form is the reverse-tree key, ordered subject-first so
/// a subject-bound reified-triple pattern is one key range.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TermKey {
    pub s_id: u64,
    pub p_id: u32,
    pub o_type: OType,
    pub o_key: u64,
}

impl TermKey {
    /// Encoded key width in bytes.
    pub const LEN: usize = 8 + 4 + 2 + 8;

    /// Big-endian key bytes: `s_id, p_id, o_type, o_key`.
    #[inline]
    pub fn to_be_bytes(&self) -> [u8; Self::LEN] {
        let mut b = [0u8; Self::LEN];
        b[0..8].copy_from_slice(&self.s_id.to_be_bytes());
        b[8..12].copy_from_slice(&self.p_id.to_be_bytes());
        b[12..14].copy_from_slice(&self.o_type.as_u16().to_be_bytes());
        b[14..22].copy_from_slice(&self.o_key.to_be_bytes());
        b
    }

    /// Decode a key written by [`Self::to_be_bytes`]. `None` on a wrong width.
    #[inline]
    pub fn from_be_bytes(b: &[u8]) -> Option<Self> {
        if b.len() != Self::LEN {
            return None;
        }
        Some(Self {
            s_id: u64::from_be_bytes(b[0..8].try_into().ok()?),
            p_id: u32::from_be_bytes(b[8..12].try_into().ok()?),
            o_type: OType::from_u16(u16::from_be_bytes(b[12..14].try_into().ok()?)),
            o_key: u64::from_be_bytes(b[14..22].try_into().ok()?),
        })
    }

    /// Key prefix shared by every term with this subject: the first 8 bytes.
    #[inline]
    pub fn subject_prefix(s_id: u64) -> [u8; 8] {
        s_id.to_be_bytes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handle_roundtrip_and_range() {
        let h = term_handle(7, 42);
        assert_eq!(term_handle_p_id(h), 7);
        assert_eq!(term_handle_seq(h), 42);
        let (lo, hi) = term_handle_range(7);
        assert!(lo <= h && h <= hi);
        assert!(term_handle(8, 0) > hi);
        assert!(term_handle(6, u32::MAX) < lo);
    }

    #[test]
    fn key_roundtrip_orders_subject_first() {
        let k = TermKey {
            s_id: 0x0001_0000_0000_0005,
            p_id: 9,
            o_type: OType::IRI_REF,
            o_key: 77,
        };
        let b = k.to_be_bytes();
        assert_eq!(TermKey::from_be_bytes(&b), Some(k));
        let k2 = TermKey {
            s_id: k.s_id + 1,
            p_id: 0,
            ..k
        };
        assert!(k2.to_be_bytes() > b);
        assert!(TermKey::from_be_bytes(&b[..10]).is_none());
    }
}
