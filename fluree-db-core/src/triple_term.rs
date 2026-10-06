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

use crate::o_type::{DecodeKind, OType};
use crate::value::FlakeValue;

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

/// First per-predicate sequence of a provisional handle: one dictionary
/// novelty assigns to a term the index has not interned. Indexed sequences
/// stay below it, so a provisional handle still falls in its predicate's
/// interval and never collides with an indexed one.
pub const NOVELTY_TERM_SEQ_BASE: u32 = 1 << 31;

/// The provisional handle for dictionary-novelty term `index` under
/// `inner_p_id`.
#[inline]
pub const fn novelty_term_handle(inner_p_id: u32, index: u32) -> u64 {
    term_handle(inner_p_id, NOVELTY_TERM_SEQ_BASE | index)
}

/// The dictionary-novelty index of a provisional handle; `None` for an
/// indexed one.
#[inline]
pub const fn novelty_term_index(handle: u64) -> Option<u32> {
    let seq = term_handle_seq(handle);
    if seq >= NOVELTY_TERM_SEQ_BASE {
        Some(seq - NOVELTY_TERM_SEQ_BASE)
    } else {
        None
    }
}

/// Inclusive `o_key` interval holding every term under `inner_p_id`.
#[inline]
pub const fn term_handle_range(inner_p_id: u32) -> (u64, u64) {
    (
        term_handle(inner_p_id, 0),
        term_handle(inner_p_id, u32::MAX),
    )
}

/// Whether a term key's object is keyed by [`lexical_term_object`]: an `o_key`
/// that is a string-dictionary id, where the main index holds an arena handle.
#[inline]
pub const fn is_lexical_term_object(o_type: OType) -> bool {
    matches!(
        o_type.decode_kind(),
        DecodeKind::NumBigArena | DecodeKind::VectorArena
    )
}

/// The `o_type` and canonical form that key a decimal, big-integer or vector
/// object in a term. The main index keys these by arena handles scoped to a
/// graph and predicate, which name nothing across graphs, so a term keys them
/// by the string-dictionary id of this form instead. `None` for every other
/// object, and for an integer that fits `i64` (keyed inline).
///
/// Equal values share a form: a decimal is normalized as the main index's
/// arena normalizes it, and a vector is read at the `f32` precision ingest
/// quantizes it to. A decimal's form always carries an exponent and a big
/// integer's never does, so the two cannot collide under one `o_type`.
pub fn lexical_term_object(o: &FlakeValue) -> Option<(OType, String)> {
    match o {
        FlakeValue::Decimal(d) => {
            let (unscaled, scale) = d.normalized().as_bigint_and_exponent();
            Some((
                OType::NUM_BIG_OVERFLOW,
                format!("{unscaled}e{}", -i128::from(scale)),
            ))
        }
        FlakeValue::BigInt(b) if num_traits::ToPrimitive::to_i64(b.as_ref()).is_none() => {
            Some((OType::NUM_BIG_OVERFLOW, b.to_string()))
        }
        FlakeValue::Vector(v) => {
            let elements: Vec<String> = v
                .iter()
                // -0.0 equals 0.0 as a vector element; one form for both.
                .map(|x| (*x as f32 + 0.0).to_string())
                .collect();
            Some((OType::VECTOR, format!("[{}]", elements.join(","))))
        }
        _ => None,
    }
}

/// The object a [`lexical_term_object`] form names; `None` for a malformed
/// form or an `o_type` that is not keyed that way.
pub fn parse_lexical_term_object(o_type: OType, form: &str) -> Option<FlakeValue> {
    match o_type.decode_kind() {
        DecodeKind::NumBigArena => match form.split_once('e') {
            Some((unscaled, exp)) => {
                let unscaled: num_bigint::BigInt = unscaled.parse().ok()?;
                let exp: i128 = exp.parse().ok()?;
                Some(FlakeValue::Decimal(Box::new(bigdecimal::BigDecimal::new(
                    unscaled,
                    i64::try_from(-exp).ok()?,
                ))))
            }
            None => Some(FlakeValue::BigInt(Box::new(form.parse().ok()?))),
        },
        DecodeKind::VectorArena => {
            let inner = form.strip_prefix('[')?.strip_suffix(']')?;
            let elements = if inner.is_empty() {
                Vec::new()
            } else {
                inner
                    .split(',')
                    .map(|x| x.parse::<f32>().ok().map(f64::from))
                    .collect::<Option<Vec<f64>>>()?
            };
            Some(FlakeValue::Vector(elements.into()))
        }
        _ => None,
    }
}

/// The encoded identity of a triple term: the base edge\'s `(s_id, p_id,
/// o_type, o_key)` as the main index stores it, except that a decimal,
/// big-integer or vector object is keyed by [`lexical_term_object`]. No graph,
/// no list index.
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

    /// Big-endian object-first key bytes: `o_type, o_key, p_id, s_id`, the
    /// object reverse tree's order, so an object-bound reified-triple
    /// pattern is one key range.
    #[inline]
    pub fn to_object_first_bytes(&self) -> [u8; Self::LEN] {
        let mut b = [0u8; Self::LEN];
        b[0..2].copy_from_slice(&self.o_type.as_u16().to_be_bytes());
        b[2..10].copy_from_slice(&self.o_key.to_be_bytes());
        b[10..14].copy_from_slice(&self.p_id.to_be_bytes());
        b[14..22].copy_from_slice(&self.s_id.to_be_bytes());
        b
    }

    /// Decode a key written by [`Self::to_object_first_bytes`].
    #[inline]
    pub fn from_object_first_bytes(b: &[u8]) -> Option<Self> {
        if b.len() != Self::LEN {
            return None;
        }
        Some(Self {
            o_type: OType::from_u16(u16::from_be_bytes(b[0..2].try_into().ok()?)),
            o_key: u64::from_be_bytes(b[2..10].try_into().ok()?),
            p_id: u32::from_be_bytes(b[10..14].try_into().ok()?),
            s_id: u64::from_be_bytes(b[14..22].try_into().ok()?),
        })
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
    fn provisional_handles_stay_in_their_predicate_interval() {
        let h = novelty_term_handle(7, 3);
        let (lo, hi) = term_handle_range(7);
        assert!(lo <= h && h <= hi);
        assert_eq!(term_handle_p_id(h), 7);
        assert_eq!(novelty_term_index(h), Some(3));
        assert_eq!(
            novelty_term_index(term_handle(7, NOVELTY_TERM_SEQ_BASE - 1)),
            None
        );
    }

    #[test]
    fn lexical_forms_identify_values_and_round_trip() {
        let decimal = |s: &str| FlakeValue::Decimal(Box::new(s.parse().unwrap()));
        let form = |o: &FlakeValue| lexical_term_object(o).expect("arena kind").1;

        assert_eq!(form(&decimal("1.5")), form(&decimal("1.50")));
        assert_ne!(form(&decimal("1.5")), form(&decimal("15")));
        let big: num_bigint::BigInt = "123456789012345678901234567890".parse().unwrap();
        let big = FlakeValue::BigInt(Box::new(big));
        let integral = decimal("123456789012345678901234567890");
        assert_ne!(
            form(&big),
            form(&integral),
            "decimal and integer stay apart"
        );
        assert!(lexical_term_object(&FlakeValue::BigInt(Box::new(7.into()))).is_none());
        assert!(lexical_term_object(&FlakeValue::Long(7)).is_none());

        let vector = |v: &[f64]| FlakeValue::Vector(v.to_vec().into());
        assert_eq!(form(&vector(&[-0.0, 1.5])), form(&vector(&[0.0, 1.5])));

        for o in [
            decimal("1.50"),
            decimal("-0.000123"),
            decimal("1E+30"),
            decimal("0"),
            big,
            vector(&[0.1f32 as f64, -2.5, 1e-30f32 as f64]),
        ] {
            let (o_type, s) = lexical_term_object(&o).unwrap();
            assert!(is_lexical_term_object(o_type));
            let back = parse_lexical_term_object(o_type, &s).unwrap();
            assert_eq!(back, o, "{s}");
            assert_eq!(std::mem::discriminant(&back), std::mem::discriminant(&o));
            assert_eq!(lexical_term_object(&back).unwrap().1, s);
        }
        assert!(!is_lexical_term_object(OType::XSD_STRING));
        assert!(parse_lexical_term_object(OType::XSD_STRING, "1").is_none());
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

        let o = k.to_object_first_bytes();
        assert_eq!(TermKey::from_object_first_bytes(&o), Some(k));
        let other_object = TermKey {
            o_key: 78,
            s_id: 0,
            ..k
        };
        assert!(other_object.to_object_first_bytes() > o, "object first");
        assert!(TermKey::from_object_first_bytes(&o[..10]).is_none());
    }
}
