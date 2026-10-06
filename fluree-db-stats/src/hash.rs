//! Value hashing for the sketches.
//!
//! Every sketch keys on a 64-bit hash, so what "the same value" means is
//! decided here, once. Two rules:
//!
//! - **Domain separation.** A string `"7"`, the integer `7`, and an IRI
//!   `<7>` are different values. Each kind hashes under its own seed so
//!   they never collide by construction.
//! - **Numeric canonicalisation.** `7`, `7.0` and the decimal `7.00` are
//!   the same value. Any float that is integral and fits an `i64` hashes
//!   as that integer; every other float hashes on its bit pattern with
//!   `-0.0` folded into `0.0`. Distinct-count and top-value questions over
//!   a column that mixes integer and double literals then answer the way
//!   a person would.

use xxhash_rust::xxh3::xxh3_64_with_seed;

use crate::profile::ProfileValue;

const SEED_BOOL: u64 = 0x1;
const SEED_INT: u64 = 0x2;
const SEED_FLOAT: u64 = 0x3;
const SEED_STR: u64 = 0x4;
const SEED_REF: u64 = 0x5;
const SEED_TEMPORAL: u64 = 0x6;
const SEED_OTHER: u64 = 0x7;
const SEED_BYTES: u64 = 0x8;

/// Hash a value under its kind's domain. Nulls have no hash; callers
/// count them separately and never call this for them.
#[inline]
pub fn value_hash(value: &ProfileValue<'_>) -> Option<u64> {
    Some(match value {
        ProfileValue::Null => return None,
        ProfileValue::Bool(b) => xxh3_64_with_seed(&[u8::from(*b)], SEED_BOOL),
        ProfileValue::Int(i) => int_hash(*i),
        ProfileValue::Float(f) => float_hash(*f),
        ProfileValue::Str(s) => xxh3_64_with_seed(s.as_bytes(), SEED_STR),
        ProfileValue::Ref(s) => xxh3_64_with_seed(s.as_bytes(), SEED_REF),
        ProfileValue::Bytes(b) => xxh3_64_with_seed(b, SEED_BYTES),
        ProfileValue::Temporal(t) => xxh3_64_with_seed(&t.to_le_bytes(), SEED_TEMPORAL),
        ProfileValue::Other(s) => xxh3_64_with_seed(s.as_bytes(), SEED_OTHER),
    })
}

#[inline]
fn int_hash(i: i64) -> u64 {
    xxh3_64_with_seed(&i.to_le_bytes(), SEED_INT)
}

#[inline]
fn float_hash(f: f64) -> u64 {
    // Integral floats within i64 range are the same value as the integer.
    // `i64::MAX as f64` rounds up to 2^63, so compare strictly below it.
    if f.is_finite() && f.fract() == 0.0 && f.abs() < 9_223_372_036_854_775_808.0 {
        return int_hash(f as i64);
    }
    let bits = if f == 0.0 {
        0.0f64.to_bits()
    } else {
        f.to_bits()
    };
    xxh3_64_with_seed(&bits.to_le_bytes(), SEED_FLOAT)
}

/// Hash a group key. Group keys are opaque strings assembled by the
/// caller (one or more property values joined); they live in their own
/// domain so a key never collides with a column value.
#[inline]
pub fn group_hash(key: &str) -> u64 {
    xxh3_64_with_seed(key.as_bytes(), 0x9_0000)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integral_float_hashes_as_the_integer() {
        assert_eq!(
            value_hash(&ProfileValue::Int(7)),
            value_hash(&ProfileValue::Float(7.0))
        );
        assert_ne!(
            value_hash(&ProfileValue::Int(7)),
            value_hash(&ProfileValue::Float(7.5))
        );
    }

    #[test]
    fn kinds_are_domain_separated() {
        let s = value_hash(&ProfileValue::Str("7"));
        let i = value_hash(&ProfileValue::Int(7));
        let r = value_hash(&ProfileValue::Ref("7"));
        let b = value_hash(&ProfileValue::Bytes(b"7"));
        assert_ne!(s, i);
        assert_ne!(s, r);
        assert_ne!(i, r);
        assert_ne!(s, b);
    }

    #[test]
    fn negative_zero_is_zero() {
        assert_eq!(
            value_hash(&ProfileValue::Float(-0.0)),
            value_hash(&ProfileValue::Float(0.0))
        );
    }

    #[test]
    fn null_has_no_hash() {
        assert_eq!(value_hash(&ProfileValue::Null), None);
    }
}
