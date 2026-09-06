//! HyperLogLog with the register count fixed at compile time.
//!
//! One implementation serves two very different callers. The indexer keeps
//! a [`Hll256`] per (graph, property) and feeds it every flake of every
//! reindex, so its hot path is a fixed-size array, a constant shift and a
//! register compare: nothing the compiler cannot fold. Profiling runs on
//! demand and can afford [`Hll4096`], sixteen times the registers and a
//! quarter of the error, enough to say whether a column is a key.
//!
//! Registers hold the maximum rank observed for their bucket: the top `p`
//! bits of a hash choose the register, the leading zeros of the rest give
//! the rank. Merge is a register-wise maximum, so the estimate of a merge
//! is the estimate of the union and never decreases.

use serde::{Deserialize, Serialize};

/// Registers in the profiling default: precision 12, ~1.6% error.
pub const DEFAULT_REGISTERS: usize = 4096;

/// The indexer's sketch: 256 registers, precision 8, ~6.5% error, and a
/// persisted form of exactly 256 bytes.
pub type Hll256 = Hll<256>;
/// The profiling sketch.
pub type Hll4096 = Hll<DEFAULT_REGISTERS>;

/// A HyperLogLog cardinality sketch with `M` registers, `M` a power of two
/// between 16 and 65,536.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Hll<const M: usize = DEFAULT_REGISTERS> {
    registers: [u8; M],
}

impl<const M: usize> Default for Hll<M> {
    fn default() -> Self {
        Self::new()
    }
}

const fn alpha(m: usize) -> f64 {
    match m {
        16 => 0.673,
        32 => 0.697,
        64 => 0.709,
        _ => 0.7213 / (1.0 + 1.079 / m as f64),
    }
}

impl<const M: usize> Hll<M> {
    /// Precision `p`, with `M == 2^p`.
    pub const PRECISION: u32 = {
        assert!(
            M.is_power_of_two() && M >= 16 && M <= 65_536,
            "HLL register count must be a power of two between 16 and 65536"
        );
        M.trailing_zeros()
    };
    const REMAINING_BITS: u32 = 64 - Self::PRECISION;
    const ALPHA: f64 = alpha(M);

    /// A new empty sketch.
    pub const fn new() -> Self {
        Self {
            registers: [0u8; M],
        }
    }

    /// Rebuild a sketch from raw registers.
    pub const fn from_registers(registers: [u8; M]) -> Self {
        Self { registers }
    }

    /// Rebuild from raw register bytes; `None` unless exactly `M` bytes.
    pub fn from_slice(bytes: &[u8]) -> Option<Self> {
        let registers: [u8; M] = bytes.try_into().ok()?;
        Some(Self { registers })
    }

    /// The precision `p` as a byte.
    pub const fn precision(&self) -> u8 {
        Self::PRECISION as u8
    }

    /// The raw registers, for persistence.
    pub const fn registers(&self) -> &[u8; M] {
        &self.registers
    }

    /// The raw registers by value: the persisted form, no header.
    pub const fn to_bytes(&self) -> [u8; M] {
        self.registers
    }

    /// Rebuild from the persisted form.
    pub const fn from_bytes(bytes: &[u8; M]) -> Self {
        Self { registers: *bytes }
    }

    /// Persisted form with a one-byte version header.
    pub fn to_bytes_versioned(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(M + 1);
        bytes.push(1);
        bytes.extend_from_slice(&self.registers);
        bytes
    }

    /// Rebuild from the versioned form; `None` on a wrong length or an
    /// unsupported version.
    pub fn from_bytes_versioned(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != M + 1 || bytes[0] != 1 {
            return None;
        }
        Self::from_slice(&bytes[1..])
    }

    /// Insert a 64-bit hash.
    #[inline]
    pub fn insert_hash(&mut self, hash: u64) {
        let index = (hash >> Self::REMAINING_BITS) as usize;
        let w = hash << Self::PRECISION;
        let rho = if w == 0 {
            (Self::REMAINING_BITS + 1) as u8
        } else {
            (w.leading_zeros() + 1) as u8
        };
        if rho > self.registers[index] {
            self.registers[index] = rho;
        }
    }

    /// Register-wise maximum: the sketch of the union.
    pub fn merge(&mut self, other: &Self) {
        for (mine, theirs) in self.registers.iter_mut().zip(&other.registers) {
            if *theirs > *mine {
                *mine = *theirs;
            }
        }
    }

    /// Estimated number of distinct hashes inserted, with linear counting
    /// below `2.5 M`.
    ///
    /// There is no large-range correction. The original paper's
    /// `-2^32 ln(1 - E/2^32)` term compensates for 32-bit hash
    /// collisions; every hash here is 64 bits, so the raw estimator holds
    /// all the way up. Applied to 64-bit hashes that term read 2.3% high
    /// at ~200 M and, once `E` passed `2^32`, took the log of a negative
    /// number and reported zero.
    pub fn estimate(&self) -> u64 {
        let m = M as f64;
        let mut sum = 0.0f64;
        let mut zeros = 0u32;
        for &reg in &self.registers {
            sum += 2.0f64.powi(-i32::from(reg));
            if reg == 0 {
                zeros += 1;
            }
        }
        let raw = Self::ALPHA * m * m / sum;
        if raw <= 2.5 * m && zeros > 0 {
            return (m * (m / f64::from(zeros)).ln()).round() as u64;
        }
        raw.round() as u64
    }

    /// Typical relative standard error of this register count,
    /// `1.04 / sqrt(M)`; a property of the type, so a caller can quote
    /// it without holding a sketch.
    pub fn typical_error() -> f64 {
        1.04 / (M as f64).sqrt()
    }

    /// Typical relative standard error, `1.04 / sqrt(M)`.
    pub fn relative_error(&self) -> f64 {
        Self::typical_error()
    }

    /// Whether nothing has been inserted.
    pub fn is_empty(&self) -> bool {
        self.registers.iter().all(|&r| r == 0)
    }
}

/// Wire form: precision plus hex registers, so a profile serialises to
/// readable JSON without a four-thousand-element integer array.
#[derive(Serialize, Deserialize)]
struct HllWire {
    p: u8,
    registers: String,
}

impl<const M: usize> Serialize for Hll<M> {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        HllWire {
            p: self.precision(),
            registers: hex::encode(self.registers),
        }
        .serialize(s)
    }
}

impl<'de, const M: usize> Deserialize<'de> for Hll<M> {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let wire = HllWire::deserialize(d)?;
        if u32::from(wire.p) != Self::PRECISION {
            return Err(serde::de::Error::custom(format!(
                "HLL precision {} does not match the expected {}",
                wire.p,
                Self::PRECISION
            )));
        }
        let bytes = hex::decode(&wire.registers).map_err(serde::de::Error::custom)?;
        Self::from_slice(&bytes)
            .ok_or_else(|| serde::de::Error::custom("HLL register count does not match precision"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use xxhash_rust::xxh3::xxh3_64;

    fn filled<const M: usize>(n: u64) -> Hll<M> {
        let mut h = Hll::<M>::new();
        for i in 0..n {
            h.insert_hash(xxh3_64(&i.to_le_bytes()));
        }
        h
    }

    #[test]
    fn precision_follows_register_count() {
        assert_eq!(Hll256::PRECISION, 8);
        assert_eq!(Hll4096::PRECISION, 12);
        assert_eq!(Hll::<65_536>::PRECISION, 16);
        assert_eq!(Hll256::new().precision(), 8);
    }

    #[test]
    fn empty_estimates_zero() {
        assert_eq!(Hll::<4096>::new().estimate(), 0);
        assert!(Hll256::new().is_empty());
    }

    #[test]
    fn one_hash_is_one() {
        let mut h = Hll256::new();
        h.insert_hash(0x1234_5678_9abc_def0);
        assert_eq!(h.estimate(), 1);
        for _ in 0..1000 {
            h.insert_hash(0x1234_5678_9abc_def0);
        }
        assert_eq!(h.estimate(), 1, "duplicates do not count");
    }

    #[test]
    fn small_counts_are_near_exact() {
        for n in [5u64, 50, 500] {
            let est = filled::<4096>(n).estimate();
            let err = (est as f64 - n as f64).abs() / n as f64;
            assert!(err < 0.05, "n={n} est={est}");
        }
    }

    #[test]
    fn large_counts_within_error_bound_at_both_sizes() {
        let n = 200_000u64;
        let h = filled::<4096>(n);
        let err = (h.estimate() as f64 - n as f64).abs() / n as f64;
        assert!(err < 3.0 * h.relative_error(), "4096: {err}");
        let h = filled::<256>(n);
        let err = (h.estimate() as f64 - n as f64).abs() / n as f64;
        assert!(err < 3.0 * h.relative_error(), "256: {err}");
    }

    #[test]
    fn billions_of_distinct_hashes_do_not_estimate_zero() {
        // Every register at the same rank makes the raw estimate exactly
        // `alpha * M * 2^rank`. Rank 21 at 4096 registers is ~6.2 billion,
        // past 2^32; the 32-bit large-range correction took `ln` of a
        // negative number there and rounded NaN to zero. Rank 16 is
        // ~194 million, where the same correction read 2.3% high.
        let raw = |m: usize, rank: u8| alpha(m) * m as f64 * 2f64.powi(i32::from(rank));
        let h = Hll::<4096>::from_registers([21u8; 4096]);
        assert!(h.estimate() > 6_000_000_000, "{}", h.estimate());
        assert_eq!(h.estimate(), raw(4096, 21).round() as u64);
        let h = Hll::<4096>::from_registers([16u8; 4096]);
        assert_eq!(h.estimate(), raw(4096, 16).round() as u64);
        let h = Hll256::from_registers([25u8; 256]);
        assert_eq!(h.estimate(), raw(256, 25).round() as u64);
    }

    #[test]
    fn merge_is_union_and_monotone() {
        let mut a = filled::<4096>(10_000);
        let before = a.estimate();
        let mut b = Hll::<4096>::new();
        for i in 5_000u64..15_000 {
            b.insert_hash(xxh3_64(&i.to_le_bytes()));
        }
        a.merge(&b);
        let est = a.estimate();
        assert!(est >= before);
        let err = (est as f64 - 15_000.0).abs() / 15_000.0;
        assert!(err < 0.05, "est={est}");
    }

    #[test]
    fn register_layout_is_top_bits_index_then_leading_zeros() {
        let mut h = Hll256::new();
        let hash = 0x0F00_0000_0000_0001u64;
        h.insert_hash(hash);
        assert_eq!(h.registers()[0x0F], 1 + (hash << 8).leading_zeros() as u8);
        let mut h = Hll4096::new();
        h.insert_hash(hash);
        assert_eq!(h.registers()[0x0F0], 1 + (hash << 12).leading_zeros() as u8);
    }

    #[test]
    fn byte_round_trips() {
        let h = filled::<256>(1_000);
        assert_eq!(Hll256::from_bytes(&h.to_bytes()), h);
        assert_eq!(Hll256::from_slice(&h.to_bytes()).unwrap(), h);
        assert_eq!(
            Hll256::from_bytes_versioned(&h.to_bytes_versioned()).unwrap(),
            h
        );
        let mut bad = h.to_bytes_versioned();
        assert!(Hll256::from_bytes_versioned(&bad[..100]).is_none());
        bad[0] = 2;
        assert!(Hll256::from_bytes_versioned(&bad).is_none());
        assert!(Hll256::from_slice(&[0u8; 255]).is_none());
    }

    #[test]
    fn json_round_trip_and_precision_check() {
        let h = filled::<4096>(1_000);
        let json = serde_json::to_string(&h).unwrap();
        let back: Hll4096 = serde_json::from_str(&json).unwrap();
        assert_eq!(h, back);
        assert!(serde_json::from_str::<Hll256>(&json).is_err());
    }
}
