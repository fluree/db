//! Minimal HyperLogLog implementation for property statistics
//!
//! A fixed p=8 HLL sketch with 256 registers, designed for:
//! - Direct serialization (just 256 bytes)
//! - Register-wise merge for incremental updates
//! - Monotone NDV estimation (never decreases)
//!
//! This implementation persists raw registers,
//! load and merge during refresh, no hasher generics or serde complexity.

/// HLL precision: p=8 means 2^8 = 256 registers
const HLL_PRECISION: u8 = 8;
/// Number of registers (2^p)
const NUM_REGISTERS: usize = 1 << HLL_PRECISION; // 256
/// Bits used for index extraction
const INDEX_BITS: u32 = HLL_PRECISION as u32;
/// Remaining bits for leading zero count
const REMAINING_BITS: u32 = 64 - INDEX_BITS; // 56

/// Alpha constant for HLL estimation (p=8)
/// alpha_m = 0.7213 / (1 + 1.079/m) where m = 256
const ALPHA_M: f64 = 0.7213 / (1.0 + 1.079 / 256.0);

/// Minimal HLL sketch with 256 registers (p=8)
///
/// Each register stores the maximum observed leading-zero count + 1 (rho value).
/// Registers are u8 since max rho is 56+1=57, well within u8 range.
///
/// # Serialization
///
/// Serializes as exactly 256 bytes (raw registers) via `to_bytes()`/`from_bytes()`.
/// This enables trivial persistence/loading.
/// No serde derive needed - we serialize raw bytes directly.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HllSketch256 {
    /// Version byte for future compatibility (currently always 1)
    version: u8,
    /// 256 registers, each storing max(rho) for that bucket
    registers: [u8; NUM_REGISTERS],
}

impl Default for HllSketch256 {
    fn default() -> Self {
        Self::new()
    }
}

impl HllSketch256 {
    /// Create a new empty sketch (all registers zero)
    pub fn new() -> Self {
        Self {
            version: 1,
            registers: [0u8; NUM_REGISTERS],
        }
    }

    /// Create a sketch from raw register bytes
    ///
    /// Used when loading persisted sketches from storage.
    pub fn from_registers(registers: [u8; NUM_REGISTERS]) -> Self {
        Self {
            version: 1,
            registers,
        }
    }

    /// Insert a pre-computed 64-bit hash value
    ///
    /// This expects the hash to already be computed (e.g., from `FlakeValue::canonical_hash()`).
    /// Extracts the register index from the top p bits and computes rho from the remaining 64-p bits.
    ///
    /// Standard HLL algorithm:
    /// - index = top p bits (here p=8, so top 8 bits)
    /// - w = remaining 64-p bits shifted to MSB position
    /// - rho = leading_zeros(w) + 1 (position of first 1-bit, 1-indexed)
    #[inline]
    pub fn insert_hash(&mut self, hash: u64) {
        // Extract index from top p bits
        let index = (hash >> REMAINING_BITS) as usize;

        // Get remaining 56 bits shifted to MSB position for leading zero count
        let w = hash << INDEX_BITS;

        // Compute rho: position of leftmost 1-bit (1-indexed)
        // If w == 0 (all remaining bits are zero), rho = REMAINING_BITS + 1 = 57
        // Otherwise, rho = leading_zeros(w) + 1
        let rho = if w == 0 {
            (REMAINING_BITS + 1) as u8
        } else {
            (w.leading_zeros() + 1) as u8
        };

        // Update register with max
        if rho > self.registers[index] {
            self.registers[index] = rho;
        }
    }

    /// Merge another sketch into this one (register-wise maximum)
    ///
    /// This is the key operation for incremental stats: merge prior sketch with novelty sketch.
    /// NDV is monotone: merged NDV >= max(self NDV, other NDV).
    pub fn merge_inplace(&mut self, other: &HllSketch256) {
        for i in 0..NUM_REGISTERS {
            if other.registers[i] > self.registers[i] {
                self.registers[i] = other.registers[i];
            }
        }
    }

    /// Estimate the cardinality (number of distinct values)
    ///
    /// Uses the standard HLL estimator with small-range and large-range corrections.
    pub fn estimate(&self) -> u64 {
        let m = NUM_REGISTERS as f64;

        // Compute harmonic mean of 2^(-register[i])
        let mut sum = 0.0f64;
        let mut zeros = 0u32;

        for &reg in &self.registers {
            sum += 2.0f64.powi(-(reg as i32));
            if reg == 0 {
                zeros += 1;
            }
        }

        // Raw HLL estimate
        let raw_estimate = ALPHA_M * m * m / sum;

        // Small range correction (linear counting)
        if raw_estimate <= 2.5 * m && zeros > 0 {
            // Use linear counting for small cardinalities
            return (m * (m / zeros as f64).ln()).round() as u64;
        }

        // Large range correction (for very large cardinalities)
        // 2^32 threshold - not typically needed for p=8, but included for completeness
        let two_pow_32 = 4_294_967_296.0f64;
        if raw_estimate > two_pow_32 / 30.0 {
            return (-two_pow_32 * (1.0 - raw_estimate / two_pow_32).ln()).round() as u64;
        }

        // Normal range - no correction needed
        raw_estimate.round() as u64
    }

    /// Get the raw registers (for persistence)
    pub fn registers(&self) -> &[u8; NUM_REGISTERS] {
        &self.registers
    }

    /// Serialize to raw bytes (256 bytes)
    ///
    /// Format: just the 256 register bytes, no header.
    /// For future compatibility, consider using `to_bytes_versioned()`.
    pub fn to_bytes(&self) -> [u8; NUM_REGISTERS] {
        self.registers
    }

    /// Serialize to bytes with version header (257 bytes)
    ///
    /// Format: [version: u8][registers: [u8; 256]]
    pub fn to_bytes_versioned(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(NUM_REGISTERS + 1);
        bytes.push(self.version);
        bytes.extend_from_slice(&self.registers);
        bytes
    }

    /// Deserialize from raw bytes (256 bytes)
    pub fn from_bytes(bytes: &[u8; NUM_REGISTERS]) -> Self {
        Self::from_registers(*bytes)
    }

    /// Deserialize from versioned bytes (257 bytes)
    ///
    /// Returns None if version is unsupported or bytes are wrong length.
    pub fn from_bytes_versioned(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != NUM_REGISTERS + 1 {
            return None;
        }
        let version = bytes[0];
        if version != 1 {
            return None; // Unsupported version
        }
        let mut registers = [0u8; NUM_REGISTERS];
        registers.copy_from_slice(&bytes[1..]);
        Some(Self { version, registers })
    }

    /// Check if the sketch is empty (no values inserted)
    pub fn is_empty(&self) -> bool {
        self.registers.iter().all(|&r| r == 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_sketch_is_empty() {
        let sketch = HllSketch256::new();
        assert!(sketch.is_empty());
        assert_eq!(sketch.estimate(), 0);
    }

    #[test]
    fn test_insert_single_value() {
        let mut sketch = HllSketch256::new();
        sketch.insert_hash(0x1234_5678_9abc_def0);

        assert!(!sketch.is_empty());
        assert_eq!(sketch.estimate(), 1);
    }

    #[test]
    fn test_insert_multiple_distinct_values() {
        use xxhash_rust::xxh64::xxh64;

        let mut sketch = HllSketch256::new();

        // Insert 1000 distinct values using proper xxHash64 hashing
        // (not just multiplication by a constant, which has poor bit distribution)
        for i in 0..1000u64 {
            let hash = xxh64(&i.to_le_bytes(), 0);
            sketch.insert_hash(hash);
        }

        let estimate = sketch.estimate();
        // HLL at p=8 has ~6.5% standard error (1.04/sqrt(256))
        // For 1000 values: 1000 ± 65 at 1σ, ± 130 at 2σ, ± 195 at 3σ
        // We use 3σ bounds (99.7% confidence): [805, 1195]
        // Add some margin for no bias correction: [700, 1300]
        assert!(
            (700..=1300).contains(&estimate),
            "estimate {estimate} not in expected range [700, 1300] for 1000 values"
        );
    }

    #[test]
    fn test_insert_duplicate_values() {
        let mut sketch = HllSketch256::new();

        // Insert same value 100 times
        for _ in 0..100 {
            sketch.insert_hash(0xdead_beef_cafe_babe);
        }

        // Should still estimate as 1
        assert_eq!(sketch.estimate(), 1);
    }

    #[test]
    fn test_merge_sketches() {
        use xxhash_rust::xxh64::xxh64;

        let mut sketch1 = HllSketch256::new();
        let mut sketch2 = HllSketch256::new();

        // Insert different values into each sketch using proper hashing
        for i in 0..500u64 {
            sketch1.insert_hash(xxh64(&i.to_le_bytes(), 0));
        }
        for i in 500..1000u64 {
            sketch2.insert_hash(xxh64(&i.to_le_bytes(), 0));
        }

        // Merge sketch2 into sketch1
        sketch1.merge_inplace(&sketch2);

        let estimate = sketch1.estimate();
        // Combined 1000 distinct values, expect ~6.5% error
        // Using 3σ bounds with margin: [700, 1300]
        assert!(
            (700..=1300).contains(&estimate),
            "merged estimate {estimate} not in expected range [700, 1300]"
        );
    }

    #[test]
    fn test_merge_is_monotone() {
        use xxhash_rust::xxh64::xxh64;

        let mut sketch1 = HllSketch256::new();
        let mut sketch2 = HllSketch256::new();

        for i in 0..50u64 {
            sketch1.insert_hash(xxh64(&i.to_le_bytes(), 0));
        }
        for i in 0..30u64 {
            sketch2.insert_hash(xxh64(&i.to_le_bytes(), 0));
        }

        let est1_before = sketch1.estimate();
        sketch1.merge_inplace(&sketch2);
        let est1_after = sketch1.estimate();

        // Merged estimate should be >= original (monotone)
        assert!(
            est1_after >= est1_before,
            "merge decreased estimate: {est1_before} -> {est1_after}"
        );
    }

    #[test]
    fn test_serialization_roundtrip() {
        use xxhash_rust::xxh64::xxh64;

        let mut sketch = HllSketch256::new();

        for i in 0..100u64 {
            sketch.insert_hash(xxh64(&i.to_le_bytes(), 0));
        }

        // Test raw bytes roundtrip
        let bytes = sketch.to_bytes();
        let restored = HllSketch256::from_bytes(&bytes);
        assert_eq!(sketch, restored);

        // Test versioned bytes roundtrip
        let versioned = sketch.to_bytes_versioned();
        assert_eq!(versioned.len(), 257);
        let restored_v = HllSketch256::from_bytes_versioned(&versioned).unwrap();
        assert_eq!(sketch, restored_v);
    }

    #[test]
    fn test_estimate_large_cardinality() {
        use xxhash_rust::xxh64::xxh64;

        let mut sketch = HllSketch256::new();

        // Insert 10000 distinct values with proper hashing
        for i in 0..10000u64 {
            sketch.insert_hash(xxh64(&i.to_le_bytes(), 0));
        }

        let estimate = sketch.estimate();
        // HLL at p=8 has ~6.5% standard error
        // For 10000 values: 10000 ± 650 at 1σ, ± 1300 at 2σ, ± 1950 at 3σ
        // Using 3σ bounds with margin: [7000, 13000]
        assert!(
            (7000..=13000).contains(&estimate),
            "large estimate {estimate} not in expected range [7000, 13000]"
        );
    }

    #[test]
    fn test_deterministic_hash_insertion() {
        // Same hash should always update the same register
        let mut sketch1 = HllSketch256::new();
        let mut sketch2 = HllSketch256::new();

        sketch1.insert_hash(0x1234_5678_9abc_def0);
        sketch2.insert_hash(0x1234_5678_9abc_def0);

        assert_eq!(sketch1, sketch2);
    }
}
