//! Random `f64` vector generators.
//!
//! Two flavors:
//!
//! - [`hashed_pair()`] / [`hashed_one()`] — deterministic via `DefaultHasher`,
//!   no RNG state needed. Lifted verbatim from `vector_math.rs`'s
//!   `random_vectors`. Use for pure-math micro-benches that want
//!   reproducibility without an `Rng` argument.
//! - [`rng_one()`] — RNG-driven uniform `[-1.0, 1.0)`. Lifted from
//!   `vector_query.rs`'s `random_vector`. Use when you already have a
//!   seeded `Rng` (e.g., a bench that generates many vectors plus other
//!   per-document fields from the same RNG chain).

use rand::Rng;

/// Two deterministic `f64` vectors of the requested dimension.
///
/// Same algorithm as `vector_math.rs`'s `random_vectors`: salt the index `i`
/// with `0` for the first vector and with `1000` for the second, hash with
/// `DefaultHasher`, and project the resulting `u64` into `[-1.0, 1.0)`.
///
/// Output is byte-identical across runs given the same `dim`.
pub fn hashed_pair(dim: usize) -> (Vec<f64>, Vec<f64>) {
    let mut a = Vec::with_capacity(dim);
    let mut b = Vec::with_capacity(dim);
    for i in 0..dim {
        a.push(hashed_value(i));
        b.push(hashed_value(i + 1000));
    }
    (a, b)
}

/// One deterministic `f64` vector of the requested dimension, salted by
/// `seed`. Two calls with the same `(dim, seed)` return the same vector.
pub fn hashed_one(dim: usize, seed: usize) -> Vec<f64> {
    (0..dim).map(|i| hashed_value(i + seed)).collect()
}

/// Hash an index into `[-1.0, 1.0)`. Replicates `vector_math.rs`'s
/// `(h as f64) / (u64::MAX as f64) * 2.0 - 1.0` projection.
fn hashed_value(i: usize) -> f64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut hasher = DefaultHasher::new();
    i.hash(&mut hasher);
    let h = hasher.finish();
    (h as f64) / (u64::MAX as f64) * 2.0 - 1.0
}

/// Generate one `f64` vector of the requested dimension by drawing from the
/// supplied RNG. Each component is uniform in `[-1.0, 1.0)`.
///
/// Lifted from `vector_query.rs`'s `random_vector`. Returns `Vec<f64>`
/// (not `Vec<f32>`) to match the existing bench's intermediate type; the
/// `@vector` ingest path quantizes to `f32` separately.
pub fn rng_one(rng: &mut impl Rng, dim: usize) -> Vec<f64> {
    (0..dim)
        .map(|_| rng.gen_range(-1.0f32..1.0f32) as f64)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hashed_pair_is_deterministic() {
        let (a1, b1) = hashed_pair(64);
        let (a2, b2) = hashed_pair(64);
        assert_eq!(a1, a2);
        assert_eq!(b1, b2);
    }

    #[test]
    fn hashed_pair_components_in_range() {
        let (a, b) = hashed_pair(128);
        assert_eq!(a.len(), 128);
        assert_eq!(b.len(), 128);
        for x in a.iter().chain(b.iter()) {
            assert!((-1.0..=1.0).contains(x), "out of range: {x}");
        }
    }

    #[test]
    fn hashed_pair_distinct_seeds() {
        let (a, b) = hashed_pair(32);
        // The two vectors must not be identical (the +1000 salt should diverge).
        assert_ne!(a, b);
    }

    #[test]
    fn hashed_one_seed_changes_output() {
        let v0 = hashed_one(16, 0);
        let v1 = hashed_one(16, 1);
        assert_ne!(v0, v1);
    }

    #[test]
    fn rng_one_in_range() {
        use rand::rngs::StdRng;
        use rand::SeedableRng;
        let mut rng = StdRng::seed_from_u64(42);
        let v = rng_one(&mut rng, 64);
        assert_eq!(v.len(), 64);
        for x in &v {
            assert!((-1.0..=1.0).contains(x), "out of range: {x}");
        }
    }
}
