//! Mergeable quantile sketch (t-digest, merging variant).
//!
//! Centroids are kept sorted by mean with a size limit that shrinks toward
//! the tails, so the median is approximate to about one percent and the
//! first and ninety-ninth percentiles far tighter. New points buffer and
//! are merged in batches; merging two digests is the same batch merge over
//! both centroid sets, which makes the result of merging shards a valid
//! digest of the union.
//!
//! Dunning & Ertl, "Computing extremely accurate quantiles using
//! t-digests" (2019), with the `k_1` scale function.
//!
//! The observed extremes are options rather than ±infinity so an empty
//! digest serialises to JSON and reads back (serde_json writes a
//! non-finite float as `null`, which a bare `f64` refuses).

use serde::{Deserialize, Serialize};
use std::f64::consts::PI;

/// Compression parameter used when a caller does not choose one. The
/// digest holds at most about this many centroids after compression.
pub const DEFAULT_COMPRESSION: f64 = 100.0;

/// Smallest compression that still bounds the centroid count.
const MIN_COMPRESSION: f64 = 10.0;

fn clamp_compression(c: f64) -> f64 {
    if c.is_finite() {
        c.max(MIN_COMPRESSION)
    } else {
        DEFAULT_COMPRESSION
    }
}

fn de_compression<'de, D>(d: D) -> Result<f64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Ok(clamp_compression(f64::deserialize(d)?))
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
struct Centroid {
    mean: f64,
    weight: f64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TDigest {
    /// Clamped on the way in as well as in [`TDigest::new`]: `q_limit`
    /// is NaN for a compression of zero, so every point survives
    /// compression as its own centroid and the digest silently stops
    /// being a sketch.
    #[serde(deserialize_with = "de_compression")]
    compression: f64,
    centroids: Vec<Centroid>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    buffer: Vec<Centroid>,
    total: f64,
    min: Option<f64>,
    max: Option<f64>,
}

impl Default for TDigest {
    fn default() -> Self {
        Self::new(DEFAULT_COMPRESSION)
    }
}

impl TDigest {
    pub fn new(compression: f64) -> Self {
        Self {
            compression: clamp_compression(compression),
            centroids: Vec::new(),
            buffer: Vec::new(),
            total: 0.0,
            min: None,
            max: None,
        }
    }

    fn buffer_capacity(&self) -> usize {
        (5.0 * self.compression) as usize
    }

    /// Add one observation. Non-finite values are ignored.
    #[inline]
    pub fn add(&mut self, x: f64) {
        self.add_weighted(x, 1.0);
    }

    pub fn add_weighted(&mut self, x: f64, weight: f64) {
        if !x.is_finite() || weight.is_nan() || weight <= 0.0 {
            return;
        }
        self.buffer.push(Centroid { mean: x, weight });
        self.total += weight;
        self.min = Some(self.min.map_or(x, |m| m.min(x)));
        self.max = Some(self.max.map_or(x, |m| m.max(x)));
        if self.buffer.len() >= self.buffer_capacity() {
            self.compress();
        }
    }

    /// Fold another digest in. Compression parameters need not match; the
    /// result keeps this digest's.
    pub fn merge(&mut self, other: &TDigest) {
        if other.total == 0.0 {
            return;
        }
        self.buffer.extend_from_slice(&other.centroids);
        self.buffer.extend_from_slice(&other.buffer);
        self.total += other.total;
        self.min = min_of(self.min, other.min);
        self.max = max_of(self.max, other.max);
        self.compress();
    }

    /// Total weight observed.
    /// Centroids currently held, buffer excluded.
    pub fn centroid_count(&self) -> usize {
        self.centroids.len()
    }

    pub fn count(&self) -> f64 {
        self.total
    }

    pub fn is_empty(&self) -> bool {
        self.total == 0.0
    }

    /// Merge the buffer into the centroid list. Idempotent when the
    /// buffer is empty.
    pub fn compress(&mut self) {
        if self.buffer.is_empty() {
            return;
        }
        let mut points = std::mem::take(&mut self.centroids);
        points.append(&mut self.buffer);
        points.sort_by(|a, b| a.mean.total_cmp(&b.mean));

        let total: f64 = points.iter().map(|c| c.weight).sum();
        let mut merged: Vec<Centroid> = Vec::with_capacity(points.len());
        let mut iter = points.into_iter();
        let Some(mut current) = iter.next() else {
            return;
        };
        let mut weight_before = 0.0f64;
        let mut q_limit = self.q_from_k(self.k(0.0) + 1.0);

        for next in iter {
            let q = (weight_before + current.weight + next.weight) / total;
            if q <= q_limit {
                let w = current.weight + next.weight;
                current.mean += (next.mean - current.mean) * next.weight / w;
                current.weight = w;
            } else {
                weight_before += current.weight;
                merged.push(current);
                q_limit = self.q_from_k(self.k(weight_before / total) + 1.0);
                current = next;
            }
        }
        merged.push(current);
        // `merged` was sized for every input point but holds only the
        // surviving centroids — around a hundred against a buffer of
        // five hundred. A grouped profile keeps one digest per group,
        // so the slack is per group; the shrink costs one realloc
        // against the sort just done.
        merged.shrink_to_fit();
        self.centroids = merged;
        self.total = total;
    }

    fn k(&self, q: f64) -> f64 {
        self.compression / (2.0 * PI) * (2.0 * q - 1.0).clamp(-1.0, 1.0).asin()
    }

    fn q_from_k(&self, k: f64) -> f64 {
        f64::midpoint((k * 2.0 * PI / self.compression).sin(), 1.0)
    }

    /// A copy with its buffer folded in, or `self` when there is
    /// nothing buffered.
    ///
    /// [`Self::quantile`] has to do this internally when the buffer is
    /// dirty, so a caller reading several quantiles from one digest —
    /// the seven a column summary reports — should compress once here
    /// and read them all from the result.
    pub fn compressed(&self) -> std::borrow::Cow<'_, TDigest> {
        if self.buffer.is_empty() {
            return std::borrow::Cow::Borrowed(self);
        }
        let mut c = self.clone();
        c.compress();
        std::borrow::Cow::Owned(c)
    }

    /// The value at quantile `q` in `[0, 1]`. `None` when empty.
    ///
    /// Compresses a clone when the buffer is dirty; see
    /// [`Self::compressed`] to pay that once across several reads.
    pub fn quantile(&self, q: f64) -> Option<f64> {
        if self.total == 0.0 {
            return None;
        }
        if !self.buffer.is_empty() {
            return self.compressed().quantile(q);
        }
        let (min, max) = (self.min?, self.max?);
        let q = q.clamp(0.0, 1.0);
        if q == 0.0 {
            return Some(min);
        }
        if q == 1.0 {
            return Some(max);
        }
        let target = q * self.total;
        let n = self.centroids.len();
        if n == 1 {
            return Some(self.centroids[0].mean);
        }

        // Each centroid sits at the midpoint of the weight it covers;
        // interpolate between neighbouring centroid positions, and between
        // the observed extremes and the outermost centroids.
        let mut cum = 0.0f64;
        let mut prev_pos = 0.0f64;
        let mut prev_mean = min;
        for (i, c) in self.centroids.iter().enumerate() {
            let pos = cum + c.weight / 2.0;
            if target < pos {
                let span = pos - prev_pos;
                let frac = if span > 0.0 {
                    (target - prev_pos) / span
                } else {
                    0.0
                };
                return Some(prev_mean + (c.mean - prev_mean) * frac);
            }
            cum += c.weight;
            prev_pos = pos;
            prev_mean = c.mean;
            if i + 1 == n {
                let span = self.total - pos;
                let frac = if span > 0.0 {
                    (target - pos) / span
                } else {
                    1.0
                };
                return Some(c.mean + (max - c.mean) * frac);
            }
        }
        Some(max)
    }

    pub fn median(&self) -> Option<f64> {
        self.quantile(0.5)
    }

    /// Estimated fraction of observations at or below `x`.
    pub fn cdf(&self, x: f64) -> Option<f64> {
        if self.total == 0.0 {
            return None;
        }
        if !self.buffer.is_empty() {
            let mut c = self.clone();
            c.compress();
            return c.cdf(x);
        }
        let (min, max) = (self.min?, self.max?);
        if x <= min {
            return Some(0.0);
        }
        if x >= max {
            return Some(1.0);
        }
        let mut cum = 0.0f64;
        let mut prev_pos = 0.0f64;
        let mut prev_mean = min;
        for c in &self.centroids {
            let pos = cum + c.weight / 2.0;
            if x < c.mean {
                let span = c.mean - prev_mean;
                let frac = if span > 0.0 {
                    (x - prev_mean) / span
                } else {
                    0.0
                };
                return Some((prev_pos + (pos - prev_pos) * frac) / self.total);
            }
            cum += c.weight;
            prev_pos = pos;
            prev_mean = c.mean;
        }
        let span = max - prev_mean;
        let frac = if span > 0.0 {
            (x - prev_mean) / span
        } else {
            1.0
        };
        Some((prev_pos + (self.total - prev_pos) * frac) / self.total)
    }

    pub fn min(&self) -> Option<f64> {
        self.min
    }

    pub fn max(&self) -> Option<f64> {
        self.max
    }
}

fn min_of(a: Option<f64>, b: Option<f64>) -> Option<f64> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, b) => a.or(b),
    }
}

fn max_of(a: Option<f64>, b: Option<f64>) -> Option<f64> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.max(b)),
        (a, b) => a.or(b),
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn deserialised_compression_is_clamped() {
        // Without the clamp `q_limit` is NaN, no two centroids ever
        // merge, and the digest keeps one centroid per point.
        let mut d: super::TDigest = serde_json::from_str(
            r#"{"compression":0.0,"centroids":[],"total":0.0,"min":null,"max":null}"#,
        )
        .unwrap();
        for i in 0..5_000 {
            d.add(f64::from(i));
        }
        d.compress();
        assert!(d.centroid_count() < 600, "{}", d.centroid_count());
        assert!((d.median().unwrap() - 2_500.0).abs() < 100.0);
    }

    use super::*;
    use rand::{rngs::StdRng, Rng, SeedableRng};

    fn exact_quantile(sorted: &[f64], q: f64) -> f64 {
        let idx = ((sorted.len() - 1) as f64 * q).round() as usize;
        sorted[idx]
    }

    #[test]
    fn empty_has_no_quantiles() {
        assert_eq!(TDigest::default().median(), None);
        assert_eq!(TDigest::default().cdf(1.0), None);
    }

    #[test]
    fn single_value() {
        let mut d = TDigest::default();
        d.add(42.0);
        assert_eq!(d.quantile(0.0), Some(42.0));
        assert_eq!(d.median(), Some(42.0));
        assert_eq!(d.quantile(1.0), Some(42.0));
    }

    #[test]
    fn uniform_quantiles_are_close() {
        let mut rng = StdRng::seed_from_u64(7);
        let mut xs: Vec<f64> = (0..50_000).map(|_| rng.gen_range(0.0..1000.0)).collect();
        let mut d = TDigest::default();
        for &x in &xs {
            d.add(x);
        }
        xs.sort_by(f64::total_cmp);
        for q in [0.01, 0.1, 0.25, 0.5, 0.75, 0.9, 0.99] {
            let est = d.quantile(q).unwrap();
            let exact = exact_quantile(&xs, q);
            assert!((est - exact).abs() < 15.0, "q={q} est={est} exact={exact}");
        }
        assert!((d.cdf(500.0).unwrap() - 0.5).abs() < 0.02);
    }

    #[test]
    fn skewed_tail_is_tight() {
        // Log-normal-ish: most values small, a long right tail. The tails
        // are where a price-outlier rule reads, so they must be accurate.
        let mut rng = StdRng::seed_from_u64(11);
        let mut xs: Vec<f64> = (0..50_000)
            .map(|_| (rng.gen_range(0.0f64..1.0) * 6.0).exp())
            .collect();
        let mut d = TDigest::default();
        for &x in &xs {
            d.add(x);
        }
        xs.sort_by(f64::total_cmp);
        let est = d.quantile(0.99).unwrap();
        let exact = exact_quantile(&xs, 0.99);
        assert!(
            (est - exact).abs() / exact < 0.05,
            "est={est} exact={exact}"
        );
    }

    #[test]
    fn merged_shards_match_whole() {
        let mut rng = StdRng::seed_from_u64(3);
        let xs: Vec<f64> = (0..30_000).map(|_| rng.gen_range(-50.0..50.0)).collect();
        let mut whole = TDigest::default();
        let mut a = TDigest::default();
        let mut b = TDigest::default();
        for (i, &x) in xs.iter().enumerate() {
            whole.add(x);
            if i % 2 == 0 {
                a.add(x);
            } else {
                b.add(x);
            }
        }
        a.merge(&b);
        assert_eq!(a.count(), whole.count());
        for q in [0.05, 0.5, 0.95] {
            let m = a.quantile(q).unwrap();
            let w = whole.quantile(q).unwrap();
            assert!((m - w).abs() < 2.0, "q={q} merged={m} whole={w}");
        }
    }

    #[test]
    fn empty_digest_round_trips_json() {
        let json = serde_json::to_string(&TDigest::default()).unwrap();
        let back: TDigest = serde_json::from_str(&json).unwrap();
        assert!(back.is_empty());
        assert_eq!(back.min(), None);
        assert_eq!(back.median(), None);
    }

    #[test]
    fn json_round_trip_preserves_estimates() {
        let mut d = TDigest::default();
        for i in 0..1000 {
            d.add(f64::from(i));
        }
        let json = serde_json::to_string(&d).unwrap();
        let back: TDigest = serde_json::from_str(&json).unwrap();
        assert_eq!(back.median(), d.median());
    }
}
