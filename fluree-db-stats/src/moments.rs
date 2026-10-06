//! Exact running moments: count, mean, variance, min, max, sum.
//!
//! Welford's update for a single stream, Chan's formula for merging two
//! streams, so the merge of two shards' moments is exactly the moments of
//! the union.

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Moments {
    count: u64,
    mean: f64,
    m2: f64,
    sum: f64,
    min: Option<f64>,
    max: Option<f64>,
}

impl Moments {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add one observation. Non-finite values are ignored: a NaN would
    /// poison every derived statistic and an infinity has no mean.
    #[inline]
    pub fn add(&mut self, x: f64) {
        if !x.is_finite() {
            return;
        }
        self.count += 1;
        let delta = x - self.mean;
        self.mean += delta / self.count as f64;
        self.m2 += delta * (x - self.mean);
        self.sum += x;
        self.min = Some(self.min.map_or(x, |m| m.min(x)));
        self.max = Some(self.max.map_or(x, |m| m.max(x)));
    }

    /// Combine with another stream's moments.
    pub fn merge(&mut self, other: &Moments) {
        if other.count == 0 {
            return;
        }
        if self.count == 0 {
            *self = other.clone();
            return;
        }
        let n_a = self.count as f64;
        let n_b = other.count as f64;
        let n = n_a + n_b;
        let delta = other.mean - self.mean;
        self.mean += delta * n_b / n;
        self.m2 += other.m2 + delta * delta * n_a * n_b / n;
        self.sum += other.sum;
        self.count += other.count;
        self.min = match (self.min, other.min) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        self.max = match (self.max, other.max) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (a, b) => a.or(b),
        };
    }

    pub fn count(&self) -> u64 {
        self.count
    }

    pub fn sum(&self) -> f64 {
        self.sum
    }

    pub fn mean(&self) -> Option<f64> {
        (self.count > 0).then_some(self.mean)
    }

    /// Sample variance (n − 1 denominator); `None` below two observations.
    pub fn variance(&self) -> Option<f64> {
        (self.count > 1).then(|| self.m2 / (self.count - 1) as f64)
    }

    pub fn stddev(&self) -> Option<f64> {
        self.variance().map(f64::sqrt)
    }

    pub fn min(&self) -> Option<f64> {
        self.min
    }

    pub fn max(&self) -> Option<f64> {
        self.max
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn close(a: f64, b: f64) -> bool {
        (a - b).abs() < 1e-9 * (1.0 + a.abs().max(b.abs()))
    }

    #[test]
    fn matches_two_pass_statistics() {
        let xs: Vec<f64> = (1..=100).map(|i| f64::from(i) * 0.5).collect();
        let mut m = Moments::new();
        for &x in &xs {
            m.add(x);
        }
        let mean = xs.iter().sum::<f64>() / xs.len() as f64;
        let var = xs.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / (xs.len() - 1) as f64;
        assert!(close(m.mean().unwrap(), mean));
        assert!(close(m.variance().unwrap(), var));
        assert_eq!(m.min(), Some(0.5));
        assert_eq!(m.max(), Some(50.0));
        assert_eq!(m.count(), 100);
    }

    #[test]
    fn merge_equals_single_stream() {
        let xs: Vec<f64> = (0..1000).map(|i| (f64::from(i) * 7.3) % 101.0).collect();
        let mut whole = Moments::new();
        let mut a = Moments::new();
        let mut b = Moments::new();
        for (i, &x) in xs.iter().enumerate() {
            whole.add(x);
            if i % 3 == 0 {
                a.add(x);
            } else {
                b.add(x);
            }
        }
        a.merge(&b);
        assert_eq!(a.count(), whole.count());
        assert!(close(a.mean().unwrap(), whole.mean().unwrap()));
        assert!(close(a.variance().unwrap(), whole.variance().unwrap()));
        assert_eq!(a.min(), whole.min());
        assert_eq!(a.max(), whole.max());
    }

    #[test]
    fn non_finite_is_ignored() {
        let mut m = Moments::new();
        m.add(f64::NAN);
        m.add(f64::INFINITY);
        assert_eq!(m.count(), 0);
        assert_eq!(m.mean(), None);
    }

    #[test]
    fn merge_into_empty_copies() {
        let mut a = Moments::new();
        let mut b = Moments::new();
        b.add(3.0);
        a.merge(&b);
        assert_eq!(a, b);
    }
}
