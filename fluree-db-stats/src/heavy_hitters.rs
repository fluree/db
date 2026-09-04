//! Frequent values with bounded memory (Misra–Gries).
//!
//! Keeps at most `capacity` counters. A value's reported count is never
//! above its true count, and never below it by more than the sketch's
//! recorded `decrements`, so every answer carries its own error bar. When
//! the column has no more distinct values than the capacity, no decrement
//! ever happens and the counts are exact: a boolean, a division code, a
//! unit-of-measure column all profile exactly.
//!
//! Merging two sketches (Agarwal et al., 2012) sums matching counters,
//! then trims back to capacity by subtracting the `(capacity+1)`-th
//! largest count from everything. The error bound is preserved: it is the
//! sum of both inputs' decrements plus the trim.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Counters kept when a caller does not choose.
pub const DEFAULT_CAPACITY: usize = 64;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Counter {
    /// A display sample of the value behind this hash: the first one seen.
    sample: String,
    count: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeavyHitters {
    capacity: usize,
    counters: HashMap<u64, Counter>,
    /// Total amount subtracted from every counter over the sketch's life;
    /// the bound on how far any reported count sits below the truth.
    decrements: u64,
}

/// One frequent value and the interval its true count lies in.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HitCount {
    pub value: String,
    /// Lower bound on the true count (the counter itself).
    pub count: u64,
    /// Upper bound on the true count.
    pub count_upper: u64,
}

impl Default for HeavyHitters {
    fn default() -> Self {
        Self::new(DEFAULT_CAPACITY)
    }
}

impl HeavyHitters {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            counters: HashMap::with_capacity(capacity.max(1) + 1),
            decrements: 0,
        }
    }

    /// Record one occurrence of the value behind `hash`. `sample` is only
    /// called when a new counter is opened, so callers can pass a closure
    /// that formats the value lazily.
    #[inline]
    pub fn observe(&mut self, hash: u64, sample: impl FnOnce() -> String) {
        if let Some(c) = self.counters.get_mut(&hash) {
            c.count += 1;
            return;
        }
        if self.counters.len() < self.capacity {
            self.counters.insert(
                hash,
                Counter {
                    sample: sample(),
                    count: 1,
                },
            );
            return;
        }
        // Full and unseen: charge one to every counter, drop the zeros.
        self.decrements += 1;
        self.counters.retain(|_, c| {
            c.count -= 1;
            c.count > 0
        });
    }

    /// Fold another sketch in.
    pub fn merge(&mut self, other: &HeavyHitters) {
        for (hash, theirs) in &other.counters {
            match self.counters.get_mut(hash) {
                Some(mine) => mine.count += theirs.count,
                None => {
                    self.counters.insert(*hash, theirs.clone());
                }
            }
        }
        self.decrements += other.decrements;
        if self.counters.len() > self.capacity {
            let mut counts: Vec<u64> = self.counters.values().map(|c| c.count).collect();
            counts.sort_unstable_by(|a, b| b.cmp(a));
            let cut = counts[self.capacity];
            self.decrements += cut;
            self.counters.retain(|_, c| {
                c.count = c.count.saturating_sub(cut);
                c.count > 0
            });
        }
    }

    /// Whether every reported count is the true count.
    pub fn is_exact(&self) -> bool {
        self.decrements == 0
    }

    /// The number of counters currently held. When [`is_exact`](Self::is_exact)
    /// this is the column's exact distinct count.
    pub fn len(&self) -> usize {
        self.counters.len()
    }

    pub fn is_empty(&self) -> bool {
        self.counters.is_empty()
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn decrements(&self) -> u64 {
        self.decrements
    }

    /// The most frequent values, highest first, at most `n`.
    pub fn top(&self, n: usize) -> Vec<HitCount> {
        let mut out: Vec<HitCount> = self
            .counters
            .values()
            .map(|c| HitCount {
                value: c.sample.clone(),
                count: c.count,
                count_upper: c.count + self.decrements,
            })
            .collect();
        out.sort_by(|a, b| b.count.cmp(&a.count).then_with(|| a.value.cmp(&b.value)));
        out.truncate(n);
        out
    }

    /// Lower-bound count for one value's hash; zero when not tracked.
    pub fn count_of(&self, hash: u64) -> u64 {
        self.counters.get(&hash).map_or(0, |c| c.count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn h(s: &str) -> u64 {
        xxhash_rust::xxh3::xxh3_64(s.as_bytes())
    }

    #[test]
    fn exact_below_capacity() {
        let mut hh = HeavyHitters::new(8);
        for (v, n) in [("kg", 5), ("g", 3), ("lb", 1)] {
            for _ in 0..n {
                hh.observe(h(v), || v.to_string());
            }
        }
        assert!(hh.is_exact());
        assert_eq!(hh.len(), 3);
        let top = hh.top(10);
        assert_eq!(top[0].value, "kg");
        assert_eq!(top[0].count, 5);
        assert_eq!(top[0].count_upper, 5);
    }

    #[test]
    fn heavy_value_survives_a_flood_of_singletons() {
        let mut hh = HeavyHitters::new(4);
        for i in 0..10_000u32 {
            if i % 3 == 0 {
                hh.observe(h("catch-all"), || "catch-all".into());
            } else {
                let s = format!("part-{i}");
                hh.observe(h(&s), || s.clone());
            }
        }
        assert!(!hh.is_exact());
        let top = hh.top(1);
        assert_eq!(top[0].value, "catch-all");
        let truth = 10_000u64.div_ceil(3);
        assert!(top[0].count <= truth);
        assert!(top[0].count_upper >= truth);
    }

    #[test]
    fn merge_keeps_bounds() {
        let mut a = HeavyHitters::new(3);
        let mut b = HeavyHitters::new(3);
        for i in 0..300u32 {
            let s = if i % 2 == 0 {
                "x".to_string()
            } else {
                format!("y{i}")
            };
            if i < 150 {
                a.observe(h(&s), || s.clone());
            } else {
                b.observe(h(&s), || s.clone());
            }
        }
        a.merge(&b);
        assert!(a.len() <= 3);
        let x = a.top(1).into_iter().next().unwrap();
        assert_eq!(x.value, "x");
        assert!(x.count <= 150 && x.count_upper >= 150);
    }

    #[test]
    fn json_round_trip() {
        let mut hh = HeavyHitters::new(4);
        hh.observe(h("a"), || "a".into());
        let json = serde_json::to_string(&hh).unwrap();
        let back: HeavyHitters = serde_json::from_str(&json).unwrap();
        assert_eq!(hh, back);
    }
}
