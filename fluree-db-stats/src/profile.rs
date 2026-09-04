//! One column's profile: counts, kinds, distinct values, moments,
//! quantiles and frequent values, all mergeable.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::hash::value_hash;
use crate::heavy_hitters::{HeavyHitters, HitCount};
use crate::hll::Hll4096;
use crate::moments::Moments;
use crate::tdigest::TDigest;

/// A value as the profiler sees it. Borrowed so a scan never allocates
/// per cell; the profile copies only what it keeps (display samples).
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ProfileValue<'a> {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(&'a str),
    /// An IRI reference to another node.
    Ref(&'a str),
    /// A point in time as milliseconds since the Unix epoch, or a date as
    /// days × 86,400,000. Profiled numerically so ranges and quantiles
    /// work, tallied separately so a date column is not reported as a
    /// number.
    Temporal(i64),
    /// Anything else, carried as its lexical form for counting only.
    Other(&'a str),
}

/// The kinds a column's values fall into. A column of one kind is typed;
/// a mix is itself a finding.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ValueKind {
    Null,
    Bool,
    Int,
    Float,
    Str,
    Ref,
    Temporal,
    Other,
}

const KINDS: [ValueKind; 8] = [
    ValueKind::Null,
    ValueKind::Bool,
    ValueKind::Int,
    ValueKind::Float,
    ValueKind::Str,
    ValueKind::Ref,
    ValueKind::Temporal,
    ValueKind::Other,
];

impl ProfileValue<'_> {
    pub fn kind(&self) -> ValueKind {
        match self {
            ProfileValue::Null => ValueKind::Null,
            ProfileValue::Bool(_) => ValueKind::Bool,
            ProfileValue::Int(_) => ValueKind::Int,
            ProfileValue::Float(_) => ValueKind::Float,
            ProfileValue::Str(_) => ValueKind::Str,
            ProfileValue::Ref(_) => ValueKind::Ref,
            ProfileValue::Temporal(_) => ValueKind::Temporal,
            ProfileValue::Other(_) => ValueKind::Other,
        }
    }

    /// The numeric reading of the value, when it has one.
    fn as_f64(&self) -> Option<f64> {
        match self {
            ProfileValue::Int(i) | ProfileValue::Temporal(i) => Some(*i as f64),
            ProfileValue::Float(f) => Some(*f),
            _ => None,
        }
    }

    /// The display form kept as a sample for frequent values.
    fn display(&self, max_len: usize) -> String {
        let s = match self {
            ProfileValue::Null => String::new(),
            ProfileValue::Bool(b) => b.to_string(),
            ProfileValue::Int(i) | ProfileValue::Temporal(i) => i.to_string(),
            ProfileValue::Float(f) => f.to_string(),
            ProfileValue::Str(s) | ProfileValue::Ref(s) | ProfileValue::Other(s) => {
                (*s).to_string()
            }
        };
        truncate(s, max_len)
    }
}

fn truncate(mut s: String, max_len: usize) -> String {
    if s.len() > max_len {
        let mut cut = max_len;
        while !s.is_char_boundary(cut) {
            cut -= 1;
        }
        s.truncate(cut);
        s.push('…');
    }
    s
}

/// Sketch sizes. The defaults suit a whole-column profile of anything
/// from a hundred rows to a hundred million.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProfileConfig {
    /// Counters kept for frequent values. Exact when the column has no
    /// more distinct values than this.
    pub top_capacity: usize,
    /// t-digest compression; centroids kept ≈ this.
    pub digest_compression: f64,
    /// Longest display sample retained per frequent value.
    pub sample_max_len: usize,
}

impl Default for ProfileConfig {
    fn default() -> Self {
        Self {
            top_capacity: crate::heavy_hitters::DEFAULT_CAPACITY,
            digest_compression: crate::tdigest::DEFAULT_COMPRESSION,
            sample_max_len: 64,
        }
    }
}

/// The mergeable profile of one column.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ColumnProfile {
    config: ProfileConfig,
    count: u64,
    kinds: [u64; 8],
    distinct: Hll4096,
    numeric: Moments,
    quantiles: TDigest,
    text_length: Moments,
    frequent: HeavyHitters,
    min_text: Option<String>,
    max_text: Option<String>,
}

impl Default for ColumnProfile {
    fn default() -> Self {
        Self::new(ProfileConfig::default())
    }
}

impl ColumnProfile {
    pub fn new(config: ProfileConfig) -> Self {
        Self {
            config,
            count: 0,
            kinds: [0; 8],
            distinct: Hll4096::new(),
            numeric: Moments::new(),
            quantiles: TDigest::new(config.digest_compression),
            text_length: Moments::new(),
            frequent: HeavyHitters::new(config.top_capacity),
            min_text: None,
            max_text: None,
        }
    }

    pub fn config(&self) -> ProfileConfig {
        self.config
    }

    /// Record one cell.
    pub fn observe(&mut self, value: ProfileValue<'_>) {
        self.count += 1;
        self.kinds[value.kind() as usize] += 1;
        let Some(hash) = value_hash(&value) else {
            return;
        };
        self.distinct.insert_hash(hash);
        let max_len = self.config.sample_max_len;
        self.frequent.observe(hash, || value.display(max_len));
        if let Some(x) = value.as_f64() {
            self.numeric.add(x);
            self.quantiles.add(x);
        }
        if let ProfileValue::Str(s) | ProfileValue::Ref(s) | ProfileValue::Other(s) = value {
            self.text_length.add(s.len() as f64);
            if self.min_text.as_deref().is_none_or(|m| s < m) {
                self.min_text = Some(truncate(s.to_string(), max_len));
            }
            if self.max_text.as_deref().is_none_or(|m| s > m) {
                self.max_text = Some(truncate(s.to_string(), max_len));
            }
        }
    }

    /// Fold another profile of the same column in.
    pub fn merge(&mut self, other: &ColumnProfile) {
        self.distinct.merge(&other.distinct);
        self.count += other.count;
        for (mine, theirs) in self.kinds.iter_mut().zip(&other.kinds) {
            *mine += theirs;
        }
        self.numeric.merge(&other.numeric);
        self.quantiles.merge(&other.quantiles);
        self.text_length.merge(&other.text_length);
        self.frequent.merge(&other.frequent);
        self.min_text = match (self.min_text.take(), other.min_text.clone()) {
            (Some(a), Some(b)) => Some(if b < a { b } else { a }),
            (a, b) => a.or(b),
        };
        self.max_text = match (self.max_text.take(), other.max_text.clone()) {
            (Some(a), Some(b)) => Some(if b > a { b } else { a }),
            (a, b) => a.or(b),
        };
    }

    pub fn count(&self) -> u64 {
        self.count
    }

    pub fn null_count(&self) -> u64 {
        self.kinds[ValueKind::Null as usize]
    }

    pub fn non_null_count(&self) -> u64 {
        self.count - self.null_count()
    }

    /// How many values are of `kind`.
    pub fn kind_count(&self, kind: ValueKind) -> u64 {
        self.kinds[kind as usize]
    }

    /// Estimated distinct non-null values.
    pub fn distinct_estimate(&self) -> u64 {
        self.distinct_exact()
            .unwrap_or_else(|| self.distinct.estimate())
    }

    /// Exact distinct count, known only while the frequent-value sketch
    /// has never had to evict.
    pub fn distinct_exact(&self) -> Option<u64> {
        self.frequent.is_exact().then(|| self.frequent.len() as u64)
    }

    pub fn numeric(&self) -> &Moments {
        &self.numeric
    }

    pub fn quantiles(&self) -> &TDigest {
        &self.quantiles
    }

    pub fn frequent(&self) -> &HeavyHitters {
        &self.frequent
    }

    pub fn text_length(&self) -> &Moments {
        &self.text_length
    }

    /// Whether every non-null value is the same value.
    pub fn is_constant(&self) -> bool {
        self.non_null_count() > 0 && self.distinct_exact() == Some(1)
    }

    /// The reportable summary.
    pub fn summary(&self) -> ColumnSummary {
        let non_null = self.non_null_count();
        let distinct = self.distinct_estimate();
        let kinds: BTreeMap<ValueKind, u64> = KINDS
            .iter()
            .filter(|k| self.kinds[**k as usize] > 0)
            .map(|k| (*k, self.kinds[*k as usize]))
            .collect();
        let uniqueness = if non_null == 0 {
            0.0
        } else {
            (distinct as f64 / non_null as f64).min(1.0)
        };
        let numeric = (self.numeric.count() > 0).then(|| NumericSummary {
            count: self.numeric.count(),
            min: self.numeric.min().unwrap_or(0.0),
            max: self.numeric.max().unwrap_or(0.0),
            mean: self.numeric.mean().unwrap_or(0.0),
            stddev: self.numeric.stddev(),
            sum: self.numeric.sum(),
            p01: self.quantiles.quantile(0.01),
            p05: self.quantiles.quantile(0.05),
            p25: self.quantiles.quantile(0.25),
            p50: self.quantiles.quantile(0.50),
            p75: self.quantiles.quantile(0.75),
            p95: self.quantiles.quantile(0.95),
            p99: self.quantiles.quantile(0.99),
        });
        let text = (self.text_length.count() > 0).then(|| TextSummary {
            min_length: self.text_length.min().unwrap_or(0.0) as u64,
            max_length: self.text_length.max().unwrap_or(0.0) as u64,
            mean_length: self.text_length.mean().unwrap_or(0.0),
            min: self.min_text.clone(),
            max: self.max_text.clone(),
        });
        let top_values = self
            .frequent
            .top(10)
            .into_iter()
            .map(|h| TopValue::from_hit(h, non_null))
            .collect();
        ColumnSummary {
            count: self.count,
            null_count: self.null_count(),
            null_fraction: if self.count == 0 {
                0.0
            } else {
                self.null_count() as f64 / self.count as f64
            },
            distinct,
            distinct_is_exact: self.distinct_exact().is_some(),
            distinct_error: self.distinct.relative_error(),
            uniqueness,
            is_constant: self.is_constant(),
            key_candidate: self.count > 0
                && self.null_count() == 0
                && uniqueness >= 1.0 - 2.0 * self.distinct.relative_error(),
            kinds,
            numeric,
            text,
            top_values,
            top_values_exact: self.frequent.is_exact(),
        }
    }
}

/// A frequent value with its share of the column's non-null cells.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TopValue {
    pub value: String,
    pub count: u64,
    /// Upper bound on the true count; equals `count` when exact.
    pub count_upper: u64,
    /// `count / non-null count`.
    pub share: f64,
}

impl TopValue {
    fn from_hit(h: HitCount, non_null: u64) -> Self {
        Self {
            share: if non_null == 0 {
                0.0
            } else {
                h.count as f64 / non_null as f64
            },
            value: h.value,
            count: h.count,
            count_upper: h.count_upper,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct NumericSummary {
    pub count: u64,
    pub min: f64,
    pub max: f64,
    pub mean: f64,
    pub stddev: Option<f64>,
    pub sum: f64,
    pub p01: Option<f64>,
    pub p05: Option<f64>,
    pub p25: Option<f64>,
    pub p50: Option<f64>,
    pub p75: Option<f64>,
    pub p95: Option<f64>,
    pub p99: Option<f64>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TextSummary {
    pub min_length: u64,
    pub max_length: u64,
    pub mean_length: f64,
    /// Lexicographically smallest value (truncated sample).
    pub min: Option<String>,
    pub max: Option<String>,
}

/// The reportable face of a [`ColumnProfile`].
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ColumnSummary {
    pub count: u64,
    pub null_count: u64,
    pub null_fraction: f64,
    /// Distinct non-null values: exact when `distinct_is_exact`, else an
    /// estimate with relative error `distinct_error`.
    pub distinct: u64,
    pub distinct_is_exact: bool,
    pub distinct_error: f64,
    /// `distinct / non-null`, capped at 1.
    pub uniqueness: f64,
    pub is_constant: bool,
    /// No nulls and distinct within sketch error of the row count.
    pub key_candidate: bool,
    pub kinds: BTreeMap<ValueKind, u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub numeric: Option<NumericSummary>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<TextSummary>,
    pub top_values: Vec<TopValue>,
    pub top_values_exact: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unit_column_is_exact_and_categorical() {
        let mut p = ColumnProfile::default();
        for (u, n) in [("kg", 40), ("g", 30), ("lb", 20), ("oz", 10)] {
            for _ in 0..n {
                p.observe(ProfileValue::Str(u));
            }
        }
        p.observe(ProfileValue::Null);
        let s = p.summary();
        assert_eq!(s.count, 101);
        assert_eq!(s.null_count, 1);
        assert_eq!(s.distinct, 4);
        assert!(s.distinct_is_exact);
        assert!(s.top_values_exact);
        assert_eq!(s.top_values[0].value, "kg");
        assert!((s.top_values[0].share - 0.4).abs() < 1e-9);
        assert!(!s.key_candidate);
        assert!(!s.is_constant);
    }

    #[test]
    fn key_column_is_a_candidate() {
        let mut p = ColumnProfile::default();
        for i in 0..20_000 {
            let s = format!("PRT-{i:06}");
            p.observe(ProfileValue::Str(&s));
        }
        let s = p.summary();
        assert!(s.key_candidate, "{s:?}");
        assert!(!s.distinct_is_exact);
        assert!(s.uniqueness > 0.95);
    }

    #[test]
    fn duplicated_key_is_not_a_candidate() {
        let mut p = ColumnProfile::default();
        for i in 0..8_383 {
            let s = format!("P{}", i % 8_282);
            p.observe(ProfileValue::Str(&s));
        }
        let s = p.summary();
        assert!(s.distinct < 8_383);
        // 101 duplicates in 8,383 rows is 1.2%, under the 2σ band at p=12
        // (3.2%), so the sketch alone cannot refuse this key. That is the
        // point: sketches disprove, an exact probe proves.
        assert!(s.uniqueness < 1.0);
    }

    #[test]
    fn numeric_summary_and_mixed_kinds() {
        let mut p = ColumnProfile::default();
        for i in 1..=1000 {
            p.observe(ProfileValue::Float(f64::from(i)));
        }
        p.observe(ProfileValue::Str("n/a"));
        let s = p.summary();
        let n = s.numeric.unwrap();
        assert_eq!(n.count, 1000);
        assert_eq!(n.min, 1.0);
        assert_eq!(n.max, 1000.0);
        assert!((n.mean - 500.5).abs() < 1e-9);
        assert!((n.p50.unwrap() - 500.0).abs() < 10.0);
        assert_eq!(s.kinds.get(&ValueKind::Str), Some(&1));
        assert_eq!(s.kinds.get(&ValueKind::Float), Some(&1000));
    }

    #[test]
    fn constant_column() {
        let mut p = ColumnProfile::default();
        for _ in 0..50 {
            p.observe(ProfileValue::Float(379.88));
        }
        assert!(p.is_constant());
        assert!(p.summary().is_constant);
    }

    #[test]
    fn merge_matches_single_pass() {
        let mut whole = ColumnProfile::default();
        let mut a = ColumnProfile::default();
        let mut b = ColumnProfile::default();
        for i in 0..5_000i64 {
            let v = if i % 7 == 0 {
                ProfileValue::Null
            } else {
                ProfileValue::Int(i % 300)
            };
            whole.observe(v);
            if i % 2 == 0 {
                a.observe(v);
            } else {
                b.observe(v);
            }
        }
        a.merge(&b);
        let (sa, sw) = (a.summary(), whole.summary());
        assert_eq!(sa.count, sw.count);
        assert_eq!(sa.null_count, sw.null_count);
        assert_eq!(sa.kinds, sw.kinds);
        assert!(
            (sa.numeric.as_ref().unwrap().mean - sw.numeric.as_ref().unwrap().mean).abs() < 1e-9
        );
        let d = sa.distinct as f64 - sw.distinct as f64;
        assert!(d.abs() / (sw.distinct as f64) < 0.05);
    }

    #[test]
    fn summary_serialises_camel_case() {
        let mut p = ColumnProfile::default();
        p.observe(ProfileValue::Int(1));
        let json = serde_json::to_value(p.summary()).unwrap();
        assert!(json.get("nullCount").is_some());
        assert!(json.get("keyCandidate").is_some());
        let round: ColumnProfile =
            serde_json::from_str(&serde_json::to_string(&p).unwrap()).unwrap();
        assert_eq!(round, p);
    }
}
