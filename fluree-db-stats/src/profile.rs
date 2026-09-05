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
    /// Binary content, hashed on its bytes so distinct and frequent
    /// values are real; reported as a hex sample.
    Bytes(&'a [u8]),
    /// A point in time as milliseconds since the Unix epoch, or a date as
    /// days × 86,400,000. The millis feed the numeric moments and
    /// quantiles so ranges work; the kind is tallied on its own so a
    /// date column is visibly a date column, not a number column.
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
    Bytes,
    Temporal,
    Other,
}

const KINDS: [ValueKind; 9] = [
    ValueKind::Null,
    ValueKind::Bool,
    ValueKind::Int,
    ValueKind::Float,
    ValueKind::Str,
    ValueKind::Ref,
    ValueKind::Bytes,
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
            ProfileValue::Bytes(_) => ValueKind::Bytes,
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

    /// The value as text: what a group key or a display sample is built
    /// from. Nulls read as the empty string, bytes as `0x` plus hex.
    pub fn to_text(&self) -> String {
        let mut out = String::new();
        self.write_text(&mut out);
        out
    }

    /// Append the display form to `out`.
    ///
    /// Building a group key over several cells is the hot use, and it
    /// wants the text in a buffer it already owns rather than one
    /// `String` per cell.
    pub fn write_text(&self, out: &mut String) {
        use std::fmt::Write;
        match self {
            ProfileValue::Null => {}
            ProfileValue::Bool(b) => {
                let _ = write!(out, "{b}");
            }
            ProfileValue::Int(i) | ProfileValue::Temporal(i) => {
                let _ = write!(out, "{i}");
            }
            ProfileValue::Float(f) => {
                let _ = write!(out, "{f}");
            }
            ProfileValue::Str(s) | ProfileValue::Ref(s) | ProfileValue::Other(s) => {
                out.push_str(s);
            }
            ProfileValue::Bytes(b) => {
                out.push_str("0x");
                for byte in *b {
                    let _ = write!(out, "{byte:02x}");
                }
            }
        }
    }

    /// The display form kept as a sample for frequent values.
    fn display(&self, max_len: usize) -> String {
        truncate(self.to_text(), max_len)
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
    kinds: [u64; 9],
    /// Absent until the frequent-value table fills. Below that the
    /// distinct count is exact from the counters and the sketch would
    /// never be read; a grouped profile keeps one profile per group and
    /// most groups never get there, so the 4 KB of registers is not
    /// paid for them. Allocated the moment the table reaches capacity,
    /// seeded with the counters' hashes while they are still all there,
    /// and fed every hash from then on, so its registers are exactly
    /// what an always-present sketch would hold.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    distinct: Option<Box<Hll4096>>,
    numeric: Moments,
    quantiles: TDigest,
    /// Length in characters of every text value.
    text_length: Moments,
    /// Numeric cells that were not finite. They count toward `count` and
    /// their kind, but are kept out of the moments and the digest: one
    /// infinity poisons `sum`, `mean`, `min` and `max` alike, and a
    /// non-finite `f64` serialises as JSON `null`, which then fails to
    /// read back as an `f64` — the whole report stops round-tripping.
    non_finite: u64,
    frequent: HeavyHitters,
    /// The lexicographic extremes, kept whole so comparisons stay exact;
    /// only the reported summary truncates them.
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
            kinds: [0; 9],
            distinct: None,
            numeric: Moments::new(),
            quantiles: TDigest::new(config.digest_compression),
            text_length: Moments::new(),
            non_finite: 0,
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
        let max_len = self.config.sample_max_len;
        self.frequent.observe(hash, || value.display(max_len));
        match &mut self.distinct {
            Some(hll) => hll.insert_hash(hash),
            None if self.frequent.len() >= self.frequent.capacity() => {
                self.distinct = Some(Box::new(sketch_of(self.frequent.hashes())));
            }
            None => {}
        }
        if let Some(x) = value.as_f64() {
            if x.is_finite() {
                self.numeric.add(x);
                self.quantiles.add(x);
            } else {
                self.non_finite += 1;
            }
        }
        if let ProfileValue::Str(s) | ProfileValue::Ref(s) | ProfileValue::Other(s) = value {
            self.text_length.add(s.chars().count() as f64);
            if self.min_text.as_deref().is_none_or(|m| s < m) {
                self.min_text = Some(s.to_string());
            }
            if self.max_text.as_deref().is_none_or(|m| s > m) {
                self.max_text = Some(s.to_string());
            }
        }
    }

    /// Fold another profile of the same column in.
    pub fn merge(&mut self, other: &ColumnProfile) {
        // The sketch must exist before the frequent-value merge trims,
        // since a trimmed counter's hash is gone. Two exact tables that
        // fit together stay exact and need none.
        let could_trim = self.frequent.len() + other.frequent.len() > self.frequent.capacity();
        if self.distinct.is_some() || other.distinct.is_some() || could_trim {
            let mut hll = self
                .distinct
                .take()
                .unwrap_or_else(|| Box::new(sketch_of(self.frequent.hashes())));
            match &other.distinct {
                Some(theirs) => hll.merge(theirs),
                None => {
                    for hash in other.frequent.hashes() {
                        hll.insert_hash(hash);
                    }
                }
            }
            self.distinct = Some(hll);
        }
        self.count += other.count;
        for (mine, theirs) in self.kinds.iter_mut().zip(&other.kinds) {
            *mine += theirs;
        }
        self.numeric.merge(&other.numeric);
        self.quantiles.merge(&other.quantiles);
        self.text_length.merge(&other.text_length);
        self.non_finite += other.non_finite;
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
            .or_else(|| self.distinct.as_ref().map(|h| h.estimate()))
            .unwrap_or(0)
    }

    /// Whether the cardinality sketch has been allocated: only once the
    /// frequent-value table has filled.
    pub fn has_sketch(&self) -> bool {
        self.distinct.is_some()
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

    /// Numeric cells that were infinite or NaN, and so were left out of
    /// the moments and the digest.
    pub fn non_finite_count(&self) -> u64 {
        self.non_finite
    }

    /// Whether every non-null value is the same value.
    pub fn is_constant(&self) -> bool {
        self.non_null_count() > 0 && self.distinct_exact() == Some(1)
    }

    /// Whether any numeric cell was a number rather than a point in time.
    fn has_summable_kind(&self) -> bool {
        self.kind_count(ValueKind::Int) > 0 || self.kind_count(ValueKind::Float) > 0
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
        let distinct_error = Hll4096::typical_error();
        // One compression for the seven quantiles below: `quantile`
        // would otherwise clone and compress the digest on each call,
        // and a grouped profile summarises every group.
        let quantiles = self.quantiles.compressed();
        let numeric = (self.numeric.count() > 0).then(|| NumericSummary {
            count: self.numeric.count(),
            min: self.numeric.min().unwrap_or(0.0),
            max: self.numeric.max().unwrap_or(0.0),
            mean: self.numeric.mean().and_then(finite),
            stddev: self.numeric.stddev().and_then(finite),
            sum: self
                .has_summable_kind()
                .then(|| self.numeric.sum())
                .and_then(finite),
            p01: quantiles.quantile(0.01).and_then(finite),
            p05: quantiles.quantile(0.05).and_then(finite),
            p25: quantiles.quantile(0.25).and_then(finite),
            p50: quantiles.quantile(0.50).and_then(finite),
            p75: quantiles.quantile(0.75).and_then(finite),
            p95: quantiles.quantile(0.95).and_then(finite),
            p99: quantiles.quantile(0.99).and_then(finite),
        });
        let sample_max_len = self.config.sample_max_len;
        let text = (self.text_length.count() > 0).then(|| TextSummary {
            min_length: self.text_length.min().unwrap_or(0.0) as u64,
            max_length: self.text_length.max().unwrap_or(0.0) as u64,
            mean_length: self.text_length.mean().unwrap_or(0.0),
            min: self.min_text.clone().map(|s| truncate(s, sample_max_len)),
            max: self.max_text.clone().map(|s| truncate(s, sample_max_len)),
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
            distinct_error,
            uniqueness,
            is_constant: self.is_constant(),
            key_candidate: self.count > 0
                && self.null_count() == 0
                && uniqueness >= 1.0 - 2.0 * distinct_error,
            kinds,
            non_finite: self.non_finite,
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
    /// Observed extremes, so always finite: `observe` turns away
    /// non-finite input before it reaches the moments.
    pub min: f64,
    pub max: f64,
    /// `None` when the running mean overflowed. Welford's increment is
    /// `mean += (x - mean) / n`, and `x - mean` can overflow when the
    /// column mixes extreme magnitudes of opposite sign.
    pub mean: Option<f64>,
    /// `None` when there are fewer than two values, or when the sum of
    /// squares overflowed.
    pub stddev: Option<f64>,
    /// `None` when the running total overflowed (two cells at `1e308`
    /// are enough, from wholly finite input), and `None` for a column
    /// whose only numeric kind is temporal: a total of epoch
    /// milliseconds means nothing, while its mean, extremes and
    /// quantiles are still dates.
    pub sum: Option<f64>,
    pub p01: Option<f64>,
    pub p05: Option<f64>,
    pub p25: Option<f64>,
    pub p50: Option<f64>,
    pub p75: Option<f64>,
    pub p95: Option<f64>,
    pub p99: Option<f64>,
}

/// Lengths are in characters, not bytes.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TextSummary {
    pub min_length: u64,
    pub max_length: u64,
    pub mean_length: f64,
    /// Lexicographically smallest value, truncated to the sample length
    /// for display only; the comparison used the whole value.
    pub min: Option<String>,
    pub max: Option<String>,
}

/// A fresh sketch over the given hashes.
fn sketch_of(hashes: impl Iterator<Item = u64>) -> Hll4096 {
    let mut hll = Hll4096::new();
    for hash in hashes {
        hll.insert_hash(hash);
    }
    hll
}

fn is_zero(n: &u64) -> bool {
    *n == 0
}

/// Keep a derived float only when it is finite.
///
/// `observe` already turns away non-finite input, but a total or a sum
/// of squares over finite cells can still overflow — two cells at
/// `1e308` overflow both. A non-finite `f64` serialises as JSON `null`
/// and only reads back into an `Option`, so every derived float the
/// summary reports goes through here and is absent rather than
/// unreadable.
pub(crate) fn finite(x: f64) -> Option<f64> {
    x.is_finite().then_some(x)
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
    /// No nulls, and distinct within two sigma of the row count: the
    /// sketch could not disprove a key. Not proof of one. A column with
    /// fewer duplicates than twice `distinct_error` (about 3% at the
    /// default sketch size) passes here; only an exact probe can
    /// confirm it. Exact when `distinct_is_exact`.
    pub key_candidate: bool,
    pub kinds: BTreeMap<ValueKind, u64>,
    /// Numeric cells that were infinite or NaN. They are counted here
    /// and in `kinds`, and excluded from `numeric`.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub non_finite: u64,
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
    fn non_finite_floats_stay_out_of_the_numeric_summary() {
        // A Parquet double column can carry these; one of them used to
        // poison sum/mean/min/max into JSON `null`, which no longer
        // reads back as an `f64`.
        let mut p = ColumnProfile::default();
        p.observe(ProfileValue::Float(1.0));
        p.observe(ProfileValue::Float(3.0));
        p.observe(ProfileValue::Float(f64::INFINITY));
        p.observe(ProfileValue::Float(f64::NEG_INFINITY));
        p.observe(ProfileValue::Float(f64::NAN));
        p.observe(ProfileValue::Float(1e308));
        p.observe(ProfileValue::Float(1e308));

        let s = p.summary();
        assert_eq!(s.count, 7);
        assert_eq!(s.non_finite, 3);
        assert_eq!(s.kinds.get(&ValueKind::Float), Some(&7));

        let n = s.numeric.as_ref().unwrap();
        assert_eq!(n.count, 4);
        assert_eq!(n.mean, Some(5e307));
        assert!(n.min.is_finite() && n.max.is_finite());
        assert_eq!(n.min, 1.0);
        assert_eq!(n.max, 1e308);
        // Two 1e308 cells overflow the running total. That is finite
        // input, so the ingest guard cannot catch it; the summary
        // reports no sum rather than an unreadable one.
        assert_eq!(n.sum, None);

        let json = serde_json::to_string(&s).unwrap();
        let back: ColumnSummary = serde_json::from_str(&json).unwrap();
        assert_eq!(back, s);
    }

    #[test]
    fn extreme_mixed_magnitudes_still_round_trip() {
        // Opposite-signed extremes are the case that can overflow a
        // centroid interpolation as well as the total.
        let mut p = ColumnProfile::default();
        for _ in 0..50 {
            p.observe(ProfileValue::Float(f64::MAX));
            p.observe(ProfileValue::Float(f64::MIN));
            p.observe(ProfileValue::Float(0.0));
        }
        let s = p.summary();
        let json = serde_json::to_string(&s).unwrap();
        let back: ColumnSummary = serde_json::from_str(&json).unwrap();
        assert_eq!(back, s);
    }

    #[test]
    fn non_finite_counts_survive_a_merge() {
        let mut a = ColumnProfile::default();
        a.observe(ProfileValue::Float(f64::NAN));
        a.observe(ProfileValue::Float(2.0));
        let mut b = ColumnProfile::default();
        b.observe(ProfileValue::Float(f64::INFINITY));
        b.observe(ProfileValue::Float(4.0));
        a.merge(&b);
        assert_eq!(a.non_finite_count(), 2);
        let n = a.summary().numeric.unwrap();
        assert_eq!(n.count, 2);
        assert_eq!(n.mean, Some(3.0));
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
        assert!((n.mean.unwrap() - 500.5).abs() < 1e-9);
        assert!((n.p50.unwrap() - 500.0).abs() < 10.0);
        assert_eq!(s.kinds.get(&ValueKind::Str), Some(&1));
        assert_eq!(s.kinds.get(&ValueKind::Float), Some(&1000));
    }

    #[test]
    fn temporal_only_column_has_no_sum() {
        let mut p = ColumnProfile::default();
        p.observe(ProfileValue::Temporal(86_400_000));
        p.observe(ProfileValue::Temporal(172_800_000));
        let n = p.summary().numeric.unwrap();
        assert_eq!(n.sum, None);
        assert_eq!(n.mean, Some(129_600_000.0));
        assert_eq!(n.max, 172_800_000.0);
        assert!(n.p50.is_some());
        // One number among the dates and the total is reported again.
        p.observe(ProfileValue::Int(1));
        assert_eq!(p.summary().numeric.unwrap().sum, Some(259_200_001.0));
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
            (sa.numeric.as_ref().unwrap().mean.unwrap()
                - sw.numeric.as_ref().unwrap().mean.unwrap())
            .abs()
                < 1e-9
        );
        let d = sa.distinct as f64 - sw.distinct as f64;
        assert!(d.abs() / (sw.distinct as f64) < 0.05);
    }

    #[test]
    fn text_extremes_compare_whole_values_and_lengths_are_chars() {
        let mut p = ColumnProfile::new(ProfileConfig {
            sample_max_len: 8,
            ..ProfileConfig::default()
        });
        // Both share a prefix longer than the sample; a truncated compare
        // would call them equal and keep whichever came first.
        p.observe(ProfileValue::Str("prefix-prefix-zzz"));
        p.observe(ProfileValue::Str("prefix-prefix-aaa"));
        p.observe(ProfileValue::Str("héllo"));
        let t = p.summary().text.unwrap();
        assert_eq!(t.min.as_deref(), Some("héllo"));
        assert_eq!(t.max.as_deref(), Some("prefix-p…"));
        assert_eq!(t.min_length, 5, "characters, not bytes");
        let mut other = ColumnProfile::new(p.config());
        other.observe(ProfileValue::Str("prefix-prefix-zzzz"));
        p.merge(&other);
        assert_eq!(p.max_text.as_deref(), Some("prefix-prefix-zzzz"));
    }

    #[test]
    fn sketch_is_allocated_only_when_the_table_fills() {
        let cap = ProfileConfig::default().top_capacity;
        let mut p = ColumnProfile::default();
        for i in 0..cap - 1 {
            p.observe(ProfileValue::Int(i as i64));
        }
        assert!(!p.has_sketch());
        assert_eq!(p.distinct_estimate(), (cap - 1) as u64);
        p.observe(ProfileValue::Int((cap - 1) as i64));
        assert!(
            p.has_sketch(),
            "allocated on the value that fills the table"
        );
        assert!(
            p.summary().distinct_is_exact,
            "and still exact until an eviction"
        );
    }

    #[test]
    fn lazy_sketch_matches_an_eager_one_register_for_register() {
        // Every hash the profile has seen must be in the sketch, the
        // first `capacity` by replay and the rest directly, so its
        // registers equal a sketch that saw them all as they came.
        let mut p = ColumnProfile::default();
        let mut eager = Hll4096::new();
        for i in 0..20_000 {
            let s = format!("v{}", i % 7_000);
            let v = ProfileValue::Str(&s);
            p.observe(v);
            eager.insert_hash(value_hash(&v).unwrap());
        }
        assert_eq!(p.distinct.as_deref(), Some(&eager));
        assert_eq!(p.distinct_estimate(), eager.estimate());
    }

    #[test]
    fn merge_allocates_a_sketch_only_when_the_union_could_trim() {
        let profile_over = |lo: i64, hi: i64| {
            let mut p = ColumnProfile::default();
            for i in lo..hi {
                p.observe(ProfileValue::Int(i));
            }
            p
        };
        // Two small exact tables that fit together: still exact, no sketch.
        let mut a = profile_over(0, 20);
        a.merge(&profile_over(10, 40));
        assert!(!a.has_sketch());
        assert_eq!(a.distinct_estimate(), 40);

        // Two exact tables whose union overflows: the sketch is built
        // from both counter sets before the trim, so no hash is lost.
        let mut b = profile_over(0, 50);
        b.merge(&profile_over(30, 100));
        assert!(b.has_sketch());
        let mut whole = Hll4096::new();
        for i in 0..100 {
            whole.insert_hash(value_hash(&ProfileValue::Int(i)).unwrap());
        }
        assert_eq!(b.distinct.as_deref(), Some(&whole));

        // Sketch on one side, exact table on the other.
        let mut c = profile_over(0, 5_000);
        c.merge(&profile_over(4_990, 5_010));
        let mut whole = Hll4096::new();
        for i in 0..5_010 {
            whole.insert_hash(value_hash(&ProfileValue::Int(i)).unwrap());
        }
        assert_eq!(c.distinct.as_deref(), Some(&whole));
        let mut d = profile_over(4_990, 5_010);
        d.merge(&profile_over(0, 5_000));
        assert_eq!(d.distinct.as_deref(), Some(&whole));
    }

    #[test]
    fn bytes_hash_on_content() {
        let mut p = ColumnProfile::default();
        p.observe(ProfileValue::Bytes(&[1, 2, 3]));
        p.observe(ProfileValue::Bytes(&[1, 2, 3]));
        p.observe(ProfileValue::Bytes(&[9]));
        let s = p.summary();
        assert_eq!(s.distinct, 2);
        assert!(!s.is_constant);
        assert_eq!(s.kinds.get(&ValueKind::Bytes), Some(&3));
        assert_eq!(s.top_values[0].value, "0x010203");
        assert!(s.text.is_none());
    }

    #[test]
    fn text_only_profile_round_trips_json() {
        let mut p = ColumnProfile::default();
        p.observe(ProfileValue::Str("a"));
        let back: ColumnProfile =
            serde_json::from_str(&serde_json::to_string(&p).unwrap()).unwrap();
        assert_eq!(back, p);
        assert!(back.summary().numeric.is_none());
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
