//! Statistics a quality rule evaluates, derived from profiles.
//!
//! Pure functions over [`ColumnProfile`] and [`GroupedProfile`]; policy
//! (which threshold, which severity, what to say) stays with the caller.

use serde::{Deserialize, Serialize};

use crate::grouped::GroupedProfile;
use crate::profile::{finite, ColumnProfile};

/// A group's numeric baseline.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupBaseline {
    pub key: String,
    pub count: u64,
    pub mean: f64,
    pub median: f64,
    pub stddev: Option<f64>,
    pub min: f64,
    pub max: f64,
}

/// Which centre a ratio rule measures against. The median is robust to
/// the very outliers the rule hunts; the mean is what most people quote.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Centre {
    Mean,
    Median,
}

/// "More than `ratio` times the group's centre" — the shape of Magna's
/// suspect-high-price rule.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RatioRule {
    pub centre: Centre,
    pub ratio: f64,
    /// Groups smaller than this have no baseline worth trusting.
    pub min_count: u64,
}

impl RatioRule {
    /// Whether `x` breaks the rule against `baseline`.
    pub fn exceeds(&self, x: f64, baseline: &GroupBaseline) -> bool {
        if baseline.count < self.min_count {
            return false;
        }
        let centre = match self.centre {
            Centre::Mean => baseline.mean,
            Centre::Median => baseline.median,
        };
        centre > 0.0 && x > self.ratio * centre
    }
}

/// "More than `threshold` standard deviations from the group mean".
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ZScoreRule {
    pub threshold: f64,
    pub min_count: u64,
}

impl ZScoreRule {
    pub fn z(&self, x: f64, baseline: &GroupBaseline) -> Option<f64> {
        if baseline.count < self.min_count {
            return None;
        }
        let sd = baseline.stddev?;
        (sd > 0.0).then(|| (x - baseline.mean) / sd)
    }

    pub fn exceeds(&self, x: f64, baseline: &GroupBaseline) -> bool {
        self.z(x, baseline)
            .is_some_and(|z| z.abs() > self.threshold)
    }
}

fn baseline_of(key: &str, p: &ColumnProfile) -> Option<GroupBaseline> {
    let n = p.numeric();
    // A group whose mean or median overflowed has no baseline worth
    // comparing against — every `RatioRule` against a NaN centre
    // answers false — so it is omitted like a group with no numeric
    // values at all, rather than reported as an unreadable one.
    Some(GroupBaseline {
        key: key.to_string(),
        count: n.count(),
        mean: finite(n.mean()?)?,
        median: finite(p.quantiles().median()?)?,
        stddev: n.stddev().and_then(finite),
        min: n.min()?,
        max: n.max()?,
    })
}

/// The numeric baseline of every kept group, largest first. Groups with
/// no numeric values are omitted.
pub fn group_baselines(g: &GroupedProfile) -> Vec<GroupBaseline> {
    let mut out: Vec<GroupBaseline> = g.groups().filter_map(|(k, p)| baseline_of(k, p)).collect();
    out.sort_by(|a, b| b.count.cmp(&a.count).then_with(|| a.key.cmp(&b.key)));
    out
}

/// The baseline of one group.
pub fn group_baseline(g: &GroupedProfile, key: &str) -> Option<GroupBaseline> {
    g.group(key).and_then(|p| baseline_of(key, p))
}

/// How much of a column's non-null cells its most frequent values take.
/// One value carrying a large share of a reference column is the
/// catch-all-record finding.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Concentration {
    pub value: String,
    pub count: u64,
    pub share: f64,
}

pub fn concentration(p: &ColumnProfile, top_n: usize) -> Vec<Concentration> {
    let non_null = p.non_null_count();
    p.frequent()
        .top(top_n)
        .into_iter()
        .map(|h| Concentration {
            value: h.value,
            count: h.count,
            share: if non_null == 0 {
                0.0
            } else {
                h.count as f64 / non_null as f64
            },
        })
        .collect()
}

/// Whether a column is constant within each group: how many groups have
/// exactly one distinct value. A price that never varies within a part
/// is a unit price; a weight that never varies within a part is a
/// property of the part, not the record.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConstancyPerGroup {
    pub groups: u64,
    /// Groups with at least two non-null observations, the only ones
    /// that can show variation.
    pub groups_with_repeats: u64,
    pub constant_groups: u64,
    /// `constant_groups / groups_with_repeats`; `None` when no group repeats.
    pub fraction: Option<f64>,
}

pub fn constancy_per_group(g: &GroupedProfile) -> ConstancyPerGroup {
    let mut with_repeats = 0u64;
    let mut constant = 0u64;
    for (_, p) in g.groups() {
        if p.non_null_count() >= 2 {
            with_repeats += 1;
            if p.is_constant() {
                constant += 1;
            }
        }
    }
    ConstancyPerGroup {
        groups: g.group_count() as u64,
        groups_with_repeats: with_repeats,
        constant_groups: constant,
        fraction: (with_repeats > 0).then(|| constant as f64 / with_repeats as f64),
    }
}

/// A group whose median sits far from the column's overall median: the
/// division whose prices run at a tenth of everyone else's.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScaleDrift {
    pub key: String,
    pub count: u64,
    pub median: f64,
    pub overall_median: f64,
    /// `median / overall_median`.
    pub ratio: f64,
}

/// How far a ratio sits from one, on a log scale, for ordering only.
///
/// The rule is defined for positive-scale columns, so a group whose
/// median is zero or negative has no log ratio. Such a group is still
/// reported — a group at -5 against an overall 60 is the anomaly the
/// rule exists to surface — and sorts first as maximally drifted.
fn drift_distance(ratio: f64) -> f64 {
    if ratio > 0.0 {
        ratio.ln().abs()
    } else {
        f64::INFINITY
    }
}

/// Every kept group's median against the whole column's, sorted by how
/// far the ratio is from one.
pub fn scale_drift(g: &GroupedProfile) -> Vec<ScaleDrift> {
    let Some(overall) = g.total().quantiles().median() else {
        return Vec::new();
    };
    if overall <= 0.0 {
        return Vec::new();
    }
    let mut out: Vec<ScaleDrift> = g
        .groups()
        .filter_map(|(k, p)| {
            let median = p.quantiles().median()?;
            Some(ScaleDrift {
                key: k.to_string(),
                count: p.numeric().count(),
                median,
                overall_median: overall,
                ratio: median / overall,
            })
        })
        .collect();
    // `total_cmp` on a precomputed distance: `partial_cmp` on a raw
    // `ln` would return `None` for a non-positive ratio, and a
    // comparator that answers `Equal` to a NaN is not a total order,
    // which makes `sort_by` panic rather than merely misorder.
    out.sort_by(|a, b| {
        drift_distance(b.ratio)
            .total_cmp(&drift_distance(a.ratio))
            .then_with(|| a.key.cmp(&b.key))
    });
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::profile::{ProfileConfig, ProfileValue};

    fn magna_like() -> GroupedProfile {
        // Three divisions at ~$500 and one at ~$50, eleven lines per part,
        // one wild line in Rome.
        let mut g = GroupedProfile::new(ProfileConfig::default(), 100);
        for div in ["rome", "hollywood", "dorchester"] {
            for i in 0..200 {
                g.observe(div, ProfileValue::Float(450.0 + f64::from(i % 11) * 10.0));
            }
        }
        for i in 0..200 {
            g.observe("montreal", ProfileValue::Float(45.0 + f64::from(i % 11)));
        }
        g.observe("rome", ProfileValue::Float(55_809.0));
        g
    }

    #[test]
    fn ratio_rule_flags_the_wild_line() {
        let g = magna_like();
        let rome = group_baseline(&g, "rome").unwrap();
        let rule = RatioRule {
            centre: Centre::Median,
            ratio: 5.0,
            min_count: 3,
        };
        assert!(rule.exceeds(55_809.0, &rome));
        assert!(!rule.exceeds(520.0, &rome));
        // The mean is dragged up by the outlier but still flags it.
        let mean_rule = RatioRule {
            centre: Centre::Mean,
            ..rule
        };
        assert!(mean_rule.exceeds(55_809.0, &rome));
    }

    #[test]
    fn small_groups_have_no_baseline() {
        let mut g = GroupedProfile::default();
        g.observe("k", ProfileValue::Float(1.0));
        let b = group_baseline(&g, "k").unwrap();
        let rule = RatioRule {
            centre: Centre::Mean,
            ratio: 5.0,
            min_count: 3,
        };
        assert!(!rule.exceeds(100.0, &b));
    }

    #[test]
    fn z_score_rule() {
        let g = magna_like();
        let h = group_baseline(&g, "hollywood").unwrap();
        let rule = ZScoreRule {
            threshold: 3.0,
            min_count: 10,
        };
        assert!(rule.exceeds(5_000.0, &h));
        assert!(!rule.exceeds(500.0, &h));
    }

    #[test]
    fn scale_drift_finds_montreal() {
        let drift = scale_drift(&magna_like());
        assert_eq!(drift[0].key, "montreal");
        assert!(drift[0].ratio < 0.2, "{:?}", drift[0]);
    }

    #[test]
    fn scale_drift_keeps_negative_median_groups_first() {
        // A negative group median gives `ratio < 0`, whose `ln` is NaN.
        // Ordering through that NaN is not a total order and `sort_by`
        // panics on it; the group is also the one worth reporting.
        let mut g = GroupedProfile::new(ProfileConfig::default(), 1_000);
        for i in 0..200 {
            let key = format!("g-{i}");
            let value = if i % 3 == 0 {
                -10.0 - f64::from(i)
            } else {
                10.0 + f64::from(i)
            };
            for _ in 0..5 {
                g.observe(&key, ProfileValue::Float(value));
            }
        }

        let drift = scale_drift(&g);
        assert_eq!(drift.len(), 200);

        let negatives = drift.iter().filter(|d| d.median < 0.0).count();
        assert_eq!(negatives, 67);
        assert!(
            drift[..negatives].iter().all(|d| d.median < 0.0),
            "negative-median groups sort first: {:?}",
            &drift[..negatives.min(3)]
        );
        // The rest stay ordered by distance from the overall median.
        let tail: Vec<f64> = drift[negatives..]
            .iter()
            .map(|d| drift_distance(d.ratio))
            .collect();
        assert!(tail.windows(2).all(|w| w[0] >= w[1]), "{tail:?}");
    }

    #[test]
    fn constancy_detects_unit_price() {
        let mut g = GroupedProfile::default();
        for part in 0..100 {
            let price = 10.0 + f64::from(part);
            for _ in 0..5 {
                g.observe(&format!("part-{part}"), ProfileValue::Float(price));
            }
        }
        for _ in 0..5 {
            g.observe("part-varies", ProfileValue::Float(1.0));
        }
        g.observe("part-varies", ProfileValue::Float(2.0));
        let c = constancy_per_group(&g);
        assert_eq!(c.groups, 101);
        assert_eq!(c.constant_groups, 100);
        assert!(c.fraction.unwrap() > 0.98);
    }

    #[test]
    fn concentration_finds_the_catch_all() {
        let mut p = ColumnProfile::default();
        for i in 0..10_000u32 {
            if i % 3 == 0 {
                p.observe(ProfileValue::Ref("PRT-166104"));
            } else {
                let s = format!("PRT-{i}");
                p.observe(ProfileValue::Ref(&s));
            }
        }
        let c = concentration(&p, 1);
        assert_eq!(c[0].value, "PRT-166104");
        assert!(c[0].share > 0.3, "{:?}", c[0]);
    }
}
