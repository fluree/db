//! One profile per group key, with a bounded number of groups.
//!
//! This is the per-(part, division) baseline: the same column profiled
//! separately for every value of a grouping key, so a rule can ask "is
//! this price far from what this part usually costs in this division".
//! Groups are capped; once the cap is hit, further keys pool into one
//! overflow profile and the number of keys that spilled is estimated, so
//! the caller can see the cap was hit rather than silently losing groups.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::hash::group_hash;
use crate::hll::Hll4096;
use crate::profile::{ColumnProfile, ColumnSummary, ProfileConfig, ProfileValue};

/// Groups kept when a caller does not choose.
///
/// A kept group is a whole [`ColumnProfile`]: about a kilobyte while it
/// holds a handful of values, growing to some twenty kilobytes once it
/// has seen enough distinct values to allocate its cardinality sketch
/// and enough values to fill its digest. Ten thousand small groups is
/// therefore about ten megabytes per profiled column, ten thousand
/// large ones two hundred, and a caller profiling several columns pays
/// it per column. Raise `max_groups` deliberately, against that
/// arithmetic; keys beyond the cap are not lost, they pool into
/// `overflow` and are counted.
pub const DEFAULT_MAX_GROUPS: usize = 10_000;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GroupedProfile {
    config: ProfileConfig,
    max_groups: usize,
    /// Keyed on the key text itself, so two keys are the same group only
    /// when they are the same string; the overflow sketch is the one
    /// place a key is reduced to a hash.
    groups: HashMap<String, ColumnProfile>,
    /// Everything observed under keys that arrived after the cap.
    overflow: ColumnProfile,
    /// Distinct keys that spilled into `overflow`.
    overflow_keys: Hll4096,
    /// The column as a whole, regardless of group.
    total: ColumnProfile,
    /// Cells observed with no group key (the subject carried none).
    ungrouped: u64,
}

impl Default for GroupedProfile {
    fn default() -> Self {
        Self::new(ProfileConfig::default(), DEFAULT_MAX_GROUPS)
    }
}

impl GroupedProfile {
    pub fn new(config: ProfileConfig, max_groups: usize) -> Self {
        Self {
            config,
            max_groups: max_groups.max(1),
            groups: HashMap::new(),
            overflow: ColumnProfile::new(config),
            overflow_keys: Hll4096::new(),
            total: ColumnProfile::new(config),
            ungrouped: 0,
        }
    }

    /// Record one cell that belongs to no group: it counts toward the
    /// column as a whole and nowhere else.
    pub fn observe_ungrouped(&mut self, value: ProfileValue<'_>) {
        self.total.observe(value);
        self.ungrouped += 1;
    }

    /// Cells observed with no group key.
    pub fn ungrouped_count(&self) -> u64 {
        self.ungrouped
    }

    /// Record one cell under `key`.
    pub fn observe(&mut self, key: &str, value: ProfileValue<'_>) {
        self.total.observe(value);
        if let Some(profile) = self.groups.get_mut(key) {
            profile.observe(value);
            return;
        }
        if self.groups.len() < self.max_groups {
            let mut profile = ColumnProfile::new(self.config);
            profile.observe(value);
            self.groups.insert(key.to_string(), profile);
            return;
        }
        self.overflow_keys.insert_hash(group_hash(key));
        self.overflow.observe(value);
    }

    /// Fold another grouped profile in. Groups the merge cannot keep
    /// within the cap spill into overflow, largest groups kept.
    pub fn merge(&mut self, other: &GroupedProfile) {
        self.total.merge(&other.total);
        self.ungrouped += other.ungrouped;
        self.overflow.merge(&other.overflow);
        self.overflow_keys.merge(&other.overflow_keys);
        for (key, theirs) in &other.groups {
            match self.groups.get_mut(key) {
                Some(mine) => mine.merge(theirs),
                None => {
                    self.groups.insert(key.clone(), theirs.clone());
                }
            }
        }
        if self.groups.len() > self.max_groups {
            let mut by_size: Vec<(u64, &str)> = self
                .groups
                .iter()
                .map(|(key, p)| (p.count(), key.as_str()))
                .collect();
            by_size.sort_unstable_by(|a, b| b.cmp(a));
            let spilled: Vec<String> = by_size
                .into_iter()
                .skip(self.max_groups)
                .map(|(_, key)| key.to_string())
                .collect();
            for key in spilled {
                if let Some(p) = self.groups.remove(&key) {
                    self.overflow.merge(&p);
                    self.overflow_keys.insert_hash(group_hash(&key));
                }
            }
        }
    }

    /// The whole column, all groups together.
    pub fn total(&self) -> &ColumnProfile {
        &self.total
    }

    /// The profile for one key, if it was kept.
    pub fn group(&self, key: &str) -> Option<&ColumnProfile> {
        self.groups.get(key)
    }

    /// Every kept group as `(key, profile)`, in no particular order.
    pub fn groups(&self) -> impl Iterator<Item = (&str, &ColumnProfile)> {
        self.groups.iter().map(|(k, p)| (k.as_str(), p))
    }

    pub fn group_count(&self) -> usize {
        self.groups.len()
    }

    /// Whether any key spilled past the cap.
    pub fn overflowed(&self) -> bool {
        self.overflow.count() > 0
    }

    /// Estimated number of keys that spilled.
    pub fn overflow_key_estimate(&self) -> u64 {
        self.overflow_keys.estimate()
    }

    pub fn overflow(&self) -> &ColumnProfile {
        &self.overflow
    }

    /// Reportable form: groups largest first.
    pub fn summary(&self) -> GroupedSummary {
        let mut groups: Vec<GroupSummary> = self
            .groups
            .iter()
            .map(|(key, p)| GroupSummary {
                key: key.clone(),
                summary: p.summary(),
            })
            .collect();
        groups.sort_by(|a, b| {
            b.summary
                .count
                .cmp(&a.summary.count)
                .then_with(|| a.key.cmp(&b.key))
        });
        GroupedSummary {
            total: self.total.summary(),
            group_count: self.groups.len() as u64,
            ungrouped: self.ungrouped,
            groups,
            overflow_keys: self.overflowed().then(|| self.overflow_key_estimate()),
            overflow: self.overflowed().then(|| self.overflow.summary()),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupSummary {
    pub key: String,
    pub summary: ColumnSummary,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupedSummary {
    pub total: ColumnSummary,
    pub group_count: u64,
    /// Cells whose subject carried no group key.
    pub ungrouped: u64,
    pub groups: Vec<GroupSummary>,
    /// Present when the group cap was hit: how many keys pooled.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub overflow_keys: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub overflow: Option<ColumnSummary>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn groups_profile_separately() {
        let mut g = GroupedProfile::default();
        for _ in 0..10 {
            g.observe("rome", ProfileValue::Float(100.0));
            g.observe("montreal", ProfileValue::Float(10.0));
        }
        assert_eq!(g.group_count(), 2);
        assert_eq!(g.group("rome").unwrap().numeric().mean(), Some(100.0));
        assert_eq!(g.group("montreal").unwrap().numeric().mean(), Some(10.0));
        assert_eq!(g.total().numeric().mean(), Some(55.0));
        assert!(!g.overflowed());
    }

    #[test]
    fn cap_spills_into_overflow_with_a_key_estimate() {
        let mut g = GroupedProfile::new(ProfileConfig::default(), 3);
        for i in 0..50 {
            let k = format!("k{i}");
            g.observe(&k, ProfileValue::Int(i));
        }
        assert_eq!(g.group_count(), 3);
        assert!(g.overflowed());
        assert_eq!(g.overflow().count(), 47);
        let est = g.overflow_key_estimate();
        assert!((44..=50).contains(&est), "est={est}");
        let s = g.summary();
        assert_eq!(s.overflow_keys, Some(est));
    }

    #[test]
    fn small_groups_carry_no_sketch() {
        let mut g = GroupedProfile::default();
        for i in 0..1_000 {
            g.observe(&format!("k{i}"), ProfileValue::Int(i % 5));
        }
        assert!(g.groups().all(|(_, p)| !p.has_sketch()));
        assert!(!g.total().has_sketch(), "five distinct values overall");
        for i in 0..200 {
            g.observe("wide", ProfileValue::Int(i));
        }
        assert!(g.group("wide").unwrap().has_sketch());
        assert!(g.total().has_sketch());
    }

    #[test]
    fn merge_keeps_largest_groups() {
        let mut a = GroupedProfile::new(ProfileConfig::default(), 2);
        let mut b = GroupedProfile::new(ProfileConfig::default(), 2);
        for _ in 0..5 {
            a.observe("big", ProfileValue::Int(1));
        }
        a.observe("small-a", ProfileValue::Int(1));
        for _ in 0..3 {
            b.observe("medium", ProfileValue::Int(1));
        }
        b.observe("small-b", ProfileValue::Int(1));
        a.merge(&b);
        assert_eq!(a.group_count(), 2);
        assert!(a.group("big").is_some());
        assert!(a.group("medium").is_some());
        assert_eq!(a.overflow().count(), 2);
        assert_eq!(a.total().count(), 10);
    }
}
