//! Perf regression budgets for `vbench compare --gate`.
//!
//! Modeled on `fluree-bench-support::budget::RegressionBudget` (a
//! `default_budget_pct` + explicit overrides), but keyed by **query id** with
//! **tag-level defaults** split by gating class: native queries get a tighter
//! budget than virtual (which carries live-Snowflake variance). Cold runs are
//! advisory-only and never gate. `budgets.json` lives at the crate root.
//!
//! A violation requires **both** conditions: the observed wall exceeds the
//! baseline by more than the class/override percent **and** by at least
//! `min_delta_ms` absolute. Percentage-only gating turns micro-query noise
//! into red (10% of an 8 ms baseline is a 0.8 ms tripwire — exactly what
//! PR-0's validation tripped over); the absolute floor makes the gate
//! meaningful on single-machine medians.

use std::collections::BTreeMap;
use std::path::Path;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

/// The `budgets.json` document.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Budgets {
    #[serde(default = "one")]
    pub schema_version: u32,
    /// Tag-level default budgets by gating class.
    pub default_budget_pct: DefaultBudgets,
    /// Absolute floor: a record must also be at least this many ms over its
    /// baseline to violate, so a percent of a tiny baseline can't gate on
    /// scheduler noise. Trade-off: a regression whose absolute cost stays
    /// under the floor (e.g. a 2 ms query slipping to 40 ms) is not flagged —
    /// tighten with a per-query percent override if that ever matters.
    #[serde(default = "default_min_delta_ms")]
    pub min_delta_ms: u64,
    /// Documents that cold runs are advisory-only (they never gate regardless).
    #[serde(default)]
    pub cold: String,
    /// Per-query budget overrides (query id → percent) — win over the defaults.
    #[serde(default)]
    pub overrides: BTreeMap<String, f64>,
}

/// The gate applied to one record: percent over baseline **and** at least
/// `min_delta_ms` absolute slowdown, both required for a violation.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct EffectiveBudget {
    pub pct: f64,
    pub min_delta_ms: u64,
}

/// Tag-level defaults, split by gating class.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DefaultBudgets {
    /// Allowed % slowdown for a native hot wall.
    pub native: f64,
    /// Allowed % slowdown for a virtual hot wall.
    pub virtual_hot: f64,
}

fn one() -> u32 {
    1
}

fn default_min_delta_ms() -> u64 {
    50
}

impl Default for Budgets {
    fn default() -> Self {
        Self {
            schema_version: 1,
            default_budget_pct: DefaultBudgets {
                native: 10.0,
                virtual_hot: 20.0,
            },
            min_delta_ms: default_min_delta_ms(),
            cold: "advisory".to_string(),
            overrides: BTreeMap::new(),
        }
    }
}

impl Budgets {
    /// Load `budgets.json`, or fall back to the built-in defaults if absent.
    pub fn load(path: &Path) -> Result<Self> {
        if !path.exists() {
            return Ok(Self::default());
        }
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("reading budgets {}", path.display()))?;
        serde_json::from_str(&raw).with_context(|| format!("parsing budgets {}", path.display()))
    }

    /// The budget that gates a query, or `None` when the record is advisory
    /// (cold) and must never gate. A per-query override wins over the class
    /// default; the absolute floor applies uniformly.
    pub fn budget(&self, query_id: &str, is_virtual: bool, cold: bool) -> Option<EffectiveBudget> {
        if cold {
            return None;
        }
        let pct = self
            .overrides
            .get(query_id)
            .copied()
            .unwrap_or(if is_virtual {
                self.default_budget_pct.virtual_hot
            } else {
                self.default_budget_pct.native
            });
        Some(EffectiveBudget {
            pct,
            min_delta_ms: self.min_delta_ms,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shipped_budgets_parse() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("budgets.json");
        let b = Budgets::load(&path).expect("budgets.json must parse");
        assert_eq!(b.default_budget_pct.native, 10.0);
        assert_eq!(b.default_budget_pct.virtual_hot, 20.0);
        assert_eq!(b.min_delta_ms, 50, "shipped absolute floor");
    }

    #[test]
    fn budget_lookup_class_and_override_and_cold() {
        let mut b = Budgets::default();
        assert_eq!(b.budget("q001", false, false).unwrap().pct, 10.0);
        assert_eq!(b.budget("q001", true, false).unwrap().pct, 20.0);
        // Cold is advisory — never gates.
        assert_eq!(b.budget("q001", true, true), None);
        // Override wins on the percent; the floor applies uniformly.
        b.overrides.insert("q015".to_string(), 50.0);
        let eff = b.budget("q015", true, false).unwrap();
        assert_eq!(eff.pct, 50.0);
        assert_eq!(eff.min_delta_ms, b.min_delta_ms);
    }
}
