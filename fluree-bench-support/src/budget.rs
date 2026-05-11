//! Regression-budget loader and validator.
//!
//! Reads `regression-budget.json` at the workspace root, exposes the parsed
//! shape, and provides a `check()` helper that benches and CI can use to
//! confirm an observed wall-clock value is within budget.
//!
//! ## Schema (`regression-budget.json`)
//!
//! ```json
//! {
//!   "version": 1,
//!   "default_budget_pct": 5.0,
//!   "crates": {
//!     "fluree-db-api": {
//!       "insert_formats": {
//!         "tiny": 10.0,
//!         "small": 5.0
//!       }
//!     }
//!   }
//! }
//! ```
//!
//! - `version` — schema version. Bumped on incompatible changes.
//! - `default_budget_pct` — fallback for any (crate, bench, scale) not
//!   explicitly listed. Initial value is 5.0 (5% regression allowed).
//! - `crates.<crate>.<bench>.<scale>` — explicit budget percentage.
//!
//! ## Status (bench-1 scope)
//!
//! Loader and `check()` are functional; the workspace `[[bench]]` ↔ budget
//! reconciliation step (`validate_against_workspace`) is a stub that returns
//! `Ok(())`. The full reconciliation lands in `bench-5` (CI gate) once the
//! initial budgets are baselined. See the `// TODO(bench-5)` markers below.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::runtime::BenchScale;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegressionBudget {
    pub version: u32,
    #[serde(default = "default_budget_pct_default")]
    pub default_budget_pct: f64,
    #[serde(default)]
    pub crates: BTreeMap<String, BTreeMap<String, BTreeMap<String, f64>>>,
}

fn default_budget_pct_default() -> f64 {
    5.0
}

impl RegressionBudget {
    /// Build an empty budget that returns `default_budget_pct` for every key.
    /// Useful for tests and for the very first run before any budgets are
    /// committed.
    pub fn empty(default_budget_pct: f64) -> Self {
        Self {
            version: 1,
            default_budget_pct,
            crates: BTreeMap::new(),
        }
    }

    /// Look up the budget percentage for `(crate, bench, scale)`, falling
    /// back to `default_budget_pct` if any tier is missing.
    pub fn budget_pct(&self, crate_name: &str, bench: &str, scale: BenchScale) -> f64 {
        self.crates
            .get(crate_name)
            .and_then(|c| c.get(bench))
            .and_then(|b| b.get(scale.as_str()))
            .copied()
            .unwrap_or(self.default_budget_pct)
    }
}

/// Find the workspace root by walking up from the current dir looking for a
/// `Cargo.toml` that declares `[workspace]`.
pub fn workspace_root() -> Option<PathBuf> {
    let cwd = std::env::current_dir().ok()?;
    let mut current = cwd.as_path();
    loop {
        let manifest = current.join("Cargo.toml");
        if manifest.exists() {
            if let Ok(s) = std::fs::read_to_string(&manifest) {
                if s.contains("[workspace]") {
                    return Some(current.to_path_buf());
                }
            }
        }
        match current.parent() {
            Some(p) => current = p,
            None => return None,
        }
    }
}

/// Load `regression-budget.json` from the workspace root. Returns an empty
/// budget (default-only) if the file does not exist, so a brand-new
/// workspace can adopt the chassis before baselines are committed.
pub fn load() -> Result<RegressionBudget, BudgetError> {
    let root = workspace_root().ok_or(BudgetError::WorkspaceNotFound)?;
    load_from(&root.join("regression-budget.json"))
}

pub fn load_from(path: &Path) -> Result<RegressionBudget, BudgetError> {
    if !path.exists() {
        return Ok(RegressionBudget::empty(default_budget_pct_default()));
    }
    let raw = std::fs::read_to_string(path)
        .map_err(|e| BudgetError::Io(format!("read {}: {e}", path.display())))?;
    let parsed: RegressionBudget = serde_json::from_str(&raw)
        .map_err(|e| BudgetError::Parse(format!("parse {}: {e}", path.display())))?;
    Ok(parsed)
}

/// Compare an observed nanosecond reading to a baseline and a per-bench
/// budget. Returns `Ok(())` if `observed <= baseline * (1 + budget_pct/100)`,
/// otherwise `Err(BudgetViolation)`.
pub fn check(
    budget: &RegressionBudget,
    crate_name: &str,
    bench: &str,
    scale: BenchScale,
    baseline_ns: f64,
    observed_ns: f64,
) -> Result<(), BudgetViolation> {
    let pct = budget.budget_pct(crate_name, bench, scale);
    let allowed = baseline_ns * (1.0 + pct / 100.0);
    if observed_ns <= allowed {
        Ok(())
    } else {
        Err(BudgetViolation {
            crate_name: crate_name.into(),
            bench: bench.into(),
            scale: scale.as_str().into(),
            baseline_ns,
            observed_ns,
            budget_pct: pct,
            allowed_ns: allowed,
        })
    }
}

// Note: workspace reconciliation (bench Cargo.toml entries ↔
// regression-budget.json) is implemented as the
// `workspace_reconcile` integration test
// (`fluree-bench-support/tests/workspace_reconcile.rs`) and invoked by
// the `bench-gate` CI job. There is intentionally no library-level
// `validate_against_workspace()` function — the test is the contract.

#[derive(Debug, thiserror::Error)]
pub enum BudgetError {
    #[error("workspace root not found from current dir")]
    WorkspaceNotFound,
    #[error("io: {0}")]
    Io(String),
    #[error("parse: {0}")]
    Parse(String),
}

#[derive(Debug, Clone)]
pub struct BudgetViolation {
    pub crate_name: String,
    pub bench: String,
    pub scale: String,
    pub baseline_ns: f64,
    pub observed_ns: f64,
    pub budget_pct: f64,
    pub allowed_ns: f64,
}

impl std::fmt::Display for BudgetViolation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}/{} [{}]: observed {:.1} ns > allowed {:.1} ns (baseline {:.1} ns + {:.1}% budget)",
            self.crate_name,
            self.bench,
            self.scale,
            self.observed_ns,
            self.allowed_ns,
            self.baseline_ns,
            self.budget_pct,
        )
    }
}

impl std::error::Error for BudgetViolation {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_budget_uses_default() {
        let b = RegressionBudget::empty(7.5);
        assert_eq!(b.budget_pct("foo", "bar", BenchScale::Small), 7.5);
    }

    #[test]
    fn explicit_budget_overrides_default() {
        let mut b = RegressionBudget::empty(5.0);
        b.crates
            .entry("foo".into())
            .or_default()
            .entry("bar".into())
            .or_default()
            .insert("small".into(), 1.0);
        assert_eq!(b.budget_pct("foo", "bar", BenchScale::Small), 1.0);
        // Non-listed scale falls back to default.
        assert_eq!(b.budget_pct("foo", "bar", BenchScale::Medium), 5.0);
    }

    #[test]
    fn check_passes_within_budget() {
        let b = RegressionBudget::empty(10.0);
        let r = check(&b, "foo", "bar", BenchScale::Small, 100.0, 105.0);
        assert!(r.is_ok());
    }

    #[test]
    fn check_fails_over_budget() {
        let b = RegressionBudget::empty(5.0);
        let r = check(&b, "foo", "bar", BenchScale::Small, 100.0, 110.0);
        let v = r.unwrap_err();
        assert_eq!(v.crate_name, "foo");
        assert_eq!(v.bench, "bar");
        assert!(v.observed_ns > v.allowed_ns);
    }

    #[test]
    fn round_trip_serde() {
        let mut b = RegressionBudget::empty(5.0);
        b.crates
            .entry("foo".into())
            .or_default()
            .entry("bar".into())
            .or_default()
            .insert("small".into(), 3.0);
        let s = serde_json::to_string(&b).unwrap();
        let d: RegressionBudget = serde_json::from_str(&s).unwrap();
        assert_eq!(d.budget_pct("foo", "bar", BenchScale::Small), 3.0);
    }

    #[test]
    fn missing_file_returns_empty() {
        let r = load_from(Path::new("/nonexistent/regression-budget.json")).unwrap();
        assert!(r.crates.is_empty());
    }
}
