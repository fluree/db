//! Reconcile workspace `[[bench]]` entries with `regression-budget.json`.
//!
//! Run by the `bench-gate` CI job and any developer who wants to confirm
//! their new bench is registered correctly:
//!
//! ```bash
//! cargo test -p fluree-bench-support --test workspace_reconcile
//! ```
//!
//! Failure messages name the offending crate / bench / mismatch shape.
//! The four classes of failure:
//!
//! 1. **Missing budget** — a `[[bench]]` is declared in a crate's
//!    `Cargo.toml` but has no entry in `regression-budget.json`'s
//!    `crates.<crate>.<bench>` map. The bench will fall back to the
//!    default budget at runtime, which is fine in dev but the gate
//!    rejects it because it implies someone added a bench without
//!    explicitly thinking about its budget.
//! 2. **Stale budget** — a budget entry exists in
//!    `regression-budget.json` but no corresponding `[[bench]]` is
//!    declared anywhere. Indicates a deleted or renamed bench.
//! 3. **Unknown crate** — a budget entry points at a crate name that
//!    isn't a workspace member.
//! 4. **Bad budget shape** — a per-scale entry is missing or non-numeric.
//!    (Caught by serde at load time, not here, but we surface it for
//!    completeness.)
//!
//! See `BENCHMARKING.md` and `docs/contributing/benches.md` for the
//! contributor workflow this test enforces.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

/// Walk the workspace Cargo.toml at `root` and collect the set of
/// member crate paths.
fn workspace_members(root: &Path) -> Vec<PathBuf> {
    let manifest_path = root.join("Cargo.toml");
    let raw = std::fs::read_to_string(&manifest_path)
        .unwrap_or_else(|e| panic!("read {}: {e}", manifest_path.display()));
    let parsed: toml::Value = raw
        .parse()
        .unwrap_or_else(|e| panic!("parse {}: {e}", manifest_path.display()));

    let members = parsed
        .get("workspace")
        .and_then(|w| w.get("members"))
        .and_then(|m| m.as_array())
        .unwrap_or_else(|| {
            panic!(
                "{} does not declare [workspace] members",
                manifest_path.display()
            )
        });

    members
        .iter()
        .filter_map(|v| v.as_str())
        .map(|s| root.join(s))
        .collect()
}

/// Crate name + list of declared bench names from a single member's
/// Cargo.toml. Empty bench list is normal — most crates have none.
fn crate_benches(crate_dir: &Path) -> Option<(String, Vec<String>)> {
    let manifest_path = crate_dir.join("Cargo.toml");
    let raw = std::fs::read_to_string(&manifest_path).ok()?;
    let parsed: toml::Value = raw.parse().ok()?;

    let crate_name = parsed
        .get("package")
        .and_then(|p| p.get("name"))
        .and_then(|n| n.as_str())?
        .to_string();

    let benches = parsed
        .get("bench")
        .and_then(|b| b.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|entry| entry.get("name").and_then(|n| n.as_str()).map(String::from))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    Some((crate_name, benches))
}

#[derive(Debug, serde::Deserialize)]
struct BudgetFile {
    #[serde(default)]
    crates: BTreeMap<String, BTreeMap<String, BTreeMap<String, f64>>>,
}

fn load_budget(root: &Path) -> BudgetFile {
    let path = root.join("regression-budget.json");
    let raw = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    serde_json::from_str(&raw).unwrap_or_else(|e| panic!("parse {}: {e}", path.display()))
}

/// Locate the workspace root by walking up from the test's manifest dir.
fn workspace_root() -> PathBuf {
    // CARGO_MANIFEST_DIR points at fluree-bench-support; the workspace
    // root is one level up.
    let here = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    here.parent()
        .unwrap_or_else(|| panic!("CARGO_MANIFEST_DIR has no parent: {}", here.display()))
        .to_path_buf()
}

#[test]
fn workspace_reconciles_with_regression_budget() {
    let root = workspace_root();

    // Build the {crate -> [bench]} map from workspace members.
    let mut declared: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for member_dir in workspace_members(&root) {
        let Some((crate_name, benches)) = crate_benches(&member_dir) else {
            continue;
        };
        if !benches.is_empty() {
            declared
                .entry(crate_name)
                .or_default()
                .extend(benches.into_iter());
        }
    }

    let budget = load_budget(&root);

    let mut missing_budget: Vec<String> = Vec::new();
    let mut stale_budget: Vec<String> = Vec::new();
    let mut unknown_crate: Vec<String> = Vec::new();

    // Direction 1: every declared [[bench]] must have a budget entry.
    for (crate_name, benches) in &declared {
        let crate_budget = budget.crates.get(crate_name);
        for bench in benches {
            let has_entry = crate_budget
                .and_then(|c| c.get(bench))
                .map(|m| !m.is_empty())
                .unwrap_or(false);
            if !has_entry {
                missing_budget.push(format!("{crate_name}/{bench}"));
            }
        }
    }

    // Direction 2: every budget entry must reference a declared bench.
    for (crate_name, crate_budget) in &budget.crates {
        if !declared.contains_key(crate_name) {
            unknown_crate.push(crate_name.clone());
            continue;
        }
        let declared_benches = &declared[crate_name];
        for bench in crate_budget.keys() {
            if !declared_benches.contains(bench) {
                stale_budget.push(format!("{crate_name}/{bench}"));
            }
        }
    }

    let mut errors: Vec<String> = Vec::new();
    if !missing_budget.is_empty() {
        errors.push(format!(
            "Missing budget entries (declared but unbudgeted): {}\n  \
             → add an entry under crates.<crate>.<bench> in regression-budget.json",
            missing_budget.join(", ")
        ));
    }
    if !stale_budget.is_empty() {
        errors.push(format!(
            "Stale budget entries (budgeted but undeclared): {}\n  \
             → remove the entry from regression-budget.json or rename the bench file",
            stale_budget.join(", ")
        ));
    }
    if !unknown_crate.is_empty() {
        errors.push(format!(
            "Budget refers to unknown crate(s): {}\n  \
             → either fix the crate name in regression-budget.json or add the crate to the workspace",
            unknown_crate.join(", ")
        ));
    }

    assert!(
        errors.is_empty(),
        "Workspace [[bench]] entries do not reconcile with regression-budget.json:\n\n{}\n\n\
         See docs/contributing/benches.md (six-step workflow) for details.",
        errors.join("\n\n")
    );
}
