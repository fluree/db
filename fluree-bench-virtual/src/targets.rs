//! Target configuration: a named native ledger or virtual graph source to run
//! queries against.
//!
//! Each `targets/<id>.json` describes one endpoint. `fluree_home` is the `.fluree`
//! home directory (as the CLI uses it); the on-disk store is `<fluree_home>/storage`
//! (vbench assumes the default storage layout — it does not parse a custom
//! `[server].storage_path` from the home's `config.toml`).

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

/// Whether a target is a materialized native ledger or an R2RML/Iceberg virtual
/// graph source. Drives whether the query builder attaches `.with_r2rml()`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TargetKind {
    Native,
    Virtual,
}

/// A resolved query target.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Target {
    pub id: String,
    pub kind: TargetKind,
    /// The `.fluree` home directory.
    pub fluree_home: PathBuf,
    /// Ledger / graph-source alias passed to `fluree.graph(alias)`.
    pub alias: String,
    pub description: String,
    /// `Some("pending")` marks a target that isn't runnable yet (e.g. the
    /// Snowflake schema isn't loaded). `run`/`exec-one`/`setup` refuse it.
    #[serde(default)]
    pub status: Option<String>,
    /// Human-readable reason for a non-runnable `status`.
    #[serde(default)]
    pub status_reason: Option<String>,
    /// For native targets: the schema-stability triple count `setup --verify`
    /// asserts against.
    #[serde(default)]
    pub expected_total_triples: Option<u64>,
}

impl Target {
    /// Load `targets/<id>.json` from `targets_dir`.
    pub fn load(targets_dir: &Path, id: &str) -> Result<Self> {
        let path = targets_dir.join(format!("{id}.json"));
        let raw = std::fs::read_to_string(&path)
            .with_context(|| format!("reading target config {}", path.display()))?;
        let target: Self = serde_json::from_str(&raw)
            .with_context(|| format!("parsing target config {}", path.display()))?;
        if target.id != id {
            anyhow::bail!(
                "target id mismatch: file {} declares id '{}'",
                path.display(),
                target.id
            );
        }
        Ok(target)
    }

    /// The on-disk store directory (`<fluree_home>/storage`).
    pub fn storage_dir(&self) -> PathBuf {
        self.fluree_home.join("storage")
    }

    pub fn is_virtual(&self) -> bool {
        matches!(self.kind, TargetKind::Virtual)
    }

    /// `"native"` / `"virtual"` for the run-record fingerprint.
    pub fn kind_str(&self) -> &'static str {
        match self.kind {
            TargetKind::Native => "native",
            TargetKind::Virtual => "virtual",
        }
    }

    /// `Err` with the recorded reason if this target is marked non-runnable.
    pub fn ensure_runnable(&self) -> Result<()> {
        if let Some(status) = &self.status {
            if status != "ready" {
                let reason = self.status_reason.as_deref().unwrap_or("no reason given");
                anyhow::bail!(
                    "target '{}' is marked status='{}' and cannot be run: {}",
                    self.id,
                    status,
                    reason
                );
            }
        }
        Ok(())
    }
}
