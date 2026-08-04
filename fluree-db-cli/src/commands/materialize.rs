//! `fluree materialize <graph-source>` — build a native twin ledger from a
//! virtual (R2RML-over-Iceberg) graph source (DEC-003 Deliverable 1).
//!
//! Flow: bulk-build every triple through the R2RML scan → native import pipeline,
//! then run the parity gate against the source, then write the output. The gate
//! runs AFTER the build publishes (verification needs a queryable twin), so a
//! failed gate DROPS the twin — an unverified twin is never left announced.
//!
//! MACHINE-SAFETY: the default posture is co-resident-tolerant — a modest fixed
//! memory budget and low parallelism, never own-the-box auto-sizing. `--max-performance`
//! opts into host auto-sizing on a cleared machine.
#![cfg(feature = "iceberg")]

use std::path::{Path, PathBuf};
use std::sync::Arc;

use fluree_db_api::materialize::{verify_twin, ParityReport, VerifyMode};
// The MoR-guard env var comes from ONE definition — fluree-db-iceberg's
// `mor_guard::ALLOW_MOR_DELETES_ENV`, re-exported by fluree-db-api (which the CLI
// already depends on) — instead of a hard-copied literal that could silently drift
// on a rename.
use fluree_db_api::{
    DropMode, Fluree, FlureeR2rmlProvider, NsRecord, ALLOW_MOR_DELETES_ENV,
};

use crate::cli::{MaterializeOutput, MaterializeVerify};
use crate::context;
use crate::error::{CliError, CliResult};
use fluree_db_api::server_defaults::FlureeDir;

/// Co-resident-safe default parallelism when neither `--parallelism` nor
/// `--max-performance` is given (§14 machine-safety).
const CO_RESIDENT_PARALLELISM: usize = 2;
/// Co-resident-safe default memory budget (MB) — deliberately modest so a build
/// cannot OOM a shared machine; raise it explicitly for throughput.
const CO_RESIDENT_MEMORY_MB: usize = 512;

impl From<MaterializeVerify> for VerifyMode {
    fn from(v: MaterializeVerify) -> Self {
        match v {
            MaterializeVerify::Quick => VerifyMode::Quick,
            MaterializeVerify::Full => VerifyMode::Full,
        }
    }
}

/// Inputs for the materialize command (resolved from the CLI args + globals).
pub struct MaterializeParams<'a> {
    pub graph_source: &'a str,
    pub into: Option<&'a str>,
    pub output: MaterializeOutput,
    pub output_path: Option<&'a Path>,
    pub verify: MaterializeVerify,
    pub max_performance: bool,
    pub allow_mor_deletes: bool,
    /// `--allow-duplicate-parent-keys`: build over a source whose FK parent keys are
    /// non-unique (default `false` = decline). See the import builder for the rationale.
    pub allow_duplicate_parent_keys: bool,
    /// Global `--parallelism` (0 = unset).
    pub parallelism: usize,
    /// Global `--memory-budget-mb` (0 = unset).
    pub memory_budget_mb: usize,
    pub quiet: bool,
    /// `--tmp-dir` override for full-verify's on-disk spool (None = default under
    /// the twin's `.fluree` storage area).
    pub tmp_dir: Option<&'a Path>,
}

pub async fn run(dirs: &FlureeDir, params: &MaterializeParams<'_>) -> CliResult<()> {
    // `--output s3` needs an S3-backed home the file-backed CLI does not build.
    // Reject up front — before any work — rather than build then fail.
    if params.output == MaterializeOutput::S3 {
        return Err(CliError::Usage(
            "--output s3 is not yet wired in the file-backed CLI; build with \
             --output pack and upload the pack, or use the direct-S3 CAS builder \
             (DEC-003 §3)"
                .to_string(),
        ));
    }

    // G — MoR override: opt in BEFORE any scan so the pin-all pre-pass (the first
    // table touch) sees it. Left unset, the guard fails the build closed.
    if params.allow_mor_deletes {
        // SAFETY: single-threaded CLI startup, before any provider/scan is built.
        std::env::set_var(ALLOW_MOR_DELETES_ENV, "1");
    }

    let twin_ledger = params
        .into
        .map(str::to_string)
        .unwrap_or_else(|| default_twin_name(params.graph_source));

    // §14 co-resident posture: conservative fixed defaults unless the user set an
    // explicit value or opted into --max-performance (own-the-box auto-sizing:
    // 0 flows through to the import pipeline's ~80%-RAM auto path).
    let (parallelism, memory_budget_mb) = if params.max_performance {
        (params.parallelism, params.memory_budget_mb)
    } else {
        (
            if params.parallelism == 0 {
                CO_RESIDENT_PARALLELISM
            } else {
                params.parallelism
            },
            if params.memory_budget_mb == 0 {
                CO_RESIDENT_MEMORY_MB
            } else {
                params.memory_budget_mb
            },
        )
    };

    // One-shot CLI: leak the Fluree so the real provider is `'static` for the
    // import producer thread. Reclaimed by the OS at process exit. Keeping ONE
    // provider (hence one catalog session) across the build is required for the
    // snapshot pin + watermark capture.
    let fluree: &'static Fluree = Box::leak(Box::new(context::build_fluree(dirs)?));
    let provider = Arc::new(FlureeR2rmlProvider::new(fluree));

    // CRITICAL-1 (#1529 review): a failed parity gate drops the WHOLE ledger NAME
    // — every branch, `DropMode::Hard`, no `--force` (admin.rs collects
    // `all_records().filter(|r| r.name == ledger_name)`). But the build's freshness
    // refusal is branch-SCOPED (keyed on `name:branch`), so `--into existing:new`
    // initializes cleanly and a later gate failure — which can fire on a CORRECT
    // twin (CRITICAL-3) — then purges every OTHER branch of that ledger too,
    // irreversibly. Refuse up front unless the ledger NAME is fresh apart from the
    // target branch, restoring by construction the invariant docs/cli/materialize.md
    // rests on ("the drop can only ever hit a ledger this build just created").
    let (twin_name, twin_branch) = fluree_db_core::ledger_id::split_ledger_id(&twin_ledger)?;
    let existing_branches = fluree
        .nameservice()
        .list_branches(&twin_name)
        .await
        .map_err(|e| CliError::Import(format!("check twin ledger freshness: {e}")))?;
    if let Some(other) = blocking_existing_branch(&existing_branches, &twin_branch) {
        return Err(CliError::Usage(format!(
            "materialize requires a fresh ledger name; '{twin_name}' already has branch \
             '{other_branch}'. A failed parity gate hard-drops the entire ledger name (every \
             branch), so materializing into a new branch of an existing ledger would risk purging \
             '{twin_name}:{other_branch}'. Pick a ledger name that does not yet exist, or drop the \
             existing ledger first.",
            other_branch = other.branch,
        )));
    }

    if !params.quiet {
        println!(
            "Materializing twin '{twin_ledger}' from '{}' (parallelism={parallelism}, \
             memory_budget_mb={}, verify={:?})…",
            params.graph_source,
            if memory_budget_mb == 0 {
                "auto".to_string()
            } else {
                memory_budget_mb.to_string()
            },
            params.verify,
        );
    }

    // Build the twin.
    let result = fluree
        .create(&twin_ledger)
        .import_r2rml(Arc::clone(&provider), params.graph_source)
        .parallelism(parallelism)
        .memory_budget_mb(memory_budget_mb)
        .allow_duplicate_parent_keys(params.allow_duplicate_parent_keys)
        .execute()
        .await
        .map_err(|e| classify_build_error(&e.to_string(), params.allow_mor_deletes))?;

    // VERIFY-DEFAULT HONESTY: make the quick gate's epistemics operator-visible.
    // The quick sample compares the twin against the build's OWN enumerator, so
    // it catches ingest/index corruption but shares the enumerator's blind spot
    // (a bug in enumerator logic appears identically on both sides).
    if !params.quiet && params.verify == MaterializeVerify::Quick {
        println!(
            "Quick verify: class counts + a seeded per-class sample against the build's own \
             enumerator — catches ingest/index corruption, NOT enumerator logic (shared oracle). \
             Run `--verify full` plus the independent native diff before a production cutover."
        );
    }

    // Verify (publish-then-verify-then-drop): the gate needs a queryable twin, so
    // the build has already published; a failure drops it so nothing unverified
    // stays announced.
    let ledger = fluree
        .ledger(&twin_ledger)
        .await
        .map_err(|e| CliError::Import(format!("load twin for verification: {e}")))?;
    // Full-verify spools to disk; default under the twin's `.fluree` storage area
    // (discoverable + cleanable, and NOT a tmpfs `/tmp` that would undo the
    // bounded-memory design), or the explicit `--tmp-dir` override.
    let verify_tmp = params
        .tmp_dir
        .map(Path::to_path_buf)
        .unwrap_or_else(|| dirs.data_dir().join("materialize-verify"));
    let report = verify_twin(
        fluree,
        &ledger,
        &*provider,
        params.graph_source,
        params.verify.into(),
        Some(&verify_tmp),
    )
    .await
    .map_err(|e| CliError::Import(format!("parity gate: {e}")))?;

    if !report.passed {
        // drop_ledger drops the WHOLE ledger and rejects a `:branch` suffix, so
        // strip it — otherwise the drop 400s and the unverified twin stays
        // announced (the exact hazard this path guards against).
        let drop_result = fluree
            .drop_ledger(ledger_name_no_branch(&twin_ledger), DropMode::Hard)
            .await;
        let drop_note = match drop_result {
            Ok(_) => "dropped, not announced".to_string(),
            Err(e) => format!("WARNING: automatic drop FAILED ({e}) — drop it manually"),
        };
        return Err(CliError::Import(format!(
            "parity gate FAILED — twin '{twin_ledger}' {drop_note}.\n{}",
            format_failures(&report),
        )));
    }

    if !params.quiet {
        println!("Parity gate passed ({} checks).", report.checks.len());
    }

    // Output.
    match params.output {
        MaterializeOutput::Ledger => {
            println!(
                "Twin ledger '{twin_ledger}' built and verified: {} flakes, index t={}.",
                result.flake_count, result.index_t
            );
        }
        MaterializeOutput::Pack => {
            let path = params
                .output_path
                .map(Path::to_path_buf)
                .unwrap_or_else(|| {
                    PathBuf::from(format!("{}.flpack", sanitize_filename(&twin_ledger)))
                });
            let mut file = tokio::fs::File::create(&path).await.map_err(|e| {
                CliError::Import(format!("create pack file {}: {e}", path.display()))
            })?;
            fluree
                .archive_ledger(&twin_ledger, true, &mut file)
                .await
                .map_err(|e| CliError::Import(format!("write pack: {e}")))?;
            // #1529 review (minor): `fluree drop` rejects a `:branch` suffix and
            // requires `--force`, so print the whole-ledger id + --force (the old
            // advice `fluree drop {twin_ledger}` failed twice as written).
            println!(
                "Twin packed to {} ({} flakes). The source twin ledger '{twin_ledger}' stays \
                 registered locally; drop it with `fluree drop {} --force` when no longer needed.",
                path.display(),
                result.flake_count,
                ledger_name_no_branch(&twin_ledger),
            );
        }
        MaterializeOutput::S3 => unreachable!("rejected above"),
    }

    Ok(())
}

/// Default twin ledger name: the graph-source id with a `-twin` suffix, keeping
/// any `:branch` (e.g. `dw-gs:main` → `dw-gs-twin:main`).
fn default_twin_name(graph_source: &str) -> String {
    match graph_source.rsplit_once(':') {
        Some((name, branch)) => format!("{name}-twin:{branch}"),
        None => format!("{graph_source}-twin"),
    }
}

/// CRITICAL-1: given the nameservice records for the twin's ledger NAME and the
/// build's target branch, return the first record that BLOCKS a fresh build, if
/// any. Only a LIVE (non-retracted) branch OTHER than the target blocks: a
/// retracted branch is already gone (the gate-fail drop purging it is no data
/// loss), and the target branch itself is governed by the build's own
/// branch-scoped freshness guard. A blocker means the name-scoped gate-fail drop
/// could purge a branch this build did not create — so we refuse before building.
fn blocking_existing_branch<'a>(
    existing: &'a [NsRecord],
    target_branch: &str,
) -> Option<&'a NsRecord> {
    existing
        .iter()
        .find(|r| !r.retracted && r.branch != target_branch)
}

/// The whole-ledger id (no `:branch` suffix), as `drop_ledger` requires.
fn ledger_name_no_branch(ledger: &str) -> &str {
    ledger
        .rsplit_once(':')
        .map(|(name, _)| name)
        .unwrap_or(ledger)
}

/// A filesystem-safe basename for a default pack path (ledger ids contain `/`
/// and `:`).
fn sanitize_filename(ledger: &str) -> String {
    ledger
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// Map a build error to a helpful CLI error, surfacing the MoR guard's refusal
/// with the documented override (G).
fn classify_build_error(msg: &str, allowed: bool) -> CliError {
    let looks_like_mor = msg.contains("merge-on-read")
        || msg.contains("MergeOnRead")
        || msg.contains("delete manifest");
    if looks_like_mor && !allowed {
        return CliError::Import(format!(
            "a source table carries Iceberg merge-on-read delete files, which the twin builder \
             refuses by default — materializing them as live rows would silently include deleted \
             data.\n  Re-run with --allow-mor-deletes to build a point-in-time snapshot that MAY \
             include those rows (documented staleness).\n  Underlying error: {msg}"
        ));
    }
    CliError::Import(format!("materialize build failed: {msg}"))
}

/// One line per failing parity check.
fn format_failures(report: &ParityReport) -> String {
    report
        .failures()
        .iter()
        .map(|c| format!("  - {}: {:?}", c.name, c.outcome))
        .collect::<Vec<_>>()
        .join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_twin_name_preserves_branch() {
        assert_eq!(default_twin_name("dw-gs:main"), "dw-gs-twin:main");
        assert_eq!(default_twin_name("catalog:dev"), "catalog-twin:dev");
        assert_eq!(default_twin_name("plain"), "plain-twin");
    }

    #[test]
    fn ledger_name_no_branch_strips_suffix_for_drop() {
        // drop_ledger rejects a `:branch` suffix — the gate-fail drop must pass
        // the whole-ledger id or it 400s and leaves the twin announced.
        assert_eq!(ledger_name_no_branch("dw-gs-twin:main"), "dw-gs-twin");
        assert_eq!(ledger_name_no_branch("a/b/c:dev"), "a/b/c");
        assert_eq!(ledger_name_no_branch("nobranch"), "nobranch");
    }

    #[test]
    fn sanitize_filename_strips_path_hostile_chars() {
        assert_eq!(sanitize_filename("dw-gs-twin:main"), "dw-gs-twin_main");
        assert_eq!(sanitize_filename("a/b:c"), "a_b_c");
    }

    #[test]
    fn mor_error_surfaces_the_override_when_not_allowed() {
        let err = classify_build_error(
            "scan error: snapshot summary reports merge-on-read delete files",
            false,
        );
        let msg = err.to_string();
        assert!(
            msg.contains("--allow-mor-deletes"),
            "MoR build failure must point the user at the override; got: {msg}"
        );
    }

    #[test]
    fn mor_error_not_special_cased_once_allowed() {
        // With the override already on, a later MoR-worded error is just a plain
        // build failure (no redundant re-suggestion of the flag).
        let err = classify_build_error("merge-on-read something else went wrong", true);
        assert!(!err.to_string().contains("--allow-mor-deletes"));
    }

    fn retracted(mut r: NsRecord) -> NsRecord {
        r.retracted = true;
        r
    }

    #[test]
    fn blocking_existing_branch_refuses_a_live_non_target_branch() {
        // CRITICAL-1: the name-scoped gate-fail drop would purge this live 'main'.
        let existing = vec![NsRecord::new("analytics-twin", "main")];
        let blocker = blocking_existing_branch(&existing, "v2");
        assert!(blocker.is_some(), "a live non-target branch must block");
        assert_eq!(blocker.unwrap().branch, "main");
    }

    #[test]
    fn blocking_existing_branch_allows_fresh_or_target_only() {
        // Empty name → fresh → no block.
        assert!(blocking_existing_branch(&[], "main").is_none());
        // Only the target branch present (governed by the build's own freshness
        // guard, and the drop can only hit what this build created) → no block.
        let target_only = vec![NsRecord::new("dw-twin", "main")];
        assert!(blocking_existing_branch(&target_only, "main").is_none());
        // A RETRACTED non-target branch is already gone → purging it is no data
        // loss → no block.
        let retracted_other = vec![retracted(NsRecord::new("dw-twin", "old"))];
        assert!(blocking_existing_branch(&retracted_other, "main").is_none());
    }

    #[test]
    fn verify_mode_maps() {
        assert_eq!(
            VerifyMode::from(MaterializeVerify::Quick),
            VerifyMode::Quick
        );
        assert_eq!(VerifyMode::from(MaterializeVerify::Full), VerifyMode::Full);
    }
}
