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
use fluree_db_api::{DropMode, Fluree, FlureeR2rmlProvider};

use crate::cli::{MaterializeOutput, MaterializeVerify};
use crate::context;
use crate::error::{CliError, CliResult};
use fluree_db_api::server_defaults::FlureeDir;

/// The env var read by fluree-db-iceberg's fail-closed merge-on-read guard. A
/// stable public contract (`fluree_db_iceberg::mor_guard::ALLOW_MOR_DELETES_ENV`);
/// hard-coded here so the CLI need not take a direct dependency on that crate.
const ALLOW_MOR_DELETES_ENV: &str = "FLUREE_ICEBERG_ALLOW_MOR_DELETES";

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
    /// Global `--parallelism` (0 = unset).
    pub parallelism: usize,
    /// Global `--memory-budget-mb` (0 = unset).
    pub memory_budget_mb: usize,
    pub quiet: bool,
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
        .execute()
        .await
        .map_err(|e| classify_build_error(&e.to_string(), params.allow_mor_deletes))?;

    // Verify (publish-then-verify-then-drop): the gate needs a queryable twin, so
    // the build has already published; a failure drops it so nothing unverified
    // stays announced.
    let ledger = fluree
        .ledger(&twin_ledger)
        .await
        .map_err(|e| CliError::Import(format!("load twin for verification: {e}")))?;
    let report = verify_twin(
        fluree,
        &ledger,
        &*provider,
        params.graph_source,
        params.verify.into(),
    )
    .await
    .map_err(|e| CliError::Import(format!("parity gate: {e}")))?;

    if !report.passed {
        let _ = fluree.drop_ledger(&twin_ledger, DropMode::Hard).await;
        return Err(CliError::Import(format!(
            "parity gate FAILED — twin '{twin_ledger}' dropped, not announced.\n{}",
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
            println!(
                "Twin packed to {} ({} flakes). The source twin ledger '{twin_ledger}' stays \
                 registered locally; drop it with `fluree drop {twin_ledger}` when no longer needed.",
                path.display(),
                result.flake_count,
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

    #[test]
    fn verify_mode_maps() {
        assert_eq!(
            VerifyMode::from(MaterializeVerify::Quick),
            VerifyMode::Quick
        );
        assert_eq!(VerifyMode::from(MaterializeVerify::Full), VerifyMode::Full);
    }
}
