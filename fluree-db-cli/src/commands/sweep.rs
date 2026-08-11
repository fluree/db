//! Reclaim index artifacts that no index chain references.

use crate::context::{self, LedgerMode};
use crate::error::CliResult;
use crate::remote_client::RemoteLedgerClient;
use colored::Colorize;
use fluree_db_api::server_defaults::FlureeDir;
use fluree_db_api::wire::{SweepPlanResponse, SweepResponse};

/// Whether a sweep reports what it would reclaim, or reclaims it.
#[derive(Clone, Copy)]
pub enum SweepMode {
    Plan,
    Reclaim,
}

impl SweepMode {
    /// `--dry-run` selects the reporting form.
    pub fn from_dry_run(dry_run: bool) -> Self {
        if dry_run {
            Self::Plan
        } else {
            Self::Reclaim
        }
    }
}

/// Run a storage sweep, or report what one would reclaim.
///
/// The sweep covers every branch of a ledger, so `ledger` names the ledger
/// rather than a branch; a branch-qualified alias is rejected rather than
/// silently widened.
pub async fn run_sweep(
    ledger: Option<&str>,
    dirs: &FlureeDir,
    remote_flag: Option<&str>,
    direct: bool,
    mode: SweepMode,
) -> CliResult<()> {
    if let Some(remote_name) = remote_flag {
        let alias = context::resolve_ledger(ledger, dirs)?;
        let client = context::build_remote_client(remote_name, dirs).await?;
        sweep_remote(&client, &alias, mode).await?;
        context::persist_refreshed_tokens(&client, remote_name, dirs).await;
        return Ok(());
    }

    let resolved = {
        let resolved = context::resolve_ledger_mode(ledger, dirs).await?;
        if direct {
            resolved
        } else {
            context::try_server_route(resolved, dirs)
        }
    };

    match resolved {
        LedgerMode::Tracked {
            client,
            remote_alias,
            remote_name,
            ..
        } => {
            sweep_remote(&client, &remote_alias, mode).await?;
            context::persist_refreshed_tokens(&client, &remote_name, dirs).await;
        }
        LedgerMode::Local { fluree, alias } => match mode {
            SweepMode::Plan => {
                let plan = fluree.plan_index_sweep(&alias).await?;
                print_plan(&SweepPlanResponse::new(alias, plan));
            }
            SweepMode::Reclaim => {
                eprintln!(
                    "  {} reclaiming orphaned index artifacts for {}...",
                    "sweep:".cyan().bold(),
                    alias
                );
                let result = fluree.sweep_index_storage(&alias).await?;
                print_result(&SweepResponse::new(alias, result));
            }
        },
    }

    Ok(())
}

async fn sweep_remote(client: &RemoteLedgerClient, ledger: &str, mode: SweepMode) -> CliResult<()> {
    match mode {
        SweepMode::Plan => print_plan(&client.sweep_plan(ledger).await?),
        SweepMode::Reclaim => print_result(&client.sweep(ledger).await?),
    }
    Ok(())
}

fn print_plan(plan: &SweepPlanResponse) {
    println!(
        "{} would reclaim {} of {} index artifacts ({} still referenced)",
        plan.ledger, plan.orphan_count, plan.scanned, plan.live
    );
    for address in &plan.orphans {
        println!("  {address}");
    }
    if plan.orphan_count > 0 {
        println!(
            "\nRun without {} to reclaim them.",
            "--dry-run".yellow().bold()
        );
    }
}

fn print_result(result: &SweepResponse) {
    println!(
        "Reclaimed {} artifacts from {}",
        result.reclaimed, result.ledger
    );
    if !result.failures.is_empty() {
        println!(
            "\n{} {} artifact(s) could not be released; they stay in storage and \
             the next sweep retries them:",
            "warning:".yellow().bold(),
            result.failures.len()
        );
        for failure in &result.failures {
            println!("  {}: {}", failure.address, failure.error);
        }
    }
}
