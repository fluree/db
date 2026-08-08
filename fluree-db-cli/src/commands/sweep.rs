//! Reclaim index artifacts that no index chain references.

use crate::context::{self, LedgerMode};
use crate::error::CliResult;
use colored::Colorize;
use fluree_db_api::server_defaults::FlureeDir;
use fluree_db_api::wire::{SweepPlanResponse, SweepResponse};

/// Run a storage sweep, or report what one would reclaim.
///
/// The sweep covers every branch of a ledger, so `ledger` names the ledger
/// rather than a branch — a `name:branch` argument has its branch stripped.
pub async fn run_sweep(
    ledger: Option<&str>,
    dirs: &FlureeDir,
    remote_flag: Option<&str>,
    direct: bool,
    dry_run: bool,
) -> CliResult<()> {
    if let Some(remote_name) = remote_flag {
        let alias = context::resolve_ledger(ledger, dirs)?;
        let client = context::build_remote_client(remote_name, dirs).await?;
        let name = ledger_name(&alias);

        if dry_run {
            let plan = client.sweep_plan(&name).await?;
            print_plan(&plan);
        } else {
            let result = client.sweep(&name).await?;
            print_result(&result);
        }

        context::persist_refreshed_tokens(&client, remote_name, dirs).await;
        return Ok(());
    }

    let mode = {
        let mode = context::resolve_ledger_mode(ledger, dirs).await?;
        if direct {
            mode
        } else {
            context::try_server_route(mode, dirs)
        }
    };

    match mode {
        LedgerMode::Tracked {
            client,
            remote_alias,
            remote_name,
            ..
        } => {
            let name = ledger_name(&remote_alias);

            if dry_run {
                let plan = client.sweep_plan(&name).await?;
                print_plan(&plan);
            } else {
                let result = client.sweep(&name).await?;
                print_result(&result);
            }

            context::persist_refreshed_tokens(&client, &remote_name, dirs).await;
        }
        LedgerMode::Local { fluree, alias } => {
            let name = ledger_name(&alias);

            if dry_run {
                let plan = fluree.plan_index_sweep(&name).await?;
                print_plan(&SweepPlanResponse::new(name, plan));
            } else {
                eprintln!(
                    "  {} reclaiming orphaned index artifacts for {}...",
                    "sweep:".cyan().bold(),
                    name
                );
                let result = fluree.sweep_index_storage(&name).await?;
                print_result(&SweepResponse::new(name, result));
            }
        }
    }

    Ok(())
}

/// Strip any `:branch` suffix — a sweep is ledger-wide because dict blobs are
/// shared across a ledger's branches.
fn ledger_name(alias: &str) -> String {
    alias.split(':').next().unwrap_or(alias).to_string()
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
