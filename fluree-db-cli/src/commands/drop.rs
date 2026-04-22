use crate::config;
use crate::context;
use crate::error::{CliError, CliResult};
use fluree_db_api::admin::DropStatus;
use fluree_db_api::server_defaults::FlureeDir;

pub async fn run(name: &str, force: bool, dirs: &FlureeDir) -> CliResult<()> {
    if !force {
        return Err(CliError::Usage(format!(
            "use --force to confirm deletion of '{name}'"
        )));
    }

    let fluree = context::build_fluree(dirs)?;

    // Try dropping as a ledger first
    let report = fluree
        .drop_ledger(name, fluree_db_api::DropMode::Hard)
        .await?;

    match report.status {
        DropStatus::Dropped => {
            // If dropped ledger was active, clear it
            let active = config::read_active_ledger(dirs.data_dir());
            if active.as_deref() == Some(name) {
                config::clear_active_ledger(dirs.data_dir())?;
            }
            if report.artifacts_deleted > 0 {
                println!(
                    "Dropped ledger '{name}' (deleted {} artifacts)",
                    report.artifacts_deleted
                );
            } else {
                println!("Dropped ledger '{name}'");
            }
            for w in &report.warnings {
                eprintln!("  warning: {w}");
            }
            return Ok(());
        }
        DropStatus::AlreadyRetracted => {
            println!("Ledger '{name}' was already dropped");
            return Ok(());
        }
        DropStatus::NotFound => {
            // Not a ledger — try graph source
        }
    }

    // Try dropping as a graph source
    let gs_report = fluree
        .drop_graph_source(name, None, fluree_db_api::DropMode::Hard)
        .await?;

    match gs_report.status {
        DropStatus::Dropped => {
            println!(
                "Dropped graph source '{}:{}'",
                gs_report.name, gs_report.branch
            );
            for w in &gs_report.warnings {
                eprintln!("  warning: {w}");
            }
        }
        DropStatus::AlreadyRetracted => {
            println!(
                "Graph source '{}:{}' was already dropped",
                gs_report.name, gs_report.branch
            );
        }
        DropStatus::NotFound => {
            return Err(CliError::NotFound(format!("'{name}' not found")));
        }
    }

    Ok(())
}
