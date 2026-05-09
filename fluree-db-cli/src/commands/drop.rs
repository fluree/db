use crate::config;
use crate::context;
use crate::error::{CliError, CliResult};
use crate::remote_client::RemoteLedgerClient;
use fluree_db_api::admin::DropStatus;
use fluree_db_api::server_defaults::FlureeDir;

pub async fn run(
    name: &str,
    force: bool,
    dirs: &FlureeDir,
    remote_flag: Option<&str>,
    direct: bool,
) -> CliResult<()> {
    if !force {
        return Err(CliError::Usage(format!(
            "use --force to confirm deletion of '{name}'"
        )));
    }

    if let Some(remote_name) = remote_flag {
        let client = context::build_remote_client(remote_name, dirs).await?;
        let result = run_remote(name, &client).await;
        context::persist_refreshed_tokens(&client, remote_name, dirs).await;
        return result;
    }

    if !direct {
        if let Some(client) = context::try_server_route_client(dirs) {
            let result = run_remote(name, &client).await;
            context::persist_refreshed_tokens(&client, context::LOCAL_SERVER_REMOTE, dirs).await;
            // Auto-route operates against the same on-disk storage as `--direct`,
            // so a successful drop must also clear the local active-ledger pointer
            // to avoid leaving CLI state pointing at a deleted ledger.
            if result.is_ok() {
                let active = config::read_active_ledger(dirs.data_dir());
                if active.as_deref() == Some(name) {
                    config::clear_active_ledger(dirs.data_dir())?;
                }
            }
            return result;
        }
    }

    run_local(name, dirs).await
}

async fn run_remote(name: &str, client: &RemoteLedgerClient) -> CliResult<()> {
    let response = client
        .drop_resource(name, true)
        .await
        .map_err(|e| CliError::Remote(format!("failed to drop '{name}': {e}")))?;

    let status = response
        .get("status")
        .and_then(|v| v.as_str())
        .ok_or_else(|| CliError::Remote("unexpected drop response: missing status".into()))?;
    let ledger_id = response
        .get("ledger_id")
        .and_then(|v| v.as_str())
        .unwrap_or(name);

    match status {
        "dropped" => {
            if let Some(files) = response
                .get("files_deleted")
                .and_then(serde_json::Value::as_u64)
            {
                if files > 0 {
                    println!("Dropped '{ledger_id}' (deleted {files} artifacts)");
                } else {
                    println!("Dropped '{ledger_id}'");
                }
            } else {
                println!("Dropped '{ledger_id}'");
            }
        }
        "already_retracted" => println!("'{ledger_id}' was already dropped"),
        "not_found" => return Err(CliError::NotFound(format!("'{name}' not found"))),
        other => {
            return Err(CliError::Remote(format!(
                "unexpected drop status '{other}'"
            )))
        }
    }

    if let Some(warnings) = response.get("warnings").and_then(|v| v.as_array()) {
        for warning in warnings.iter().filter_map(|v| v.as_str()) {
            eprintln!("  warning: {warning}");
        }
    }

    Ok(())
}

async fn run_local(name: &str, dirs: &FlureeDir) -> CliResult<()> {
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
