use crate::config::{self, TomlSyncConfigStore};
use crate::context;
use crate::error::{CliError, CliResult};
use colored::Colorize;
use comfy_table::{ContentArrangement, Table};
use fluree_db_api::server_defaults::FlureeDir;

pub async fn run(dirs: &FlureeDir, remote_flag: Option<&str>, direct: bool) -> CliResult<()> {
    if let Some(remote_name) = remote_flag {
        return run_remote(remote_name, dirs).await;
    }

    // Auto-route to local server for listing if available
    if !direct {
        if let Some(client) = context::try_server_route_client(dirs) {
            return run_remote_with_client(&client, dirs).await;
        }
    }

    let fluree = context::build_fluree(dirs)?;
    let active = config::read_active_ledger(dirs.data_dir());
    let records = fluree.nameservice().all_records().await?;
    let gs_records = fluree.nameservice().all_graph_source_records().await?;

    // Filter out retracted records
    let active_records: Vec<_> = records.iter().filter(|r| !r.retracted).collect();
    let active_gs: Vec<_> = gs_records.iter().filter(|r| !r.retracted).collect();

    let store = TomlSyncConfigStore::new(dirs.config_dir().to_path_buf());
    let tracked = store.tracked_ledgers();

    let has_gs = !active_gs.is_empty();

    if active_records.is_empty() && active_gs.is_empty() && tracked.is_empty() {
        println!("No ledgers found. Run 'fluree create <name>' to create one.");
        return Ok(());
    }

    // Local ledgers + graph sources in one table
    if !active_records.is_empty() || has_gs {
        let mut table = Table::new();
        table.set_content_arrangement(ContentArrangement::Dynamic);
        if has_gs {
            table.set_header(vec!["", "NAME", "BRANCH", "TYPE", "T"]);
        } else {
            table.set_header(vec!["", "LEDGER", "BRANCH", "T"]);
        }

        for record in &active_records {
            let marker = if active.as_deref() == Some(&record.name) {
                "*"
            } else {
                " "
            };
            if has_gs {
                table.add_row(vec![
                    marker.to_string(),
                    record.name.clone(),
                    record.branch.clone(),
                    "Ledger".to_string(),
                    record.commit_t.to_string(),
                ]);
            } else {
                table.add_row(vec![
                    marker.to_string(),
                    record.name.clone(),
                    record.branch.clone(),
                    record.commit_t.to_string(),
                ]);
            }
        }

        for gs in &active_gs {
            let t_str = if gs.index_t > 0 {
                gs.index_t.to_string()
            } else {
                "-".to_string()
            };
            if has_gs {
                table.add_row(vec![
                    " ".to_string(),
                    gs.name.clone(),
                    gs.branch.clone(),
                    format_source_type(&gs.source_type),
                    t_str,
                ]);
            }
        }

        println!("{table}");
    }

    // Tracked ledgers
    if !tracked.is_empty() {
        if !active_records.is_empty() {
            println!();
        }
        println!("Tracked:");
        let mut table = Table::new();
        table.set_content_arrangement(ContentArrangement::Dynamic);
        table.set_header(vec!["", "LEDGER", "REMOTE", "REMOTE ALIAS"]);

        for t in &tracked {
            let marker = if active.as_deref() == Some(&t.local_alias) {
                "*"
            } else {
                " "
            };
            table.add_row(vec![
                marker.to_string(),
                t.local_alias.clone(),
                t.remote.clone(),
                t.remote_alias.clone(),
            ]);
        }

        println!("{table}");
    }

    Ok(())
}

/// List ledgers on a remote server.
async fn run_remote(remote_name: &str, dirs: &FlureeDir) -> CliResult<()> {
    let client = context::build_remote_client(remote_name, dirs).await?;

    let result = client
        .list_ledgers()
        .await
        .map_err(|e| CliError::Remote(format!("failed to list ledgers on '{remote_name}': {e}")))?;

    context::persist_refreshed_tokens(&client, remote_name, dirs).await;

    print_ledger_list(&result, Some(remote_name))
}

/// List ledgers via a pre-built client (used for local server auto-routing).
async fn run_remote_with_client(
    client: &crate::remote_client::RemoteLedgerClient,
    dirs: &FlureeDir,
) -> CliResult<()> {
    let result = client
        .list_ledgers()
        .await
        .map_err(|e| CliError::Remote(format!("failed to list ledgers on local server: {e}")))?;

    context::persist_refreshed_tokens(client, context::LOCAL_SERVER_REMOTE, dirs).await;

    print_ledger_list(&result, None)
}

/// Print a ledger list from a JSON response.
fn print_ledger_list(result: &serde_json::Value, remote_label: Option<&str>) -> CliResult<()> {
    let entries = match result.as_array() {
        Some(arr) => arr,
        None => {
            return Err(CliError::Remote(
                "unexpected response format: expected JSON array".into(),
            ));
        }
    };

    if entries.is_empty() {
        match remote_label {
            Some(name) => println!("No ledgers on remote '{name}'."),
            None => println!("No ledgers found."),
        }
        return Ok(());
    }

    if let Some(name) = remote_label {
        println!("Ledgers on remote '{}':", name.green());
    }

    // Check if any entry has a non-Ledger type (to decide whether to show TYPE column)
    let has_graph_sources = entries.iter().any(|e| {
        e.get("type")
            .and_then(|v| v.as_str())
            .is_some_and(|t| t != "Ledger")
    });

    let mut table = Table::new();
    table.set_content_arrangement(ContentArrangement::Dynamic);
    if has_graph_sources {
        table.set_header(vec!["NAME", "BRANCH", "TYPE", "T"]);
    } else {
        table.set_header(vec!["LEDGER", "BRANCH", "T"]);
    }

    for entry in entries {
        let name = entry
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or("(unknown)");
        let branch = entry
            .get("branch")
            .and_then(|v| v.as_str())
            .unwrap_or("main");
        let t = entry
            .get("t")
            .and_then(serde_json::Value::as_i64)
            .map(|v| v.to_string())
            .unwrap_or_else(|| "-".to_string());

        if has_graph_sources {
            let entry_type = entry
                .get("type")
                .and_then(|v| v.as_str())
                .unwrap_or("Ledger");
            table.add_row(vec![
                name.to_string(),
                branch.to_string(),
                entry_type.to_string(),
                t,
            ]);
        } else {
            table.add_row(vec![name.to_string(), branch.to_string(), t]);
        }
    }

    println!("{table}");
    Ok(())
}

fn format_source_type(st: &fluree_db_nameservice::GraphSourceType) -> String {
    match st {
        fluree_db_nameservice::GraphSourceType::Bm25 => "BM25".to_string(),
        fluree_db_nameservice::GraphSourceType::Vector => "Vector".to_string(),
        fluree_db_nameservice::GraphSourceType::Geo => "Geo".to_string(),
        fluree_db_nameservice::GraphSourceType::R2rml => "R2RML".to_string(),
        fluree_db_nameservice::GraphSourceType::Iceberg => "Iceberg".to_string(),
        fluree_db_nameservice::GraphSourceType::Unknown(s) => format!("Unknown({s})"),
    }
}
