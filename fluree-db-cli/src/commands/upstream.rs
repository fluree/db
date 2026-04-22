//! Upstream tracking management commands: set, remove, list

use crate::cli::UpstreamAction;
use crate::config::TomlSyncConfigStore;
use crate::context;
use crate::error::{CliError, CliResult};
use colored::Colorize;
use comfy_table::{Cell, Table};
use fluree_db_api::server_defaults::FlureeDir;
use fluree_db_nameservice::RemoteName;
use fluree_db_nameservice_sync::{SyncConfigStore, UpstreamConfig};

pub async fn run(action: UpstreamAction, dirs: &FlureeDir) -> CliResult<()> {
    let store = TomlSyncConfigStore::new(dirs.config_dir().to_path_buf());

    match action {
        UpstreamAction::Set {
            local,
            remote,
            remote_alias,
            auto_pull,
        } => run_set(&store, &local, &remote, remote_alias, auto_pull).await,
        UpstreamAction::Remove { local } => run_remove(&store, &local).await,
        UpstreamAction::List => run_list(&store).await,
    }
}

async fn run_set(
    store: &TomlSyncConfigStore,
    local: &str,
    remote: &str,
    remote_alias: Option<String>,
    auto_pull: bool,
) -> CliResult<()> {
    // Normalize local alias to include branch
    let local_alias = context::to_ledger_id(local);
    let remote_alias = remote_alias.unwrap_or_else(|| local_alias.clone());

    // Check that the remote exists
    let remote_name = RemoteName::new(remote);
    let remote_exists = store
        .get_remote(&remote_name)
        .await
        .map_err(|e| CliError::Config(e.to_string()))?
        .is_some();

    if !remote_exists {
        return Err(CliError::NotFound(format!(
            "remote '{remote}' not found; add it with: fluree remote add {remote} <url>"
        )));
    }

    let config = UpstreamConfig {
        local_alias: local_alias.clone(),
        remote: remote_name,
        remote_alias: remote_alias.clone(),
        auto_pull,
    };

    store
        .set_upstream(&config)
        .await
        .map_err(|e| CliError::Config(e.to_string()))?;

    println!(
        "Set upstream for '{}' -> {}/{}",
        local_alias.green(),
        remote,
        remote_alias
    );
    if auto_pull {
        println!("  auto-pull: enabled");
    }
    Ok(())
}

async fn run_remove(store: &TomlSyncConfigStore, local: &str) -> CliResult<()> {
    let local_alias = context::to_ledger_id(local);

    // Check if exists
    let existing = store
        .get_upstream(&local_alias)
        .await
        .map_err(|e| CliError::Config(e.to_string()))?;

    if existing.is_none() {
        return Err(CliError::NotFound(format!(
            "no upstream configured for '{local_alias}'"
        )));
    }

    store
        .remove_upstream(&local_alias)
        .await
        .map_err(|e| CliError::Config(e.to_string()))?;

    println!("Removed upstream for '{local_alias}'");
    Ok(())
}

async fn run_list(store: &TomlSyncConfigStore) -> CliResult<()> {
    let upstreams = store
        .list_upstreams()
        .await
        .map_err(|e| CliError::Config(e.to_string()))?;

    if upstreams.is_empty() {
        println!("No upstream tracking configured.");
        println!(
            "  {} fluree upstream set <local-ledger> <remote>",
            "hint:".cyan().bold()
        );
        return Ok(());
    }

    let mut table = Table::new();
    table.set_header(vec!["Local", "Remote", "Remote Alias", "Auto-pull"]);

    for upstream in upstreams {
        table.add_row(vec![
            Cell::new(&upstream.local_alias),
            Cell::new(upstream.remote.as_str()),
            Cell::new(&upstream.remote_alias),
            Cell::new(if upstream.auto_pull { "yes" } else { "no" }),
        ]);
    }

    println!("{table}");
    Ok(())
}
