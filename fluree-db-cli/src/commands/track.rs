//! Track commands: add, remove, list, status
//!
//! Tracked ledgers are remote-only — all operations (query, insert, upsert)
//! are forwarded to the remote server via HTTP. No local ledger storage or
//! blocks are needed.

use crate::cli::TrackAction;
use crate::config::{TomlSyncConfigStore, TrackedLedgerConfig};
use crate::context::build_client_from_auth;
use crate::error::{CliError, CliResult};
use colored::Colorize;
use comfy_table::{Cell, Table};
use fluree_db_api::server_defaults::FlureeDir;
use fluree_db_nameservice::RemoteName;
use fluree_db_nameservice_sync::{RemoteEndpoint, SyncConfigStore};

pub async fn run(action: TrackAction, dirs: &FlureeDir) -> CliResult<()> {
    let store = TomlSyncConfigStore::new(dirs.config_dir().to_path_buf());

    match action {
        TrackAction::Add {
            ledger,
            remote,
            remote_alias,
        } => {
            run_add(
                &store,
                &ledger,
                remote.as_deref(),
                remote_alias.as_deref(),
                dirs,
            )
            .await
        }
        TrackAction::Remove { ledger } => {
            let normalized = crate::context::to_ledger_id(&ledger);
            run_remove(&store, &normalized)
        }
        TrackAction::List => run_list(&store),
        TrackAction::Status { ledger } => {
            let normalized = ledger.as_deref().map(crate::context::to_ledger_id);
            run_status(&store, normalized.as_deref()).await
        }
    }
}

async fn run_add(
    store: &TomlSyncConfigStore,
    ledger: &str,
    remote_name: Option<&str>,
    remote_alias: Option<&str>,
    dirs: &FlureeDir,
) -> CliResult<()> {
    // Resolve remote: explicit arg, or default if exactly one remote configured
    let remotes = store
        .list_remotes()
        .await
        .map_err(|e| CliError::Config(e.to_string()))?;

    let remote = match remote_name {
        Some(name) => {
            let rn = RemoteName::new(name);
            store
                .get_remote(&rn)
                .await
                .map_err(|e| CliError::Config(e.to_string()))?
                .ok_or_else(|| CliError::NotFound(format!("remote '{name}' not found")))?
        }
        None => {
            if remotes.is_empty() {
                return Err(CliError::Config(
                    "no remotes configured. Add one with `fluree remote add <name> <url>`"
                        .to_string(),
                ));
            }
            if remotes.len() > 1 {
                return Err(CliError::Usage(format!(
                    "multiple remotes configured ({}). Specify one with --remote <name>",
                    remotes
                        .iter()
                        .map(|r| r.name.as_str().to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                )));
            }
            remotes.into_iter().next().unwrap()
        }
    };

    // Normalize aliases to include branch (e.g., "test4" → "test4:main")
    // so resolution works with both "test4" and "test4:main".
    let local_alias = crate::context::to_ledger_id(ledger);
    let effective_remote_alias = crate::context::to_ledger_id(remote_alias.unwrap_or(ledger));

    // Check mutual exclusion: refuse if local ledger exists
    let fluree = crate::context::build_fluree(dirs)?;
    let local_ledger_id = &local_alias;
    if fluree.ledger_exists(local_ledger_id).await.unwrap_or(false) {
        return Err(CliError::Config(format!(
            "ledger '{local_alias}' already exists locally. \
             Remove it first, or use a different local alias with `--remote-alias`."
        )));
    }

    // Build client to validate ledger exists on remote
    let base_url = match &remote.endpoint {
        RemoteEndpoint::Http { base_url } => base_url.clone(),
        other => {
            return Err(CliError::Config(format!(
                "remote '{}' uses {:?} endpoint; tracking requires an HTTP remote",
                remote.name.as_str(),
                other
            )));
        }
    };

    let client = build_client_from_auth(&base_url, &remote.auth);

    // Check ledger exists on remote
    match client.ledger_exists(&effective_remote_alias).await {
        Ok(true) => {}
        Ok(false) => {
            return Err(CliError::NotFound(format!(
                "ledger '{}' not found on remote '{}'",
                effective_remote_alias,
                remote.name.as_str()
            )));
        }
        Err(e) => {
            return Err(CliError::Remote(format!(
                "failed to check ledger on remote '{}': {}",
                remote.name.as_str(),
                e
            )));
        }
    }

    // Check not already tracked
    if store.get_tracked(&local_alias).is_some() {
        // Replace existing tracking
        eprintln!(
            "{} replacing existing tracking for '{}'",
            "note:".cyan().bold(),
            local_alias
        );
    }

    let config = TrackedLedgerConfig {
        local_alias: local_alias.clone(),
        remote: remote.name.as_str().to_string(),
        remote_alias: effective_remote_alias.to_string(),
    };

    store.add_tracked(config)?;

    println!(
        "Tracking '{}' via remote '{}' ({})",
        local_alias.green(),
        remote.name.as_str().green(),
        effective_remote_alias
    );
    Ok(())
}

fn run_remove(store: &TomlSyncConfigStore, ledger: &str) -> CliResult<()> {
    let removed = store.remove_tracked(ledger)?;
    if removed {
        println!("Removed tracking for '{ledger}'");
    } else {
        return Err(CliError::NotFound(format!(
            "ledger '{ledger}' is not tracked"
        )));
    }
    Ok(())
}

fn run_list(store: &TomlSyncConfigStore) -> CliResult<()> {
    let tracked = store.tracked_ledgers();

    if tracked.is_empty() {
        println!("No tracked ledgers.");
        println!(
            "  {} fluree track add <ledger> --remote <name>",
            "hint:".cyan().bold()
        );
        return Ok(());
    }

    let mut table = Table::new();
    table.set_header(vec!["Local Alias", "Remote", "Remote Alias"]);

    for t in tracked {
        table.add_row(vec![
            Cell::new(&t.local_alias),
            Cell::new(&t.remote),
            Cell::new(&t.remote_alias),
        ]);
    }

    println!("{table}");
    Ok(())
}

async fn run_status(store: &TomlSyncConfigStore, ledger: Option<&str>) -> CliResult<()> {
    let tracked = match ledger {
        Some(alias) => {
            let t = store
                .get_tracked(alias)
                .ok_or_else(|| CliError::NotFound(format!("ledger '{alias}' is not tracked")))?;
            vec![t]
        }
        None => {
            let all = store.tracked_ledgers();
            if all.is_empty() {
                println!("No tracked ledgers.");
                return Ok(());
            }
            all
        }
    };

    let remotes = store
        .list_remotes()
        .await
        .map_err(|e| CliError::Config(e.to_string()))?;

    for t in &tracked {
        let remote = remotes
            .iter()
            .find(|r| r.name.as_str() == t.remote)
            .ok_or_else(|| {
                CliError::Config(format!(
                    "remote '{}' referenced by tracked ledger '{}' not found in config",
                    t.remote, t.local_alias
                ))
            })?;

        let base_url = match &remote.endpoint {
            RemoteEndpoint::Http { base_url } => base_url.clone(),
            _ => {
                eprintln!(
                    "  {} '{}' remote '{}' is not HTTP, skipping",
                    "warn:".yellow().bold(),
                    t.local_alias,
                    t.remote
                );
                continue;
            }
        };

        let client = build_client_from_auth(&base_url, &remote.auth);

        println!(
            "{} {} (via {})",
            "Ledger:".bold(),
            t.local_alias.green(),
            t.remote
        );

        match client.ledger_info(&t.remote_alias, None).await {
            Ok(info) => {
                if let Some(t_val) = info.get("t").and_then(serde_json::Value::as_i64) {
                    println!("  t: {t_val}");
                }
                if let Some(commit) = info
                    .get("commitId")
                    .and_then(|v| v.as_str())
                    .or_else(|| info.get("commit_head_id").and_then(|v| v.as_str()))
                {
                    println!("  commit: {commit}");
                }
                if let Some(index) = info
                    .get("indexId")
                    .and_then(|v| v.as_str())
                    .or_else(|| info.get("index_head_id").and_then(|v| v.as_str()))
                {
                    println!("  index:  {index}");
                }
                println!("  status: {}", "reachable".green());
            }
            Err(e) => {
                println!("  status: {} ({})", "unreachable".red(), e);
            }
        }

        if tracked.len() > 1 {
            println!();
        }
    }

    Ok(())
}
