use crate::cli::DroppedAction;
use crate::commands::drop::DropSummary;
use crate::context;
use crate::error::{CliError, CliResult};
use crate::remote_client::RemoteLedgerClient;
use comfy_table::{ContentArrangement, Table};
use fluree_db_api::admin::{DroppedLedgerInfo, DroppedLedgerState};
use fluree_db_api::server_defaults::FlureeDir;
use fluree_db_api::Fluree;
use serde::Deserialize;

pub async fn run(action: DroppedAction, dirs: &FlureeDir, direct: bool) -> CliResult<()> {
    match action {
        DroppedAction::List { remote } => {
            let target = Target::resolve(dirs, remote.as_deref(), direct).await?;
            let result = list(&target).await;
            target.finish(dirs).await;
            result
        }
        DroppedAction::Restore { instance, remote } => {
            let target = Target::resolve(dirs, remote.as_deref(), direct).await?;
            let result = restore(&target, &instance).await;
            target.finish(dirs).await;
            result
        }
        DroppedAction::Purge {
            instance,
            force,
            remote,
        } => {
            if !force {
                return Err(CliError::Usage(format!(
                    "use --force to confirm deletion of dropped ledger '{instance}'"
                )));
            }
            let target = Target::resolve(dirs, remote.as_deref(), direct).await?;
            let result = purge(&target, &instance).await;
            target.finish(dirs).await;
            result
        }
    }
}

/// Where a command runs: a server (a named remote, or the local server when
/// one is running), or the local store.
enum Target {
    Server {
        client: Box<RemoteLedgerClient>,
        remote_name: String,
    },
    Local(Box<Fluree>),
}

impl Target {
    async fn resolve(dirs: &FlureeDir, remote_flag: Option<&str>, direct: bool) -> CliResult<Self> {
        if let Some(remote_name) = remote_flag {
            return Ok(Target::Server {
                client: Box::new(context::build_remote_client(remote_name, dirs).await?),
                remote_name: remote_name.to_string(),
            });
        }
        if !direct {
            if let Some(client) = context::try_server_route_client(dirs) {
                return Ok(Target::Server {
                    client: Box::new(client),
                    remote_name: context::LOCAL_SERVER_REMOTE.to_string(),
                });
            }
        }
        Ok(Target::Local(Box::new(context::build_fluree(dirs)?)))
    }

    async fn finish(&self, dirs: &FlureeDir) {
        if let Target::Server {
            client,
            remote_name,
        } = self
        {
            context::persist_refreshed_tokens(client, remote_name, dirs).await;
        }
    }
}

/// One dropped ledger, as the server reports it.
#[derive(Deserialize)]
struct Dropped {
    instance: String,
    name: String,
    dropped_at: i64,
    state: String,
    branches: Vec<String>,
}

impl From<DroppedLedgerInfo> for Dropped {
    fn from(info: DroppedLedgerInfo) -> Self {
        Self {
            instance: info.instance.to_string(),
            name: info.name,
            dropped_at: info.dropped_at,
            state: match info.state {
                DroppedLedgerState::Dropped => "dropped",
                DroppedLedgerState::Restoring => "restoring",
                DroppedLedgerState::Purging => "purging",
            }
            .to_string(),
            branches: info.branches,
        }
    }
}

fn from_response<T: serde::de::DeserializeOwned>(value: serde_json::Value) -> CliResult<T> {
    serde_json::from_value(value)
        .map_err(|e| CliError::Remote(format!("unexpected dropped-ledger response: {e}")))
}

async fn list(target: &Target) -> CliResult<()> {
    let dropped: Vec<Dropped> = match target {
        Target::Server { client, .. } => {
            #[derive(Deserialize)]
            struct DroppedList {
                dropped: Vec<Dropped>,
            }
            from_response::<DroppedList>(client.list_dropped().await?)?.dropped
        }
        Target::Local(fluree) => fluree
            .list_dropped()
            .await?
            .into_iter()
            .map(Into::into)
            .collect(),
    };

    if dropped.is_empty() {
        println!("No dropped ledgers.");
        return Ok(());
    }

    let mut table = Table::new();
    table.set_content_arrangement(ContentArrangement::Dynamic);
    table.set_header(vec!["INSTANCE", "NAME", "DROPPED", "STATE", "BRANCHES"]);
    for d in &dropped {
        let dropped_at = chrono::DateTime::from_timestamp_millis(d.dropped_at)
            .map(|t| t.format("%Y-%m-%d %H:%M:%S UTC").to_string())
            .unwrap_or_else(|| d.dropped_at.to_string());
        table.add_row(vec![
            d.instance.clone(),
            d.name.clone(),
            dropped_at,
            d.state.clone(),
            d.branches.join(", "),
        ]);
    }
    println!("{table}");
    Ok(())
}

async fn restore(target: &Target, instance: &str) -> CliResult<()> {
    let restored: Dropped = match target {
        Target::Server { client, .. } => {
            from_response(client.dropped_action("restore", instance).await?)?
        }
        Target::Local(fluree) => fluree.restore_dropped(instance).await?.into(),
    };
    println!("Restored ledger '{}'", restored.name);
    Ok(())
}

async fn purge(target: &Target, instance: &str) -> CliResult<()> {
    let summary = match target {
        Target::Server { client, .. } => {
            DropSummary::from_response(instance, &client.dropped_action("purge", instance).await?)?
        }
        Target::Local(fluree) => DropSummary::from_report(fluree.purge_dropped(instance).await?),
    };
    summary.print_purged();
    Ok(())
}
