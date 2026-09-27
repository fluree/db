use crate::config;
use crate::context;
use crate::error::{CliError, CliResult};
use crate::remote_client::RemoteLedgerClient;
use fluree_db_api::admin::{DropReport, DropStatus, DroppedData, GraphSourceDropReport};
use fluree_db_api::server_defaults::FlureeDir;
use fluree_db_api::DropMode;
use serde_json::Value;

pub async fn run(
    name: &str,
    hard: bool,
    force: bool,
    dirs: &FlureeDir,
    remote_flag: Option<&str>,
    direct: bool,
) -> CliResult<()> {
    if hard && !force {
        return Err(CliError::Usage(format!(
            "use --force with --hard to confirm permanent deletion of '{name}'"
        )));
    }

    if let Some(remote_name) = remote_flag {
        let client = context::build_remote_client(remote_name, dirs).await?;
        let result = run_remote(name, hard, &client).await;
        context::persist_refreshed_tokens(&client, remote_name, dirs).await;
        return result;
    }

    if !direct {
        if let Some(client) = context::try_server_route_client(dirs) {
            let result = run_remote(name, hard, &client).await;
            context::persist_refreshed_tokens(&client, context::LOCAL_SERVER_REMOTE, dirs).await;
            // Auto-route operates against the same on-disk storage as `--direct`,
            // so a successful drop must also clear the local active-ledger pointer
            // to avoid leaving CLI state pointing at a deleted ledger.
            if result.is_ok() {
                clear_if_active(name, dirs)?;
            }
            return result;
        }
    }

    run_local(name, hard, dirs).await
}

async fn run_remote(name: &str, hard: bool, client: &RemoteLedgerClient) -> CliResult<()> {
    let response = client
        .drop_resource(name, hard)
        .await
        .map_err(|e| CliError::Remote(format!("failed to drop '{name}': {e}")))?;
    DropSummary::from_response(name, &response)?.print(name)
}

async fn run_local(name: &str, hard: bool, dirs: &FlureeDir) -> CliResult<()> {
    let fluree = context::build_fluree(dirs)?;
    let mode = if hard { DropMode::Hard } else { DropMode::Soft };

    let report = fluree.drop_ledger(name, mode).await?;
    let summary = if matches!(report.status, DropStatus::NotFound) {
        let gs_report = fluree.drop_graph_source(name, None, mode).await?;
        DropSummary::from_graph_source(gs_report)
    } else {
        if matches!(report.status, DropStatus::Dropped) {
            clear_if_active(name, dirs)?;
        }
        DropSummary::from_report(report)
    };
    summary.print(name)
}

fn clear_if_active(name: &str, dirs: &FlureeDir) -> CliResult<()> {
    let active = config::read_active_ledger(dirs.data_dir());
    if active.as_deref() == Some(name) {
        config::clear_active_ledger(dirs.data_dir())?;
    }
    Ok(())
}

/// A drop or purge outcome, from a server response or the local API.
pub(crate) struct DropSummary {
    id: String,
    graph_source: bool,
    status: DropStatus,
    files_deleted: usize,
    branches: usize,
    /// A ledger created before name bindings has none.
    instance: Option<String>,
    data: Option<DroppedData>,
    warnings: Vec<String>,
}

impl DropSummary {
    pub(crate) fn from_report(report: DropReport) -> Self {
        Self {
            id: report.ledger_id,
            graph_source: false,
            status: report.status,
            files_deleted: report.artifacts_deleted,
            branches: report.branch_reports.len(),
            instance: report.instance.map(|i| i.to_string()),
            data: report.data,
            warnings: report.warnings,
        }
    }

    fn from_graph_source(report: GraphSourceDropReport) -> Self {
        Self {
            id: format!("{}:{}", report.name, report.branch),
            graph_source: true,
            status: report.status,
            files_deleted: report.files_deleted,
            branches: 0,
            instance: None,
            data: None,
            warnings: report.warnings,
        }
    }

    /// Reads a `/drop` or `/dropped/purge` response. Only a ledger's response
    /// carries `name_released`.
    pub(crate) fn from_response(name: &str, response: &Value) -> CliResult<Self> {
        let status = match response.get("status").and_then(Value::as_str) {
            Some("dropped" | "purged") => DropStatus::Dropped,
            Some("already_retracted") => DropStatus::AlreadyRetracted,
            Some("not_found") => DropStatus::NotFound,
            Some(other) => {
                return Err(CliError::Remote(format!(
                    "unexpected drop status '{other}'"
                )))
            }
            None => {
                return Err(CliError::Remote(
                    "unexpected drop response: missing status".into(),
                ))
            }
        };
        let strings = |field: &str| -> Vec<String> {
            response
                .get(field)
                .and_then(Value::as_array)
                .map(|values| {
                    values
                        .iter()
                        .filter_map(|v| v.as_str().map(str::to_string))
                        .collect()
                })
                .unwrap_or_default()
        };
        Ok(Self {
            id: response
                .get("ledger_id")
                .and_then(Value::as_str)
                .unwrap_or(name)
                .to_string(),
            graph_source: response.get("name_released").is_none(),
            status,
            files_deleted: response
                .get("files_deleted")
                .and_then(Value::as_u64)
                .unwrap_or(0) as usize,
            branches: strings("branches_dropped").len(),
            instance: response
                .get("instance")
                .and_then(Value::as_str)
                .map(str::to_string),
            data: match response.get("data").and_then(Value::as_str) {
                Some("retained") => Some(DroppedData::Retained),
                Some("deleted") => Some(DroppedData::Deleted),
                Some("deleting") => Some(DroppedData::Deleting),
                _ => None,
            },
            warnings: strings("warnings"),
        })
    }

    fn kind(&self) -> &'static str {
        if self.graph_source {
            "graph source"
        } else {
            "ledger"
        }
    }

    fn deleted_suffix(&self) -> String {
        match (self.files_deleted, self.branches) {
            (0, _) => String::new(),
            (n, 0 | 1) => format!(" (deleted {n} artifacts)"),
            (n, b) => format!(" (deleted {n} artifacts across {b} branches)"),
        }
    }

    fn print(&self, name: &str) -> CliResult<()> {
        let kind = self.kind();
        let id = &self.id;
        match self.status {
            DropStatus::NotFound => {
                return Err(CliError::NotFound(format!(
                    "'{name}' not found; `fluree dropped list` shows dropped ledgers"
                )))
            }
            DropStatus::AlreadyRetracted => println!("The {kind} '{id}' was already dropped"),
            DropStatus::Dropped => {
                println!("Dropped {kind} '{id}'{}", self.deleted_suffix());
                self.print_data_hint(name);
            }
        }
        self.print_warnings();
        Ok(())
    }

    /// Prints the outcome of `fluree dropped purge`.
    pub(crate) fn print_purged(&self) {
        println!(
            "Purged dropped ledger '{}'{}",
            self.id,
            self.deleted_suffix()
        );
        self.print_data_hint(&self.id);
        self.print_warnings();
    }

    fn print_data_hint(&self, name: &str) {
        match (self.data, &self.instance) {
            (Some(DroppedData::Retained), Some(instance)) => println!(
                "Its data is kept: restore it with `fluree dropped restore {instance}`, \
                 or delete it with `fluree dropped purge {instance} --force`."
            ),
            (Some(DroppedData::Retained), None) => println!(
                "Its data is kept and its name stays reserved; \
                 `fluree drop {name} --hard --force` deletes it."
            ),
            (Some(DroppedData::Deleting), Some(instance)) => eprintln!(
                "warning: not all of its data was deleted; \
                 `fluree dropped purge {instance} --force` finishes it"
            ),
            (Some(DroppedData::Deleting), None) => eprintln!(
                "warning: not all of its data was deleted; \
                 `fluree drop {name} --hard --force` finishes it"
            ),
            _ => {}
        }
    }

    fn print_warnings(&self) {
        for warning in &self.warnings {
            eprintln!("  warning: {warning}");
        }
    }
}
