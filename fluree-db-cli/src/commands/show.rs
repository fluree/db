//! `fluree show <commit>` — display a decoded commit with resolved IRIs.

use crate::context::{self, LedgerMode};
use crate::error::{CliError, CliResult};
use fluree_db_api::server_defaults::FlureeDir;

pub async fn run(
    commit: &str,
    ledger: Option<&str>,
    dirs: &FlureeDir,
    remote_flag: Option<&str>,
    direct: bool,
) -> CliResult<()> {
    let mode = if let Some(remote_name) = remote_flag {
        let alias = context::resolve_ledger(ledger, dirs)?;
        context::build_remote_mode(remote_name, &alias, dirs).await?
    } else {
        let m = context::resolve_ledger_mode(ledger, dirs).await?;
        if direct {
            m
        } else {
            context::try_server_route(m, dirs)
        }
    };

    match mode {
        LedgerMode::Tracked {
            client,
            remote_alias,
            remote_name,
            ..
        } => {
            let detail = client.commit_show(&remote_alias, commit).await?;

            context::persist_refreshed_tokens(&client, &remote_name, dirs).await;

            let json = serde_json::to_string_pretty(&detail)
                .map_err(|e| CliError::Input(format!("JSON serialization failed: {e}")))?;
            println!("{json}");
        }
        LedgerMode::Local { fluree, alias } => {
            let ledger_id = context::to_ledger_id(&alias);

            let detail = if let Some(t_str) = commit.strip_prefix("t:") {
                let t: i64 = t_str.parse().map_err(|_| {
                    CliError::Input(format!("Invalid transaction number: '{t_str}'"))
                })?;
                fluree
                    .graph(&ledger_id)
                    .commit_t(t)
                    .execute()
                    .await
                    .map_err(CliError::Api)?
            } else {
                fluree
                    .graph(&ledger_id)
                    .commit_prefix(commit)
                    .execute()
                    .await
                    .map_err(CliError::Api)?
            };

            let json = serde_json::to_string_pretty(&detail)
                .map_err(|e| CliError::Input(format!("JSON serialization failed: {e}")))?;
            println!("{json}");
        }
    }

    Ok(())
}
