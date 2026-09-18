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

            // Shared with `branch create --at` and `branch revert` (#1805) —
            // this used to be a private `t:`-or-prefix hand-roll, so `show 2`
            // was rejected while `branch create --at t:2` was not.
            let graph = fluree.graph(&ledger_id);
            let builder = match fluree_db_api::CommitRef::parse(commit).map_err(CliError::Api)? {
                fluree_db_api::CommitRef::T(t) => graph.commit_t(t),
                fluree_db_api::CommitRef::Prefix(prefix) => graph.commit_prefix(&prefix),
                // `commit_prefix` re-parses a canonical CID into the same
                // `ContentId` this arm already holds.
                fluree_db_api::CommitRef::Exact(cid) => graph.commit_prefix(&cid.to_string()),
            };
            let detail = builder.execute().await.map_err(CliError::Api)?;

            let json = serde_json::to_string_pretty(&detail)
                .map_err(|e| CliError::Input(format!("JSON serialization failed: {e}")))?;
            println!("{json}");
        }
    }

    Ok(())
}
