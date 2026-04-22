use crate::cli::PolicyArgs;
use crate::commands::insert::{
    build_policy_ctx, print_txn_result, resolve_positional_args, warn_novelty_if_needed,
};
use crate::context::{self, LedgerMode};
use crate::detect;
use crate::error::CliResult;
use crate::input;
use fluree_db_api::server_defaults::FlureeDir;
use fluree_db_api::CommitOpts;
use std::path::Path;

#[allow(clippy::too_many_arguments)]
pub async fn run(
    args: &[String],
    expr: Option<&str>,
    file_flag: Option<&Path>,
    format_flag: Option<&str>,
    dirs: &FlureeDir,
    remote_flag: Option<&str>,
    direct: bool,
    policy: &PolicyArgs,
) -> CliResult<()> {
    let (explicit_ledger, positional_inline, positional_file) = resolve_positional_args(args)?;

    // Resolve input: -e > positional inline > -f > positional file > stdin
    let source = input::resolve_input(
        expr,
        positional_inline,
        file_flag,
        positional_file.as_deref(),
    )?;
    let content = input::read_input(&source)?;

    // For format detection, prefer the -f path, then positional file
    let detect_path = file_flag.or(positional_file.as_deref());
    let data_format = detect::detect_data_format(detect_path, &content, format_flag)?;

    // Resolve ledger mode: --remote flag, local, tracked, or auto-route to local server
    let mode = if let Some(remote_name) = remote_flag {
        let alias = context::resolve_ledger(explicit_ledger, dirs)?;
        context::build_remote_mode(remote_name, &alias, dirs).await?
    } else {
        let mode = context::resolve_ledger_mode(explicit_ledger, dirs).await?;
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
            let client = client.with_policy(policy.clone());
            let result = match data_format {
                detect::DataFormat::Turtle => client.upsert_turtle(&remote_alias, &content).await?,
                detect::DataFormat::JsonLd => {
                    let json: serde_json::Value = serde_json::from_str(&content)?;
                    client.upsert_jsonld(&remote_alias, &json).await?
                }
            };

            context::persist_refreshed_tokens(&client, &remote_name, dirs).await;

            print_txn_result(&result);
        }
        LedgerMode::Local { fluree, alias } => {
            let commit_opts = CommitOpts::default();

            let policy_ctx = build_policy_ctx(&fluree, &alias, policy).await?;
            let graph = fluree.graph(&alias);

            let result = match data_format {
                detect::DataFormat::Turtle => {
                    let mut b = graph
                        .transact()
                        .upsert_turtle(&content)
                        .commit_opts(commit_opts);
                    if let Some(ctx) = policy_ctx {
                        b = b.policy(ctx);
                    }
                    b.commit().await?
                }
                detect::DataFormat::JsonLd => {
                    let json: serde_json::Value = serde_json::from_str(&content)?;
                    let mut b = graph.transact().upsert(&json).commit_opts(commit_opts);
                    if let Some(ctx) = policy_ctx {
                        b = b.policy(ctx);
                    }
                    b.commit().await?
                }
            };

            println!(
                "Committed t={}, {} flakes",
                result.receipt.t, result.receipt.flake_count
            );
            warn_novelty_if_needed(&result.indexing);
        }
    }

    Ok(())
}
