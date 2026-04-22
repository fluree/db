use crate::cli::PolicyArgs;
use crate::context::{self, LedgerMode};
use crate::detect;
use crate::error::{CliError, CliResult};
use crate::input;
use fluree_db_api::server_defaults::FlureeDir;
use fluree_db_api::CommitOpts;
use std::path::{Path, PathBuf};

/// Resolve positional args for insert/query/upsert/update commands.
///
/// Returns `(ledger_name, inline_input, file_path)`:
/// - 0 args: active ledger, no inline or file
/// - 1 arg: auto-detected as inline input (if it looks like a query/data),
///   file path (if the path exists), or ledger name (otherwise)
/// - 2 args: first is ledger name, second is inline input
pub fn resolve_positional_args(
    args: &[String],
) -> CliResult<(Option<&str>, Option<&str>, Option<PathBuf>)> {
    match args.len() {
        0 => Ok((None, None, None)),
        1 => {
            if looks_like_query(&args[0]) {
                // Inline query/data with active ledger
                Ok((None, Some(&args[0]), None))
            } else {
                let p = Path::new(&args[0]);
                if p.is_file() {
                    // Backwards compat: existing file path as positional arg
                    Ok((None, None, Some(p.to_path_buf())))
                } else {
                    // Ledger name
                    Ok((Some(&args[0]), None, None))
                }
            }
        }
        _ => {
            // 2 args: first = ledger, second = inline input
            Ok((Some(&args[0]), Some(&args[1]), None))
        }
    }
}

/// Heuristic: does this string look like a query or data literal rather than a
/// ledger name or file path?
fn looks_like_query(s: &str) -> bool {
    let trimmed = s.trim();
    // JSON-LD object or array
    if trimmed.starts_with('{') || trimmed.starts_with('[') {
        return true;
    }
    // Turtle directives
    if trimmed.starts_with("@prefix") || trimmed.starts_with("@base") {
        return true;
    }
    // IRI-based Turtle triples (e.g., "<http://...> a <http://...> .")
    if trimmed.starts_with('<') {
        return true;
    }
    // SPARQL keywords (case-insensitive)
    let first_word = trimmed.split_whitespace().next().unwrap_or("");
    matches!(
        first_word.to_ascii_uppercase().as_str(),
        "SELECT" | "ASK" | "CONSTRUCT" | "DESCRIBE" | "INSERT" | "DELETE" | "PREFIX" | "BASE"
    )
}

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
                detect::DataFormat::Turtle => client.insert_turtle(&remote_alias, &content).await?,
                detect::DataFormat::JsonLd => {
                    let json: serde_json::Value = serde_json::from_str(&content)?;
                    client.insert_jsonld(&remote_alias, &json).await?
                }
            };

            context::persist_refreshed_tokens(&client, &remote_name, dirs).await;

            // Display server response fields
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
                        .insert_turtle(&content)
                        .commit_opts(commit_opts);
                    if let Some(ctx) = policy_ctx {
                        b = b.policy(ctx);
                    }
                    b.commit().await?
                }
                detect::DataFormat::JsonLd => {
                    let json: serde_json::Value = serde_json::from_str(&content)?;
                    let mut b = graph.transact().insert(&json).commit_opts(commit_opts);
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

/// Build a `PolicyContext` from `PolicyArgs` against a freshly-loaded ledger state.
/// Returns `None` when no policy flags are set.
pub async fn build_policy_ctx(
    fluree: &fluree_db_api::Fluree,
    alias: &str,
    policy: &PolicyArgs,
) -> CliResult<Option<fluree_db_api::PolicyContext>> {
    if !policy.is_set() {
        return Ok(None);
    }
    let ledger = fluree.ledger(alias).await?;
    let opts = policy.to_options().map_err(CliError::Usage)?;
    let ctx = fluree_db_api::build_policy_context(
        &ledger.snapshot,
        ledger.novelty.as_ref(),
        Some(ledger.novelty.as_ref()),
        ledger.t(),
        &opts,
    )
    .await?;
    Ok(Some(ctx))
}

/// Print transaction result from remote server JSON response.
pub fn print_txn_result(result: &serde_json::Value) {
    // Print the full server response as pretty JSON
    println!(
        "{}",
        serde_json::to_string_pretty(result).unwrap_or_else(|_| result.to_string())
    );
}

/// Print a novelty warning to stderr if indexing is recommended.
///
/// Called after local commits to let users know when they should run
/// `fluree index` to clear novelty and maintain query performance.
pub fn warn_novelty_if_needed(indexing: &fluree_db_api::IndexingStatus) {
    if !indexing.needed {
        return;
    }

    let size_kb = indexing.novelty_size / 1024;
    let gap = indexing.commit_t - indexing.index_t;

    if indexing.enabled {
        // Background indexing is enabled but hasn't caught up yet
        eprintln!(
            "  {} novelty is {}KB ({} commits ahead of index); background indexing will catch up",
            colored::Colorize::bold(colored::Colorize::yellow("notice:")),
            size_kb,
            gap,
        );
    } else {
        eprintln!(
            "  {} novelty is {}KB ({} commits ahead of index); run `fluree index` to rebuild",
            colored::Colorize::bold(colored::Colorize::yellow("warning:")),
            size_kb,
            gap,
        );
    }
}
