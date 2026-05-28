//! `fluree multi-query` — bundle multiple queries against a shared snapshot.
//!
//! Reads a multi-query envelope (JSON) from `-e`/`-f`/positional file/stdin,
//! POSTs it to a Fluree server's `/multi-query` endpoint via the existing
//! [`RemoteLedgerClient`] transport, and prints the response.
//!
//! Multi-query has no in-process / local-only execution path in v1: the
//! dispatcher lives in `fluree-db-server` and only the server exposes
//! `/multi-query`. The CLI therefore requires either an explicit
//! `--remote <name>` or an auto-detected locally-running `fluree server`.
//! If neither is available, surface a clear error pointing the user at
//! both options.

use crate::context::{self, try_server_route_client};
use crate::error::{CliError, CliResult};
use crate::input;
use colored::Colorize;
use fluree_db_api::server_defaults::FlureeDir;
use serde_json::Value as JsonValue;
use std::path::Path;

/// Run the `multi-query` subcommand.
pub async fn run(
    args: &[String],
    expr: Option<&str>,
    file_flag: Option<&Path>,
    format: &str,
    dirs: &FlureeDir,
    remote_flag: Option<&str>,
    direct: bool,
) -> CliResult<()> {
    // Positional arg, if present, is interpreted as a file path — same
    // shape as `fluree query <FILE>` with no inline / no -f.
    let positional_file = args.first().map(Path::new).filter(|p| {
        // Treat as a file only if it actually exists; otherwise let
        // the inline / stdin path take over with a clearer error.
        p.exists() && p.is_file()
    });

    let source = input::resolve_input(expr, None, file_flag, positional_file)?;
    let content = input::read_input(&source)?;

    let envelope: JsonValue = serde_json::from_str(&content).map_err(|e| {
        CliError::Input(format!(
            "envelope is not valid JSON: {e}\n  {} the multi-query envelope must be a JSON object — see docs/api/multi-query.md",
            "help:".cyan().bold()
        ))
    })?;

    if !envelope.is_object() {
        return Err(CliError::Input(
            "multi-query envelope must be a JSON object".to_string(),
        ));
    }

    // Resolve transport: --remote takes precedence; otherwise auto-route to
    // a locally running `fluree server`. --direct disables auto-routing
    // and forces an error in the no-remote case since multi-query has no
    // in-process execution path.
    //
    // remote_name carries the named-remote slug only when we need to
    // persist refreshed OAuth tokens after the round-trip — the
    // auto-route (local server) and unauthenticated paths skip
    // persistence because the local server doesn't require a token.
    let (client, remote_name) = match remote_flag {
        Some(name) => (
            context::build_remote_client(name, dirs).await?,
            Some(name.to_string()),
        ),
        None => {
            if direct {
                return Err(CliError::Usage(
                    "multi-query has no in-process execution path; \
                     pass --remote <name> or drop --direct to auto-route through a running local server"
                        .to_string(),
                ));
            }
            match try_server_route_client(dirs) {
                Some(c) => (c, None),
                None => return Err(no_transport_error()),
            }
        }
    };

    let response = client
        .multi_query(&envelope)
        .await
        .map_err(|e| CliError::Remote(format!("multi-query request failed: {e}")))?;

    // Persist any OIDC token refresh that happened silently during the
    // round-trip back to config.toml. Without this, a successful
    // multi-query leaves the remote's stored credentials stale —
    // next command refreshes again, or fails outright if the refresh
    // token has rotated. The single-query path does the same after
    // every remote call.
    if let Some(name) = remote_name.as_deref() {
        context::persist_refreshed_tokens(&client, name, dirs).await;
    }

    print_response(&response, format)?;
    Ok(())
}

fn no_transport_error() -> CliError {
    CliError::Usage(format!(
        "multi-query requires a Fluree server to execute against.\n  \
         {} either pass {} or start a local server with `fluree server start`",
        "help:".cyan().bold(),
        "--remote <name>".bold()
    ))
}

fn print_response(response: &JsonValue, format: &str) -> CliResult<()> {
    match format.to_lowercase().as_str() {
        "json" => {
            // Pass-through: compact JSON, single line. Matches default
            // `fluree query --format json` shape (machine-friendly).
            println!(
                "{}",
                serde_json::to_string(response).expect("serialize response")
            );
        }
        "pretty" => {
            println!(
                "{}",
                serde_json::to_string_pretty(response).expect("serialize response")
            );
        }
        "aliases" => {
            print_per_alias(response);
        }
        other => {
            return Err(CliError::Usage(format!(
                "unknown output format '{other}'; valid formats: json, pretty, aliases"
            )));
        }
    }
    Ok(())
}

/// Per-alias section view: status header, snapshot summary, then each
/// alias's result block; failed aliases printed with an error header.
fn print_per_alias(response: &JsonValue) {
    let status = response
        .get("status")
        .and_then(JsonValue::as_str)
        .unwrap_or("?");
    let status_color = match status {
        "ok" => status.green().bold(),
        "partial" => status.yellow().bold(),
        "all_failed" => status.red().bold(),
        _ => status.bold(),
    };
    eprintln!("status: {status_color}");

    if let Some(snapshot) = response.get("snapshot") {
        if let Some(as_of) = snapshot.get("asOf").and_then(JsonValue::as_str) {
            eprintln!("asOf:   {}", as_of.dimmed());
        }
        if let Some(ledgers) = snapshot.get("ledgers").and_then(JsonValue::as_object) {
            for (ledger, t) in ledgers {
                eprintln!("  {} @ t:{}", ledger.cyan(), t);
            }
        }
    }
    eprintln!();

    let results = response.get("results").and_then(JsonValue::as_object);
    let errors = response.get("errors").and_then(JsonValue::as_object);

    if let Some(results) = results {
        for (alias, data) in results {
            println!("# {} (ok)", alias.green().bold());
            println!(
                "{}",
                serde_json::to_string_pretty(data).expect("serialize alias result")
            );
            println!();
        }
    }
    if let Some(errors) = errors {
        for (alias, err) in errors {
            let code = err.get("code").and_then(JsonValue::as_str).unwrap_or("?");
            println!("# {} ({})", alias.red().bold(), code);
            println!(
                "{}",
                serde_json::to_string_pretty(err).expect("serialize alias error")
            );
            println!();
        }
    }
}
