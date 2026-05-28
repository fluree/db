//! `fluree multi-query` — bundle multiple queries against a shared snapshot.
//!
//! Reads a multi-query envelope (JSON) from `-e`/`-f`/positional file/stdin,
//! routes it to one of three transports depending on flags and
//! environment, and prints the response.
//!
//! Transport priority:
//!
//! 1. `--remote <name>` — explicit named remote (HTTP).
//! 2. Locally-running `fluree server` auto-route — when `server.meta.json`
//!    reports a live pid and `--direct` is not set.
//! 3. **In-process local mode** — calls `Fluree::multi_query()` against
//!    the local storage tree. Used when no `--remote` is supplied, no
//!    auto-route is available, and `--direct` is set (or no server
//!    metadata is present at all). This is the natural counterpart to
//!    `fluree query` running locally without a server.

use crate::cli::PolicyArgs;
use crate::context::{self, try_server_route_client};
use crate::error::{CliError, CliResult};
use crate::input;
use colored::Colorize;
use fluree_db_api::query::multi::MultiQueryRequest;
use fluree_db_api::query::multi_dispatch::MultiQueryError;
use fluree_db_api::server_defaults::FlureeDir;
use serde_json::Value as JsonValue;
use std::path::Path;
use std::sync::Arc;

/// Run the `multi-query` subcommand.
#[allow(clippy::too_many_arguments)]
pub async fn run(
    args: &[String],
    expr: Option<&str>,
    file_flag: Option<&Path>,
    format: &str,
    dirs: &FlureeDir,
    remote_flag: Option<&str>,
    direct: bool,
    policy: &PolicyArgs,
) -> CliResult<()> {
    // Reject unknown --format values **before** the network round-trip
    // so a typo like `--format jzon` doesn't burn the server-side work
    // and the bearer's fuel.
    validate_format(format)?;

    // Positional arg, if present, is always interpreted as a file path
    // — the only other shape multi-query accepts via positional would be
    // inline JSON, but inline goes through -e to disambiguate. If the
    // file doesn't exist, `read_input` surfaces a clear "failed to read
    // <path>: No such file or directory" rather than silently falling
    // through to stdin (which the previous existence filter caused under
    // a piped shell).
    let positional_file = args.first().map(Path::new);

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

    // Resolve transport. Priority:
    //   1. --remote <name> -> named HTTP remote
    //   2. Auto-route to a running local server (skipped by --direct)
    //   3. In-process local execution via Fluree::multi_query()
    //
    // The remote_name slug is captured only on the --remote path so we
    // can persist any silent OIDC token refresh back to config.toml
    // after the round-trip. Auto-route and local-in-process paths skip
    // persistence (no remote credentials to update).
    let response = match remote_flag {
        Some(name) => {
            let client = context::build_remote_client(name, dirs).await?;
            // Attach CLI policy flags as request headers
            // (fluree-policy-class, fluree-policy, etc.). The server
            // folds these into the envelope's top-level opts before
            // validation via inject_headers_into_envelope.
            let client = client.with_policy(policy.clone());
            let response = client
                .multi_query(&envelope)
                .await
                .map_err(|e| CliError::Remote(format!("multi-query request failed: {e}")))?;
            // Persist refreshed OAuth tokens back to config.toml so the
            // next command sees them; matches fluree query --remote.
            context::persist_refreshed_tokens(&client, name, dirs).await;
            response
        }
        None if !direct => {
            // Auto-route through local server if one is running;
            // fall back to in-process when none is found.
            match try_server_route_client(dirs) {
                Some(client) => {
                    let client = client.with_policy(policy.clone());
                    client
                        .multi_query(&envelope)
                        .await
                        .map_err(|e| CliError::Remote(format!("multi-query request failed: {e}")))?
                }
                None => run_in_process(dirs, envelope, policy).await?,
            }
        }
        None => run_in_process(dirs, envelope, policy).await?,
    };

    print_response(&response, format);
    Ok(())
}

/// Execute the envelope in-process via [`Fluree::multi_query`] against
/// the local storage tree configured for this `FlureeDir`.
///
/// CLI policy flags are folded into the envelope's top-level `opts` so
/// the standard envelope → sub-query opts merge carries them into every
/// JSON-LD alias, matching the server-side header injection step. No
/// per-sub-query impersonation gate runs here because there is no
/// bearer identity to elevate from — the caller already has direct
/// authority over the local storage.
async fn run_in_process(
    dirs: &FlureeDir,
    envelope_json: JsonValue,
    policy: &PolicyArgs,
) -> CliResult<JsonValue> {
    let mut envelope: MultiQueryRequest = serde_json::from_value(envelope_json).map_err(|e| {
        CliError::Input(format!(
            "envelope is not a valid multi-query envelope: {e}\n  {} see docs/api/multi-query.md",
            "help:".cyan().bold()
        ))
    })?;
    inject_policy_into_envelope(&mut envelope, policy)?;

    let fluree = Arc::new(context::build_fluree(dirs)?);
    let response = fluree
        .multi_query()
        .envelope(envelope)
        .execute()
        .await
        .map_err(map_multi_query_error)?;

    serde_json::to_value(&response)
        .map_err(|e| CliError::Input(format!("failed to serialize multi-query response: {e}")))
}

fn inject_policy_into_envelope(
    envelope: &mut MultiQueryRequest,
    policy: &PolicyArgs,
) -> CliResult<()> {
    if !policy.is_set() {
        return Ok(());
    }

    // Pre-resolve --policy / --policy-file and --policy-values /
    // --policy-values-file from the typed PolicyArgs so we don't have to
    // re-parse them. Same accessors single-query reuses.
    let resolved_policy = policy.resolve_policy().map_err(CliError::Input)?;
    let resolved_values = policy.resolve_policy_values().map_err(CliError::Input)?;

    let mut opts = envelope
        .opts
        .take()
        .unwrap_or_else(|| JsonValue::Object(serde_json::Map::new()));
    let obj = opts
        .as_object_mut()
        .ok_or_else(|| CliError::Input("envelope opts must be a JSON object".to_string()))?;

    // Default-vs-override rule (matches FlureeHeaders::inject_into_opts
    // server-side): each CLI flag injects ONLY when the envelope's opts
    // doesn't already carry that key, so explicit envelope opts always
    // win over CLI flags.

    if let Some(id) = policy.identity.as_ref() {
        obj.entry("identity")
            .or_insert_with(|| JsonValue::String(id.clone()));
    }

    // policy-class is ALWAYS an array — typed Vec<String> from the
    // PolicyArgs flag keeps every value, matching the server-side
    // `FlureeHeaders.policy_class` shape. The previous code went
    // through `policy_headers`, which flattened the Vec into one
    // tuple per class and the dedup loop dropped all but the first.
    if !policy.policy_class.is_empty() && !obj.contains_key("policy-class") {
        obj.insert(
            "policy-class".to_string(),
            JsonValue::Array(
                policy
                    .policy_class
                    .iter()
                    .cloned()
                    .map(JsonValue::String)
                    .collect(),
            ),
        );
    }

    if let Some(p) = resolved_policy {
        obj.entry("policy").or_insert(p);
    }

    if let Some(values_map) = resolved_values {
        let as_object: serde_json::Map<String, JsonValue> = values_map.into_iter().collect();
        obj.entry("policy-values")
            .or_insert_with(|| JsonValue::Object(as_object));
    }

    if policy.default_allow
        && !obj.contains_key("default-allow")
        && !obj.contains_key("default_allow")
        && !obj.contains_key("defaultAllow")
    {
        obj.insert("default-allow".to_string(), JsonValue::Bool(true));
    }

    envelope.opts = Some(opts);
    Ok(())
}

fn map_multi_query_error(err: MultiQueryError) -> CliError {
    match err {
        MultiQueryError::Validation(e) => CliError::Input(e.to_string()),
        MultiQueryError::Snapshot(e) => CliError::Input(format!("snapshot resolution failed: {e}")),
        MultiQueryError::ResponseAssembly(e) => CliError::Input(e.to_string()),
        MultiQueryError::EnvelopeRequired => {
            CliError::Input("envelope was not provided to the dispatcher".to_string())
        }
    }
}

/// Reject unsupported `--format` values up front, before any envelope
/// parsing or network round-trip. Keeps the typo cost cheap — local
/// usage error instead of a successful server-side multi-query whose
/// result we can't print.
fn validate_format(format: &str) -> CliResult<()> {
    match format.to_lowercase().as_str() {
        "json" | "pretty" | "aliases" => Ok(()),
        other => Err(CliError::Usage(format!(
            "unknown output format '{other}'; valid formats: json, pretty, aliases"
        ))),
    }
}

fn print_response(response: &JsonValue, format: &str) {
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
        "aliases" => print_per_alias(response),
        // Unreachable: `validate_format` is called at command entry
        // before any of the work that produces a response, so an
        // invalid value can never reach this branch.
        other => unreachable!("unvalidated format '{other}' reached print_response"),
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn envelope_with_dummy_query() -> MultiQueryRequest {
        // Construct via deserialize so the test doesn't need to know
        // about the api crate's internal IndexMap type. The envelope
        // requires at least one sub-query to deserialize successfully;
        // tests below only inspect opts, so the body is unused.
        serde_json::from_value(serde_json::json!({
            "queries": {
                "q": {
                    "language": "jsonld",
                    "query": { "from": "x", "select": ["?s"], "where": { "@id": "?s" } }
                }
            }
        }))
        .expect("envelope_with_dummy_query")
    }

    #[test]
    fn inject_policy_preserves_all_repeated_policy_class_values() {
        // Regression: `--policy-class A --policy-class B` previously
        // walked through `policy_headers` which emitted two tuples
        // sharing the same opts key; the dedup-on-insert loop dropped
        // 'B'. Direct injection from the typed Vec preserves both.
        let mut envelope = envelope_with_dummy_query();
        let policy = PolicyArgs {
            policy_class: vec!["ex:Admin".into(), "ex:Reader".into()],
            ..Default::default()
        };
        inject_policy_into_envelope(&mut envelope, &policy).unwrap();
        let opts = envelope.opts.unwrap();
        let arr = opts["policy-class"].as_array().unwrap();
        let values: Vec<&str> = arr.iter().filter_map(JsonValue::as_str).collect();
        assert_eq!(values, vec!["ex:Admin", "ex:Reader"]);
    }

    #[test]
    fn inject_policy_envelope_opts_win_over_cli_flags() {
        // Explicit envelope opts take precedence over CLI flags, same
        // default-vs-override rule the server-side header injection
        // uses.
        let mut envelope = envelope_with_dummy_query();
        envelope.opts = Some(serde_json::json!({
            "policy-class": ["env:Override"],
            "identity":     "env:Identity"
        }));
        let policy = PolicyArgs {
            identity: Some("cli:Identity".into()),
            policy_class: vec!["cli:Admin".into()],
            ..Default::default()
        };
        inject_policy_into_envelope(&mut envelope, &policy).unwrap();
        let opts = envelope.opts.unwrap();
        let arr = opts["policy-class"].as_array().unwrap();
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0].as_str().unwrap(), "env:Override");
        assert_eq!(opts["identity"].as_str().unwrap(), "env:Identity");
    }

    #[test]
    fn inject_policy_single_policy_class_still_array() {
        // Single --policy-class value also emits an array (matches the
        // server-side FlureeHeaders::inject_into_opts shape).
        let mut envelope = envelope_with_dummy_query();
        let policy = PolicyArgs {
            policy_class: vec!["ex:Admin".into()],
            ..Default::default()
        };
        inject_policy_into_envelope(&mut envelope, &policy).unwrap();
        let opts = envelope.opts.unwrap();
        assert!(opts["policy-class"].is_array());
        assert_eq!(opts["policy-class"][0].as_str().unwrap(), "ex:Admin");
    }

    #[test]
    fn inject_policy_default_allow_only_when_true() {
        let mut envelope = envelope_with_dummy_query();
        let policy = PolicyArgs {
            default_allow: false,
            // Without other flags this would be is_set() == false; add
            // an identity so injection runs.
            identity: Some("ex:alice".into()),
            ..Default::default()
        };
        inject_policy_into_envelope(&mut envelope, &policy).unwrap();
        let opts = envelope.opts.unwrap();
        assert!(opts.as_object().unwrap().get("default-allow").is_none());
    }

    #[test]
    fn inject_policy_skips_entirely_when_no_flags_set() {
        let mut envelope = envelope_with_dummy_query();
        let policy = PolicyArgs::default();
        inject_policy_into_envelope(&mut envelope, &policy).unwrap();
        assert!(envelope.opts.is_none());
    }
}
