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
use fluree_db_api::FormatterConfig;
use serde_json::Value as JsonValue;
use std::path::Path;
use std::sync::Arc;

/// Per-alias result format selected from `--format` (matches
/// `fluree query` semantics).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AliasFormat {
    /// Default JSON-LD / SPARQL Results JSON shapes per alias language.
    Json,
    /// `{ "@value": ..., "@type": ... }` typed literal wrappers.
    TypedJson,
}

impl AliasFormat {
    fn parse(raw: &str) -> CliResult<Self> {
        match raw.to_lowercase().as_str() {
            "json" => Ok(Self::Json),
            "typed-json" | "typed_json" | "typedjson" => Ok(Self::TypedJson),
            other => Err(CliError::Usage(format!(
                "unknown --format value '{other}'; valid: json, typed-json"
            ))),
        }
    }

    /// Build a [`FormatterConfig`] for the api-crate builder. Returns
    /// `None` when neither `--format typed-json` nor `--normalize-arrays`
    /// was supplied — in that case the api crate's per-language defaults
    /// (JSON-LD for JSON-LD aliases, SPARQL JSON for SPARQL aliases) are
    /// what the user wants.
    fn to_formatter_config(self, normalize_arrays: bool) -> Option<FormatterConfig> {
        match (self, normalize_arrays) {
            (Self::Json, false) => None,
            (Self::Json, true) => Some(FormatterConfig::jsonld().with_normalize_arrays()),
            (Self::TypedJson, false) => Some(FormatterConfig::typed_json()),
            (Self::TypedJson, true) => Some(FormatterConfig::typed_json().with_normalize_arrays()),
        }
    }
}

/// Envelope display selection from `--output`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EnvelopeView {
    Json,
    Pretty,
    Aliases,
}

impl EnvelopeView {
    fn parse(raw: &str) -> CliResult<Self> {
        match raw.to_lowercase().as_str() {
            "json" => Ok(Self::Json),
            "pretty" => Ok(Self::Pretty),
            "aliases" => Ok(Self::Aliases),
            other => Err(CliError::Usage(format!(
                "unknown --output value '{other}'; valid: json, pretty, aliases"
            ))),
        }
    }
}

/// Run the `multi-query` subcommand.
#[allow(clippy::too_many_arguments)]
pub async fn run(
    args: &[String],
    expr: Option<&str>,
    file_flag: Option<&Path>,
    format: &str,
    normalize_arrays: bool,
    output: &str,
    dirs: &FlureeDir,
    remote_flag: Option<&str>,
    direct: bool,
    policy: &PolicyArgs,
) -> CliResult<()> {
    // Reject unknown --format / --output values **before** the network
    // round-trip so a typo like `--format jzon` doesn't burn the
    // server-side work and the bearer's fuel.
    let alias_format = AliasFormat::parse(format)?;
    let envelope_view = EnvelopeView::parse(output)?;
    let formatter_config = alias_format.to_formatter_config(normalize_arrays);

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
    // Header pair sent with --remote / auto-route requests so the server
    // can build the matching FormatterConfig. Local (in-process) requests
    // build the config from `formatter_config` directly without going
    // through the header round-trip.
    let format_headers = format_request_headers(alias_format, normalize_arrays);

    let response = match remote_flag {
        Some(name) => {
            let client = context::build_remote_client(name, dirs).await?;
            // Attach CLI policy flags as request headers
            // (fluree-policy-class, fluree-policy, etc.). The server
            // folds these into the envelope's top-level opts before
            // validation via inject_headers_into_envelope.
            let client = client.with_policy(policy.clone());
            let response = client
                .multi_query_with_headers(&envelope, &format_headers)
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
                        .multi_query_with_headers(&envelope, &format_headers)
                        .await
                        .map_err(|e| CliError::Remote(format!("multi-query request failed: {e}")))?
                }
                None => run_in_process(dirs, envelope, formatter_config, policy).await?,
            }
        }
        None => run_in_process(dirs, envelope, formatter_config, policy).await?,
    };

    print_response(&response, envelope_view);
    Ok(())
}

/// Build the `fluree-output-format` / `fluree-normalize-arrays` headers
/// the server reads to construct the same [`FormatterConfig`] the
/// in-process path builds locally. Empty when the user picked the
/// per-language defaults — saves a round trip through the header
/// extractor.
fn format_request_headers(
    alias_format: AliasFormat,
    normalize_arrays: bool,
) -> Vec<(&'static str, String)> {
    let mut headers = Vec::new();
    match alias_format {
        AliasFormat::Json => {}
        AliasFormat::TypedJson => {
            headers.push(("fluree-output-format", "typed-json".to_string()));
        }
    }
    if normalize_arrays {
        headers.push(("fluree-normalize-arrays", "true".to_string()));
    }
    headers
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
    formatter_config: Option<FormatterConfig>,
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
    let mut builder = fluree.multi_query().envelope(envelope);
    if let Some(cfg) = formatter_config {
        builder = builder.format(cfg);
    }
    let response = builder.execute().await.map_err(map_multi_query_error)?;

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
        MultiQueryError::UnsupportedFormat { format } => CliError::Input(format!(
            "format {format:?} produces non-JSON output and cannot be used inside a multi-query envelope"
        )),
    }
}

fn print_response(response: &JsonValue, view: EnvelopeView) {
    match view {
        EnvelopeView::Json => {
            // Pass-through: compact JSON, single line. Machine-friendly.
            println!(
                "{}",
                serde_json::to_string(response).expect("serialize response")
            );
        }
        EnvelopeView::Pretty => {
            println!(
                "{}",
                serde_json::to_string_pretty(response).expect("serialize response")
            );
        }
        EnvelopeView::Aliases => print_per_alias(response),
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

    // -------------------------------------------------------------------------
    // --format / --output / --normalize-arrays parsing & header building
    // -------------------------------------------------------------------------

    #[test]
    fn alias_format_parses_json_and_typed_json_and_rejects_unknown() {
        assert_eq!(AliasFormat::parse("json").unwrap(), AliasFormat::Json);
        assert_eq!(
            AliasFormat::parse("typed-json").unwrap(),
            AliasFormat::TypedJson
        );
        assert_eq!(
            AliasFormat::parse("Typed_JSON").unwrap(),
            AliasFormat::TypedJson
        );
        let err = AliasFormat::parse("table").unwrap_err();
        assert!(matches!(err, CliError::Usage(_)), "got: {err:?}");
    }

    #[test]
    fn envelope_view_parses_each_variant_and_rejects_unknown() {
        assert_eq!(EnvelopeView::parse("json").unwrap(), EnvelopeView::Json);
        assert_eq!(EnvelopeView::parse("pretty").unwrap(), EnvelopeView::Pretty);
        assert_eq!(
            EnvelopeView::parse("aliases").unwrap(),
            EnvelopeView::Aliases
        );
        let err = EnvelopeView::parse("typed-json").unwrap_err();
        assert!(matches!(err, CliError::Usage(_)), "got: {err:?}");
    }

    #[test]
    fn alias_format_json_without_normalize_yields_none_config() {
        // `--format json` with no --normalize-arrays should keep per-language
        // defaults — passing None to the builder preserves JSON-LD / SPARQL
        // JSON shapes for the respective aliases.
        assert!(AliasFormat::Json.to_formatter_config(false).is_none());
    }

    #[test]
    fn alias_format_json_with_normalize_yields_jsonld_normalized() {
        let cfg = AliasFormat::Json
            .to_formatter_config(true)
            .expect("normalize-arrays implies a config");
        assert!(cfg.normalize_arrays);
        assert_eq!(cfg.format, fluree_db_api::OutputFormat::JsonLd);
    }

    #[test]
    fn alias_format_typed_json_with_and_without_normalize() {
        let bare = AliasFormat::TypedJson.to_formatter_config(false).unwrap();
        assert_eq!(bare.format, fluree_db_api::OutputFormat::TypedJson);
        assert!(!bare.normalize_arrays);

        let normalized = AliasFormat::TypedJson.to_formatter_config(true).unwrap();
        assert_eq!(normalized.format, fluree_db_api::OutputFormat::TypedJson);
        assert!(normalized.normalize_arrays);
    }

    #[test]
    fn format_request_headers_omits_defaults() {
        // `--format json` without `--normalize-arrays` is the default —
        // emitting no headers keeps the wire chatty-free.
        let headers = format_request_headers(AliasFormat::Json, false);
        assert!(headers.is_empty(), "got: {headers:?}");
    }

    #[test]
    fn format_request_headers_attaches_typed_json_and_normalize() {
        let headers = format_request_headers(AliasFormat::TypedJson, true);
        assert!(
            headers
                .iter()
                .any(|(k, v)| *k == "fluree-output-format" && v == "typed-json"),
            "got: {headers:?}"
        );
        assert!(
            headers
                .iter()
                .any(|(k, v)| *k == "fluree-normalize-arrays" && v == "true"),
            "got: {headers:?}"
        );
    }

    #[test]
    fn format_request_headers_normalize_alone_only_attaches_that_header() {
        // `--format json --normalize-arrays` should send the normalize
        // header on its own — the server reads it as "default JSON-LD,
        // with array normalization" the same way the local builder does.
        let headers = format_request_headers(AliasFormat::Json, true);
        assert_eq!(headers.len(), 1);
        assert_eq!(headers[0], ("fluree-normalize-arrays", "true".to_string()));
    }
}
