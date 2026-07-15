use crate::context::{self, LedgerMode};
use crate::error::{CliError, CliResult};
use fluree_db_api::server_defaults::FlureeDir;

pub async fn run(
    ledger: Option<&str>,
    dirs: &FlureeDir,
    remote_flag: Option<&str>,
    direct: bool,
    graph: Option<&str>,
) -> CliResult<()> {
    // Reject ledger#fragment syntax — use --graph instead
    if let Some(l) = ledger {
        if l.contains('#') {
            return Err(CliError::Usage(
                "info does not support 'ledger#fragment' syntax; use --graph <name|IRI> to scope stats to a named graph"
                    .to_string(),
            ));
        }
    }

    // Resolve ledger mode: --remote flag, local, tracked, or auto-route to local server.
    // If resolution fails (not found), try graph source lookup before giving up.
    let mode = if let Some(remote_name) = remote_flag {
        let alias = context::resolve_ledger(ledger, dirs)?;
        Ok(context::build_remote_mode(remote_name, &alias, dirs).await?)
    } else {
        let mode = context::resolve_ledger_mode(ledger, dirs).await;
        match mode {
            Ok(m) => Ok(if direct {
                m
            } else {
                context::try_server_route(m, dirs)
            }),
            Err(CliError::NotFound(_)) => {
                // Ledger not found — try graph source lookup
                let alias = context::resolve_ledger(ledger, dirs)?;
                let fluree = context::build_fluree(dirs)?;
                let gs_id = context::to_ledger_id(&alias);
                if let Some(gs) = fluree.nameservice().lookup_graph_source(&gs_id).await? {
                    if graph.is_some() {
                        return Err(CliError::Usage(
                            "--graph is not applicable to graph sources".to_string(),
                        ));
                    }
                    print_graph_source_info(&gs);
                    return Ok(());
                }
                // Neither ledger nor graph source
                return Err(CliError::NotFound(format!(
                    "'{alias}' not found as a ledger or graph source"
                )));
            }
            Err(e) => Err(e),
        }
    }?;

    match mode {
        LedgerMode::Tracked {
            client,
            remote_alias,
            local_alias,
            remote_name,
        } => {
            let info = client.ledger_info(&remote_alias, graph).await?;

            context::persist_refreshed_tokens(&client, &remote_name, dirs).await;

            // Detect graph source response (has graph_source_id but no ledger_id)
            if info.get("graph_source_id").is_some() {
                if graph.is_some() {
                    return Err(CliError::Usage(
                        "--graph is not applicable to graph sources".to_string(),
                    ));
                }
                print_remote_graph_source_info(&info);
            } else {
                println!(
                    "Ledger:         {} (tracked)",
                    info.get("ledger")
                        .and_then(|v| v.as_str())
                        .unwrap_or(&local_alias)
                );
                if let Some(t) = info.get("t").and_then(serde_json::Value::as_i64) {
                    println!("t:              {t}");
                }
                if let Some(commit) = info
                    .get("commitId")
                    .and_then(|v| v.as_str())
                    .or_else(|| info.get("commit_head_id").and_then(|v| v.as_str()))
                {
                    println!("Commit ID:      {commit}");
                }
                if let Some(index) = info
                    .get("indexId")
                    .and_then(|v| v.as_str())
                    .or_else(|| info.get("index_head_id").and_then(|v| v.as_str()))
                {
                    println!("Index ID:       {index}");
                }

                // Print full JSON if there are stats
                if info.get("stats").is_some() {
                    println!();
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&info).unwrap_or_default()
                    );
                }
            }
        }
        LedgerMode::Local { fluree, alias } => {
            let ledger_id = context::to_ledger_id(&alias);

            // Try ledger first, then graph source
            if let Some(record) = fluree.nameservice().lookup(&ledger_id).await? {
                if let Some(g) = graph {
                    let info = fluree
                        .ledger_info(&ledger_id)
                        .for_graph(g)
                        .execute()
                        .await?;
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&info).unwrap_or_default()
                    );
                    return Ok(());
                }
                println!("Ledger:         {}", record.name);
                println!("Branch:         {}", record.branch);
                println!("Type:           Ledger");
                println!("Ledger ID:      {}", record.ledger_id);
                println!("Commit t:       {}", record.commit_t);
                println!(
                    "Commit ID:      {}",
                    record
                        .commit_head_id
                        .as_ref()
                        .map(std::string::ToString::to_string)
                        .as_deref()
                        .unwrap_or("(none)")
                );
                println!("Index t:        {}", record.index_t);
                println!(
                    "Index ID:       {}",
                    record
                        .index_head_id
                        .as_ref()
                        .map(std::string::ToString::to_string)
                        .as_deref()
                        .unwrap_or("(none)")
                );
            } else if let Some(gs) = fluree.nameservice().lookup_graph_source(&ledger_id).await? {
                if graph.is_some() {
                    return Err(CliError::Usage(
                        "--graph is not applicable to graph sources".to_string(),
                    ));
                }
                print_graph_source_info(&gs);
            } else {
                return Err(CliError::NotFound(format!(
                    "'{alias}' not found as a ledger or graph source"
                )));
            }
        }
    }

    Ok(())
}

/// Print graph source info from a JSON response (remote/server mode).
fn print_remote_graph_source_info(info: &serde_json::Value) {
    let name = info.get("name").and_then(|v| v.as_str()).unwrap_or("?");
    let branch = info.get("branch").and_then(|v| v.as_str()).unwrap_or("?");
    let gs_type = info.get("type").and_then(|v| v.as_str()).unwrap_or("?");
    let gs_id = info
        .get("graph_source_id")
        .and_then(|v| v.as_str())
        .unwrap_or("?");

    println!("Name:           {name}");
    println!("Branch:         {branch}");
    println!("Type:           {gs_type}");
    println!("ID:             {gs_id}");

    if let Some(t) = info.get("index_t").and_then(serde_json::Value::as_i64) {
        println!("Index t:        {t}");
    }
    if let Some(id) = info.get("index_id").and_then(|v| v.as_str()) {
        println!("Index ID:       {id}");
    }
    if let Some(deps) = info.get("dependencies").and_then(|v| v.as_array()) {
        let dep_strs: Vec<&str> = deps.iter().filter_map(|v| v.as_str()).collect();
        if !dep_strs.is_empty() {
            println!("Dependencies:   {}", dep_strs.join(", "));
        }
    }
    // Defense-in-depth: the server already redacts this config field, but an
    // older server (version skew) may not — redaction is idempotent, so passing
    // an already-redacted config through again is a no-op (issue #1497).
    if let Some(config) = info.get("config") {
        if let Some(config_block) = redact_config_pretty(&config.to_string()) {
            println!();
            println!("Configuration:");
            println!("{config_block}");
        }
    }
}

fn print_graph_source_info(gs: &fluree_db_nameservice::GraphSourceRecord) {
    println!("Name:           {}", gs.name);
    println!("Branch:         {}", gs.branch);
    println!("Type:           {}", format_source_type(&gs.source_type));
    println!("ID:             {}", gs.graph_source_id);
    println!("Retracted:      {}", gs.retracted);
    println!("Index t:        {}", gs.index_t);
    println!(
        "Index ID:       {}",
        gs.index_id
            .as_ref()
            .map(std::string::ToString::to_string)
            .as_deref()
            .unwrap_or("(none)")
    );

    if !gs.dependencies.is_empty() {
        println!("Dependencies:   {}", gs.dependencies.join(", "));
    }

    // Print config JSON (pretty), with auth leaves redacted. The stored config
    // is a live credential store (e.g. a Snowflake PAT under
    // `catalog.auth.client_secret`); LOCAL mode reads it verbatim, so it MUST be
    // routed through the redactor before display — issue #1497.
    if let Some(config_block) = redact_config_pretty(&gs.config) {
        println!();
        println!("Configuration:");
        println!("{config_block}");
    }
}

/// Redact secret leaves from a graph-source config JSON string and pretty-print
/// it for display, or `None` when there is nothing worth showing (empty / `{}` /
/// not JSON — the last preserving the prior "skip unparseable config" behavior).
///
/// Routes through [`fluree_db_api::redact_graph_source_config`]: allowlist
/// redaction that FAILS CLOSED (non-JSON input becomes a placeholder, never the
/// raw bytes). This is the single guard that keeps a stored `client_secret`/PAT
/// off the terminal for both local records and remote responses (issue #1497).
/// Redaction is idempotent, so a server response that was already redacted
/// server-side is displayed unchanged. See the canary test
/// `test_redact_graph_source_config_fails_closed_on_non_json` in `fluree-db-api`.
fn redact_config_pretty(config: &str) -> Option<String> {
    if config.is_empty() || config == "{}" {
        return None;
    }
    // Preserve the prior gate: only render configs that parse as JSON.
    serde_json::from_str::<serde_json::Value>(config).ok()?;
    let redacted = fluree_db_api::redact_graph_source_config(config);
    let parsed = serde_json::from_str::<serde_json::Value>(&redacted).ok()?;
    Some(serde_json::to_string_pretty(&parsed).unwrap_or(redacted))
}

fn format_source_type(st: &fluree_db_nameservice::GraphSourceType) -> String {
    match st {
        fluree_db_nameservice::GraphSourceType::Bm25 => "BM25".to_string(),
        fluree_db_nameservice::GraphSourceType::Vector => "Vector".to_string(),
        fluree_db_nameservice::GraphSourceType::Geo => "Geo".to_string(),
        fluree_db_nameservice::GraphSourceType::R2rml => "R2RML".to_string(),
        fluree_db_nameservice::GraphSourceType::Iceberg => "Iceberg".to_string(),
        fluree_db_nameservice::GraphSourceType::Unknown(s) => format!("Unknown({s})"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // A stored graph-source config carrying a live PAT under
    // `catalog.auth.client_secret`, mirroring the local record shape that
    // `fluree info <gs>` reads verbatim.
    const CONFIG_WITH_SECRET: &str = r#"{"catalog":{"type":"rest","uri":"https://polaris.example.com","auth":{"type":"oauth2_client_credentials","client_id":"svc-client","client_secret":"SUPER-SECRET-PAT","scope":"PRINCIPAL_ROLE:ALL"}}}"#;

    #[test]
    fn redact_config_pretty_masks_secret_keeps_identifiers() {
        let out = redact_config_pretty(CONFIG_WITH_SECRET).expect("config should render");
        // The live secret must NOT reach the terminal (issue #1497)...
        assert!(!out.contains("SUPER-SECRET-PAT"), "secret leaked: {out}");
        assert!(out.contains("[redacted]"), "expected placeholder: {out}");
        // ...while non-secret identifiers are preserved for debugging.
        assert!(out.contains("svc-client"), "client_id dropped: {out}");
        assert!(
            out.contains("https://polaris.example.com"),
            "catalog uri dropped: {out}"
        );
        // Output stays pretty-printed (multi-line), matching prior formatting.
        assert!(out.contains('\n'), "expected pretty JSON: {out}");
    }

    #[test]
    fn redact_config_pretty_skips_empty_and_non_json() {
        assert!(redact_config_pretty("").is_none());
        assert!(redact_config_pretty("{}").is_none());
        // Non-JSON is gated out (skip), preserving prior CLI behavior.
        assert!(redact_config_pretty("client_secret=not-json").is_none());
    }

    #[test]
    fn redact_config_pretty_is_idempotent() {
        let once = redact_config_pretty(CONFIG_WITH_SECRET).expect("first pass");
        // Feed a compact re-serialization of the redacted output back through.
        let compact =
            serde_json::to_string(&serde_json::from_str::<serde_json::Value>(&once).unwrap())
                .unwrap();
        let twice = redact_config_pretty(&compact).expect("second pass");
        assert!(!twice.contains("SUPER-SECRET-PAT"));
        assert!(twice.contains("svc-client"));
        assert_eq!(once, twice, "redaction must be idempotent");
    }
}
