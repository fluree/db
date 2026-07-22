//! Shared rendering of graph-source records for CLI commands.
//!
//! Every path that prints a graph-source config must go through this module:
//! stored configs can carry literal auth secrets (OAuth2 client secrets,
//! bearer tokens), so the config JSON is redacted before printing.

use fluree_db_api::ledger_info::redact_json_secrets;

/// Print graph source info from a JSON response (remote/server mode).
pub(crate) fn print_remote_graph_source(info: &serde_json::Value) {
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
    if let Some(config) = info.get("config") {
        print_config_section(config);
    }
}

/// Print graph source info from a locally-read nameservice record.
pub(crate) fn print_local_graph_source(gs: &fluree_db_nameservice::GraphSourceRecord) {
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

    if !gs.config.is_empty() && gs.config != "{}" {
        if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&gs.config) {
            print_config_section(&parsed);
        }
    }
}

pub(crate) fn format_source_type(st: &fluree_db_nameservice::GraphSourceType) -> String {
    match st {
        fluree_db_nameservice::GraphSourceType::Bm25 => "BM25".to_string(),
        fluree_db_nameservice::GraphSourceType::Vector => "Vector".to_string(),
        fluree_db_nameservice::GraphSourceType::Geo => "Geo".to_string(),
        fluree_db_nameservice::GraphSourceType::R2rml => "R2RML".to_string(),
        fluree_db_nameservice::GraphSourceType::Iceberg => "Iceberg".to_string(),
        fluree_db_nameservice::GraphSourceType::Unknown(s) => format!("Unknown({s})"),
    }
}

fn print_config_section(config: &serde_json::Value) {
    println!();
    println!("Configuration:");
    println!("{}", redacted_config_pretty(config));
}

fn redacted_config_pretty(config: &serde_json::Value) -> String {
    let mut redacted = config.clone();
    redact_json_secrets(&mut redacted);
    serde_json::to_string_pretty(&redacted).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn redacts_oauth_client_secret_and_bearer_token() {
        let config = json!({
            "catalog": {
                "type": "rest",
                "uri": "https://acct.snowflakecomputing.com/polaris/api/catalog",
                "auth": {
                    "type": "oauth2_client_credentials",
                    "client_id": "my-client",
                    "client_secret": "super-secret-pat",
                    "scope": "session:role:ICEBERG_READER"
                }
            },
            "bearer": { "token": "live-bearer-token" }
        });

        let pretty = redacted_config_pretty(&config);
        assert!(!pretty.contains("super-secret-pat"));
        assert!(!pretty.contains("live-bearer-token"));
        assert!(pretty.contains("[redacted]"));
        // Non-secret identifying fields survive.
        assert!(pretty.contains("my-client"));
        assert!(pretty.contains("session:role:ICEBERG_READER"));
    }

    #[test]
    fn keeps_env_var_name_but_masks_inline_default() {
        let config = json!({
            "auth": {
                "client_secret": { "env_var": "FLUREE_CLIENT_SECRET", "default_val": "fallback-secret" }
            }
        });

        let pretty = redacted_config_pretty(&config);
        assert!(pretty.contains("FLUREE_CLIENT_SECRET"));
        assert!(!pretty.contains("fallback-secret"));
    }

    #[test]
    fn non_secret_config_is_unchanged() {
        let config = json!({ "index": "bm25", "k1": 1.2, "b": 0.75 });
        let pretty = redacted_config_pretty(&config);
        let reparsed: serde_json::Value = serde_json::from_str(&pretty).unwrap();
        assert_eq!(reparsed, config);
    }
}
