//! Shared rendering of graph-source records for CLI commands.
//!
//! Every path that prints a graph-source config must go through this module:
//! stored configs can carry literal auth secrets (OAuth2 client secrets,
//! bearer tokens), so the config JSON is redacted before printing. Local
//! records are converted through [`build_generic_graph_source_info`] — the
//! same redacted view the server's `/info` route emits — so local and remote
//! sources render identically through one renderer.

use std::fmt::Write;

use fluree_db_api::ledger_info::{build_generic_graph_source_info, redact_json_secrets};

/// Print graph source info from a JSON response (remote/server mode).
pub(crate) fn print_remote_graph_source(info: &serde_json::Value) {
    print!("{}", render_graph_source(info));
}

/// Print graph source info from a locally-read nameservice record.
pub(crate) fn print_local_graph_source(gs: &fluree_db_nameservice::GraphSourceRecord) {
    print!("{}", render_local_graph_source(gs));
}

fn render_local_graph_source(gs: &fluree_db_nameservice::GraphSourceRecord) -> String {
    render_graph_source(&build_generic_graph_source_info(gs))
}

/// Render graph-source info from its redacted JSON view (a server `/info`
/// response, or [`build_generic_graph_source_info`] output for local records).
fn render_graph_source(info: &serde_json::Value) -> String {
    let name = info.get("name").and_then(|v| v.as_str()).unwrap_or("?");
    let branch = info.get("branch").and_then(|v| v.as_str()).unwrap_or("?");
    let gs_type = info.get("type").and_then(|v| v.as_str()).unwrap_or("?");
    let gs_id = info
        .get("graph_source_id")
        .and_then(|v| v.as_str())
        .unwrap_or("?");

    let mut out = String::new();
    let _ = writeln!(out, "Name:           {name}");
    let _ = writeln!(out, "Branch:         {branch}");
    let _ = writeln!(out, "Type:           {gs_type}");
    let _ = writeln!(out, "ID:             {gs_id}");

    if let Some(retracted) = info.get("retracted").and_then(serde_json::Value::as_bool) {
        let _ = writeln!(out, "Retracted:      {retracted}");
    }
    if let Some(t) = info.get("index_t").and_then(serde_json::Value::as_i64) {
        let _ = writeln!(out, "Index t:        {t}");
    }
    let index_id = info.get("index_id").and_then(|v| v.as_str());
    let _ = writeln!(out, "Index ID:       {}", index_id.unwrap_or("(none)"));

    if let Some(deps) = info.get("dependencies").and_then(|v| v.as_array()) {
        let dep_strs: Vec<&str> = deps.iter().filter_map(|v| v.as_str()).collect();
        if !dep_strs.is_empty() {
            let _ = writeln!(out, "Dependencies:   {}", dep_strs.join(", "));
        }
    }
    if let Some(config) = info.get("config") {
        write_config_section(&mut out, config);
    }
    out
}

fn write_config_section(out: &mut String, config: &serde_json::Value) {
    let _ = writeln!(out);
    let _ = writeln!(out, "Configuration:");
    let _ = writeln!(out, "{}", redacted_config_pretty(config));
}

fn redacted_config_pretty(config: &serde_json::Value) -> String {
    let mut redacted = config.clone();
    redact_json_secrets(&mut redacted);
    serde_json::to_string_pretty(&redacted).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_nameservice::{GraphSourceRecord, GraphSourceType};
    use serde_json::json;

    fn secret_bearing_config() -> serde_json::Value {
        json!({
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
        })
    }

    #[test]
    fn local_render_masks_config_secrets() {
        let gs = GraphSourceRecord::new(
            "my-gs",
            "main",
            GraphSourceType::Iceberg,
            secret_bearing_config().to_string(),
            vec![],
        );

        let rendered = render_local_graph_source(&gs);
        assert!(rendered.contains("Name:           my-gs"));
        assert!(rendered.contains("Type:           Iceberg"));
        assert!(rendered.contains("Retracted:      false"));
        assert!(rendered.contains("Index ID:       (none)"));
        assert!(rendered.contains("Configuration:"));
        assert!(!rendered.contains("super-secret-pat"));
        assert!(!rendered.contains("live-bearer-token"));
        assert!(rendered.contains("[redacted]"));
    }

    #[test]
    fn remote_render_masks_config_secrets() {
        let info = json!({
            "name": "my-gs",
            "branch": "main",
            "type": "Iceberg",
            "graph_source_id": "my-gs:main",
            "index_t": 0,
            "config": secret_bearing_config()
        });

        let rendered = render_graph_source(&info);
        assert!(rendered.contains("Name:           my-gs"));
        assert!(rendered.contains("Configuration:"));
        assert!(!rendered.contains("super-secret-pat"));
        assert!(!rendered.contains("live-bearer-token"));
        assert!(rendered.contains("[redacted]"));
    }

    #[test]
    fn local_render_fails_closed_on_unparseable_config() {
        let gs = GraphSourceRecord::new(
            "my-gs",
            "main",
            GraphSourceType::Iceberg,
            "client_secret=oops-not-json",
            vec![],
        );

        let rendered = render_local_graph_source(&gs);
        assert!(rendered.contains("Configuration:"));
        assert!(rendered.contains("[redacted:unparseable]"));
        assert!(!rendered.contains("oops-not-json"));
    }

    #[test]
    fn redacts_oauth_client_secret_and_bearer_token() {
        let pretty = redacted_config_pretty(&secret_bearing_config());
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
