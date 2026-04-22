//! Remote management commands: add, remove, list, show

use crate::cli::RemoteAction;
use crate::config::TomlSyncConfigStore;
use crate::error::{CliError, CliResult};
use colored::Colorize;
use comfy_table::{Cell, Table};
use fluree_db_api::server_defaults::FlureeDir;
use fluree_db_nameservice::RemoteName;
use fluree_db_nameservice_sync::{
    RemoteAuth, RemoteAuthType, RemoteConfig, RemoteEndpoint, SyncConfigStore,
};
use std::fs;

pub async fn run(action: RemoteAction, dirs: &FlureeDir) -> CliResult<()> {
    let store = TomlSyncConfigStore::new(dirs.config_dir().to_path_buf());

    match action {
        RemoteAction::Add { name, url, token } => run_add(&store, &name, &url, token).await,
        RemoteAction::Remove { name } => run_remove(&store, &name).await,
        RemoteAction::List => run_list(&store).await,
        RemoteAction::Show { name } => run_show(&store, &name).await,
    }
}

async fn run_add(
    store: &TomlSyncConfigStore,
    name: &str,
    url: &str,
    token: Option<String>,
) -> CliResult<()> {
    // Remote names cannot contain '/' — reserved as the delimiter in
    // compound `remote/ledger` syntax (e.g., `fluree query origin/mydb ...`).
    if name.contains('/') {
        return Err(CliError::Input(
            "remote name cannot contain '/' (reserved for remote/ledger syntax, e.g. origin/mydb)"
                .into(),
        ));
    }

    let input_url = url.trim_end_matches('/').to_string();

    // Load token from file if @filepath
    let auth_token = match token {
        Some(t) if t.starts_with('@') => {
            let path = t.strip_prefix('@').unwrap();
            let expanded = shellexpand::tilde(path);
            Some(
                fs::read_to_string(expanded.as_ref())
                    .map_err(|e| CliError::Input(format!("failed to read token file: {e}")))?
                    .trim()
                    .to_string(),
            )
        }
        Some(t) => Some(t),
        None => None,
    };

    // Build initial auth config
    let mut auth = RemoteAuth {
        token: auth_token,
        ..Default::default()
    };

    // Attempt discovery from `/.well-known/fluree.json` (non-fatal).
    //
    // The discovered `api_base_url` is the Fluree API base (should include the `/fluree`
    // prefix). We store that as the remote's HTTP base_url, so all other routes are
    // constructed relative to it (e.g. `{api_base_url}/query/<ledger...>`).
    let mut discovered_api_base_url: Option<String> = None;
    match discover_remote(&input_url).await {
        Ok(Some(discovered)) => {
            if let Some(api_base_url) = discovered.api_base_url {
                discovered_api_base_url = Some(api_base_url);
            }
            if let Some(discovered_auth) = discovered.auth {
                if auth.token.is_none() {
                    auth = discovered_auth;
                    eprintln!(
                        "  {} auto-discovered OIDC auth from server",
                        "info:".cyan().bold()
                    );
                    if let Some(ref issuer) = auth.issuer {
                        eprintln!("  Issuer: {issuer}");
                    }
                    eprintln!("  Run `fluree auth login --remote {name}` to authenticate");
                }
            }
        }
        Ok(None) => {
            // No discovery endpoint or not reachable yet — that's fine
        }
        Err(msg) => {
            eprintln!("  {} discovery failed: {}", "warn:".yellow().bold(), msg);
        }
    }

    // Determine the stored API base URL (always ends with `/fluree`).
    let base_url = if let Some(api) = discovered_api_base_url {
        api
    } else if input_url.ends_with("/fluree") {
        input_url.clone()
    } else {
        format!("{}/fluree", input_url.trim_end_matches('/'))
    };

    let config = RemoteConfig {
        name: RemoteName::new(name),
        endpoint: RemoteEndpoint::Http { base_url },
        auth,
        fetch_interval_secs: None,
    };

    store
        .set_remote(&config)
        .await
        .map_err(|e| CliError::Config(e.to_string()))?;

    println!("Added remote '{}'", name.green());
    Ok(())
}

#[derive(Debug, Default)]
pub(crate) struct DiscoveredRemote {
    pub(crate) api_base_url: Option<String>,
    pub(crate) auth: Option<RemoteAuth>,
}

/// Attempt to fetch `/.well-known/fluree.json` from the remote origin and parse
/// discovery configuration. Returns `Ok(None)` if the endpoint doesn't exist
/// (or if the server isn't reachable yet).
pub(crate) async fn discover_remote(remote_url: &str) -> Result<Option<DiscoveredRemote>, String> {
    let base = reqwest::Url::parse(remote_url).map_err(|e| e.to_string())?;
    let discovery_url = base
        .join("/.well-known/fluree.json")
        .map_err(|e| e.to_string())?;

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()
        .map_err(|e| e.to_string())?;

    let resp = match client.get(discovery_url.clone()).send().await {
        Ok(r) => r,
        Err(e) if e.is_connect() || e.is_timeout() => {
            // Server not reachable yet — perfectly normal during setup
            return Ok(None);
        }
        Err(e) => return Err(e.to_string()),
    };

    if !resp.status().is_success() {
        return Ok(None);
    }

    let body: serde_json::Value = resp.json().await.map_err(|e| e.to_string())?;

    let mut out = DiscoveredRemote::default();

    // api_base_url can be:
    // - absolute URL: https://data.example.com/v1/fluree
    // - absolute-path: /v1/fluree (resolved against discovery origin)
    if let Some(api_base_url) = body.get("api_base_url").and_then(|v| v.as_str()) {
        let resolved = if api_base_url.starts_with("http://")
            || api_base_url.starts_with("https://")
        {
            reqwest::Url::parse(api_base_url).map_err(|e| e.to_string())?
        } else if api_base_url.starts_with('/') {
            discovery_url
                .join(api_base_url)
                .map_err(|e| e.to_string())?
        } else {
            eprintln!(
                "  {} invalid api_base_url '{}' in discovery — expected absolute URL or absolute-path",
                "warn:".yellow().bold(),
                api_base_url
            );
            discovery_url.clone()
        };

        let mut s = resolved.to_string();
        s = s.trim_end_matches('/').to_string();
        if !s.is_empty() {
            out.api_base_url = Some(s);
        }
    }

    // Parse auth configuration (optional)
    if let Some(auth_obj) = body.get("auth").and_then(|a| a.as_object()) {
        let auth_type_str = auth_obj.get("type").and_then(|v| v.as_str()).unwrap_or("");
        match auth_type_str {
            "oidc_device" => {
                let issuer = auth_obj
                    .get("issuer")
                    .and_then(|v| v.as_str())
                    .ok_or("oidc_device discovery missing 'issuer' field")?
                    .to_string();

                let client_id = auth_obj
                    .get("client_id")
                    .and_then(|v| v.as_str())
                    .ok_or("oidc_device discovery missing 'client_id' field")?
                    .to_string();

                let exchange_url = auth_obj
                    .get("exchange_url")
                    .and_then(|v| v.as_str())
                    .ok_or("oidc_device discovery missing 'exchange_url' field")?
                    .to_string();

                // Parse optional scopes: accept JSON array or space-separated string
                let scopes = match auth_obj.get("scopes") {
                    Some(serde_json::Value::Array(arr)) => {
                        let v: Vec<String> = arr
                            .iter()
                            .filter_map(|s| s.as_str().map(String::from))
                            .collect();
                        if v.is_empty() {
                            None
                        } else {
                            Some(v)
                        }
                    }
                    Some(serde_json::Value::String(s)) => {
                        let v: Vec<String> = s.split_whitespace().map(String::from).collect();
                        if v.is_empty() {
                            None
                        } else {
                            Some(v)
                        }
                    }
                    _ => None,
                };

                // Parse optional redirect_port override
                let redirect_port = auth_obj
                    .get("redirect_port")
                    .and_then(serde_json::Value::as_u64)
                    .and_then(|p| u16::try_from(p).ok());

                out.auth = Some(RemoteAuth {
                    auth_type: Some(RemoteAuthType::OidcDevice),
                    issuer: Some(issuer),
                    client_id: Some(client_id),
                    exchange_url: Some(exchange_url),
                    scopes,
                    redirect_port,
                    ..Default::default()
                });
            }
            "token" | "" => {
                // manual token mode — nothing to auto-configure
            }
            other => {
                eprintln!(
                    "  {} unknown auth type '{}' in discovery — ignoring",
                    "warn:".yellow().bold(),
                    other
                );
            }
        }
    }

    Ok(Some(out))
}

async fn run_remove(store: &TomlSyncConfigStore, name: &str) -> CliResult<()> {
    let remote_name = RemoteName::new(name);

    let existing = store
        .get_remote(&remote_name)
        .await
        .map_err(|e| CliError::Config(e.to_string()))?;

    if existing.is_none() {
        return Err(CliError::NotFound(format!("remote '{name}' not found")));
    }

    store
        .remove_remote(&remote_name)
        .await
        .map_err(|e| CliError::Config(e.to_string()))?;

    println!("Removed remote '{name}'");
    Ok(())
}

async fn run_list(store: &TomlSyncConfigStore) -> CliResult<()> {
    let remotes = store
        .list_remotes()
        .await
        .map_err(|e| CliError::Config(e.to_string()))?;

    if remotes.is_empty() {
        println!("No remotes configured.");
        println!("  {} fluree remote add <name> <url>", "hint:".cyan().bold());
        return Ok(());
    }

    let mut table = Table::new();
    table.set_header(vec!["Name", "URL", "Auth"]);

    for remote in remotes {
        let url = match &remote.endpoint {
            RemoteEndpoint::Http { base_url } => base_url.clone(),
            RemoteEndpoint::Sse { events_url } => format!("(sse) {events_url}"),
            RemoteEndpoint::Storage { prefix } => format!("(storage) {prefix}"),
        };
        let auth = auth_display_short(&remote.auth);
        table.add_row(vec![
            Cell::new(remote.name.as_str()),
            Cell::new(url),
            Cell::new(auth),
        ]);
    }

    println!("{table}");
    Ok(())
}

async fn run_show(store: &TomlSyncConfigStore, name: &str) -> CliResult<()> {
    let remote_name = RemoteName::new(name);
    let remote = store
        .get_remote(&remote_name)
        .await
        .map_err(|e| CliError::Config(e.to_string()))?
        .ok_or_else(|| CliError::NotFound(format!("remote '{name}' not found")))?;

    println!("{}", "Remote:".bold());
    println!("  Name: {}", remote.name.as_str().green());

    match &remote.endpoint {
        RemoteEndpoint::Http { base_url } => {
            println!("  Type: HTTP");
            println!("  URL:  {base_url}");
        }
        RemoteEndpoint::Sse { events_url } => {
            println!("  Type: SSE");
            println!("  URL:  {events_url}");
        }
        RemoteEndpoint::Storage { prefix } => {
            println!("  Type: Storage");
            println!("  Prefix: {prefix}");
        }
    }

    // Auth details
    let auth = &remote.auth;
    match auth.auth_type.as_ref() {
        Some(RemoteAuthType::OidcDevice) => {
            println!("  Auth: {}", "oidc_device".cyan());
            if let Some(ref issuer) = auth.issuer {
                println!("  Issuer: {issuer}");
            }
            if let Some(ref client_id) = auth.client_id {
                println!("  Client ID: {client_id}");
            }
            if auth.token.is_some() {
                println!("  Token: {}", "cached".green());
            } else {
                println!("  Token: {}", "not logged in".yellow());
            }
        }
        Some(RemoteAuthType::Token) | None => {
            if auth.token.is_some() {
                println!("  Auth: token configured");
            } else {
                println!("  Auth: none");
            }
        }
    }

    if let Some(interval) = remote.fetch_interval_secs {
        println!("  Fetch interval: {interval}s");
    }

    Ok(())
}

/// Short auth description for the list table.
fn auth_display_short(auth: &RemoteAuth) -> &'static str {
    match auth.auth_type.as_ref() {
        Some(RemoteAuthType::OidcDevice) => {
            if auth.token.is_some() {
                "oidc (logged in)"
            } else {
                "oidc (not logged in)"
            }
        }
        Some(RemoteAuthType::Token) | None => {
            if auth.token.is_some() {
                "token"
            } else {
                "none"
            }
        }
    }
}
