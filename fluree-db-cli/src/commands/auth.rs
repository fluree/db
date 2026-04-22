//! Auth management commands: status, login, logout
//!
//! Manages bearer tokens stored in remote configs. Tokens are stored
//! in `.fluree/config.toml` as part of the remote's `auth` section.
//!
//! Token values are never printed to stdout — `status` shows presence,
//! expiry, and identity only.
//!
//! For OIDC remotes (`auth.type = "oidc_device"`), `login` auto-detects
//! the appropriate OAuth flow based on the IdP's discovery document:
//! - Device Authorization Grant (RFC 8628) if `device_authorization_endpoint` exists
//! - Authorization Code + PKCE (RFC 7636) otherwise (e.g., AWS Cognito)
//!
//! After obtaining an IdP token, it is exchanged for a Fluree-scoped
//! Bearer token via the configured exchange endpoint.

use crate::cli::AuthAction;
use crate::config::TomlSyncConfigStore;
use crate::error::{CliError, CliResult};
use colored::Colorize;
use fluree_db_api::server_defaults::FlureeDir;
use fluree_db_nameservice::RemoteName;
use fluree_db_nameservice_sync::{RemoteAuthType, RemoteEndpoint, SyncConfigStore};
use std::io::{self, Read};

pub async fn run(action: AuthAction, dirs: &FlureeDir) -> CliResult<()> {
    let store = TomlSyncConfigStore::new(dirs.config_dir().to_path_buf());

    match action {
        AuthAction::Status { remote } => run_status(&store, remote.as_deref()).await,
        AuthAction::Login { remote, token } => run_login(&store, remote.as_deref(), token).await,
        AuthAction::Logout { remote } => run_logout(&store, remote.as_deref()).await,
    }
}

/// Resolve which remote to use: explicit name, or the only configured remote.
async fn resolve_remote_name(
    store: &TomlSyncConfigStore,
    explicit: Option<&str>,
) -> CliResult<String> {
    if let Some(name) = explicit {
        return Ok(name.to_string());
    }

    let remotes = store
        .list_remotes()
        .await
        .map_err(|e| CliError::Config(e.to_string()))?;

    match remotes.len() {
        0 => Err(CliError::Config(
            "no remotes configured. Use `fluree remote add <name> <url>` first.".to_string(),
        )),
        1 => Ok(remotes[0].name.as_str().to_string()),
        _ => Err(CliError::Usage(
            "multiple remotes configured; specify one with --remote <name>".to_string(),
        )),
    }
}

// =============================================================================
// Status
// =============================================================================

async fn run_status(store: &TomlSyncConfigStore, remote: Option<&str>) -> CliResult<()> {
    let name = resolve_remote_name(store, remote).await?;
    let remote_name = RemoteName::new(&name);
    let config = store
        .get_remote(&remote_name)
        .await
        .map_err(|e| CliError::Config(e.to_string()))?
        .ok_or_else(|| CliError::NotFound(format!("remote '{name}' not found")))?;

    println!("{}", "Auth Status:".bold());
    println!("  Remote: {}", name.green());

    // Show auth type
    match config.auth.auth_type.as_ref() {
        Some(RemoteAuthType::OidcDevice) => {
            println!("  Auth type: {}", "oidc_device".cyan());
            if let Some(ref issuer) = config.auth.issuer {
                println!("  Issuer: {issuer}");
            }
        }
        Some(RemoteAuthType::Token) => {
            println!("  Auth type: token");
        }
        None => {}
    }

    match &config.auth.token {
        Some(token) => {
            println!("  Token:  {}", "configured".green());

            // Decode token claims without verification to show expiry/identity
            match decode_token_summary(token) {
                Ok(summary) => {
                    if let Some(exp) = summary.expiry {
                        println!("  Expiry: {exp}");
                    } else {
                        println!("  Expiry: {}", "no expiry claim".yellow());
                    }
                    if let Some(identity) = &summary.identity {
                        println!("  Identity: {identity}");
                    }
                    if let Some(issuer) = &summary.issuer {
                        println!("  Issuer: {issuer}");
                    }
                    if let Some(subject) = &summary.subject {
                        println!("  Subject: {subject}");
                    }
                }
                Err(()) => {
                    println!("  {}", "(could not decode token claims)".yellow());
                }
            }

            if config.auth.refresh_token.is_some() {
                println!("  Refresh: available");
            }
        }
        None => {
            println!("  Token:  {}", "not configured".yellow());
            println!(
                "  {} fluree auth login --remote {}",
                "hint:".cyan().bold(),
                name
            );
        }
    }

    Ok(())
}

// =============================================================================
// Login
// =============================================================================

async fn run_login(
    store: &TomlSyncConfigStore,
    remote: Option<&str>,
    token_arg: Option<String>,
) -> CliResult<()> {
    let name = resolve_remote_name(store, remote).await?;
    let remote_name = RemoteName::new(&name);
    let mut config = store
        .get_remote(&remote_name)
        .await
        .map_err(|e| CliError::Config(e.to_string()))?
        .ok_or_else(|| CliError::NotFound(format!("remote '{name}' not found")))?;

    // Route based on auth type
    let is_oidc = config.auth.auth_type.as_ref() == Some(&RemoteAuthType::OidcDevice);

    if is_oidc && token_arg.is_none() {
        // OIDC device code flow
        run_oidc_login(&mut config).await?;
    } else if config.auth.auth_type.is_none() && token_arg.is_none() {
        // Auth type unset and no explicit token — try discovery first
        if try_discover_and_login(&mut config).await? {
            // Discovery succeeded and OIDC login completed
        } else {
            // No discovery available — fall back to manual token prompt
            let token = read_token(token_arg)?;
            config.auth.token = Some(token);
        }
    } else {
        // Manual token flow (explicit token or token auth type)
        let token = read_token(token_arg)?;
        config.auth.token = Some(token);
    }

    store
        .set_remote(&config)
        .await
        .map_err(|e| CliError::Config(e.to_string()))?;

    println!("Token stored for remote '{}'", name.green());

    // Show a brief summary of what was stored
    if let Some(ref tok) = config.auth.token {
        if let Ok(summary) = decode_token_summary(tok) {
            if let Some(exp) = summary.expiry {
                println!("  Expiry: {exp}");
            }
            if let Some(identity) = &summary.identity {
                println!("  Identity: {identity}");
            }
        }
    }

    Ok(())
}

/// Read a token from argument, file, stdin, or interactive prompt.
fn read_token(token_arg: Option<String>) -> CliResult<String> {
    let token = match token_arg {
        Some(t) if t == "@-" => {
            let mut buf = String::new();
            io::stdin()
                .read_to_string(&mut buf)
                .map_err(|e| CliError::Input(format!("failed to read token from stdin: {e}")))?;
            buf.trim().to_string()
        }
        Some(t) if t.starts_with('@') => {
            let path = t.strip_prefix('@').unwrap();
            let expanded = shellexpand::tilde(path);
            std::fs::read_to_string(expanded.as_ref())
                .map_err(|e| CliError::Input(format!("failed to read token file: {e}")))?
                .trim()
                .to_string()
        }
        Some(t) => t,
        None => {
            eprintln!("Paste token (then press Enter):");
            let mut buf = String::new();
            io::stdin()
                .read_line(&mut buf)
                .map_err(|e| CliError::Input(format!("failed to read token: {e}")))?;
            buf.trim().to_string()
        }
    };

    if token.is_empty() {
        return Err(CliError::Input("token cannot be empty".to_string()));
    }

    Ok(token)
}

// =============================================================================
// OIDC Discovery Fallback
// =============================================================================

/// When auth type is unset, try `/.well-known/fluree.json` discovery.
/// If OIDC config is found, configure the remote and run the device flow.
/// Returns `true` if OIDC login was performed, `false` if no discovery available.
async fn try_discover_and_login(
    config: &mut fluree_db_nameservice_sync::RemoteConfig,
) -> CliResult<bool> {
    let base_url = match &config.endpoint {
        RemoteEndpoint::Http { base_url } => base_url.clone(),
        _ => return Ok(false),
    };

    eprintln!("No auth type configured; checking server for OIDC discovery...");

    match super::remote::discover_remote(&base_url).await {
        Ok(Some(discovered)) => {
            // If discovery provides an API base URL, store it for subsequent operations.
            if let Some(api) = discovered.api_base_url {
                config.endpoint = RemoteEndpoint::Http { base_url: api };
            }

            // If discovery provides OIDC config, run the device flow.
            if let Some(discovered_auth) = discovered.auth {
                if discovered_auth.auth_type.as_ref() == Some(&RemoteAuthType::OidcDevice) {
                    // Apply discovered OIDC config
                    config.auth = discovered_auth;
                    eprintln!(
                        "  {} auto-discovered OIDC auth from server",
                        "info:".cyan().bold()
                    );

                    // Run the OIDC device flow
                    run_oidc_login(config).await?;
                    Ok(true)
                } else {
                    Ok(false)
                }
            } else {
                Ok(false)
            }
        }
        Ok(None) => {
            eprintln!("  No OIDC discovery available; using manual token login.");
            Ok(false)
        }
        Err(msg) => {
            eprintln!("  {} discovery failed: {}", "warn:".yellow().bold(), msg);
            Ok(false)
        }
    }
}

// =============================================================================
// OIDC Login (auto-detects device code vs authorization code + PKCE)
// =============================================================================

/// OIDC endpoints discovered from the provider's openid-configuration.
/// The CLI auto-selects the appropriate flow based on which endpoints exist.
enum OidcFlow {
    /// Provider supports Device Authorization Grant (RFC 8628)
    DeviceCode {
        device_authorization_endpoint: String,
        token_endpoint: String,
    },
    /// Provider supports Authorization Code + PKCE (RFC 7636)
    /// Used when device_authorization_endpoint is absent (e.g., AWS Cognito).
    AuthorizationCode {
        authorization_endpoint: String,
        token_endpoint: String,
    },
}

/// Run the full OIDC login flow + Fluree token exchange.
///
/// Auto-detects which OAuth flow the IdP supports:
/// - Device Authorization Grant if `device_authorization_endpoint` is present
/// - Authorization Code + PKCE if only `authorization_endpoint` is present
async fn run_oidc_login(config: &mut fluree_db_nameservice_sync::RemoteConfig) -> CliResult<()> {
    let issuer = config
        .auth
        .issuer
        .as_ref()
        .ok_or_else(|| CliError::Config("OIDC auth configured but 'issuer' is missing".into()))?
        .clone();

    let client_id = config
        .auth
        .client_id
        .as_ref()
        .ok_or_else(|| CliError::Config("OIDC auth configured but 'client_id' is missing".into()))?
        .clone();

    let exchange_url = config
        .auth
        .exchange_url
        .as_ref()
        .ok_or_else(|| {
            CliError::Config("OIDC auth configured but 'exchange_url' is missing".into())
        })?
        .clone();

    // Resolve scopes: config → default "openid"
    let scopes = match &config.auth.scopes {
        Some(s) if !s.is_empty() => s.join(" "),
        _ => "openid".to_string(),
    };

    let http = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .map_err(|e| CliError::Remote(format!("failed to build HTTP client: {e}")))?;

    // Step 1: Discover OIDC endpoints and determine flow
    eprintln!("Discovering OIDC endpoints...");
    let flow = discover_oidc_endpoints(&http, &issuer).await?;

    // Step 2: Run the appropriate flow
    let idp_tokens = match flow {
        OidcFlow::DeviceCode {
            device_authorization_endpoint,
            token_endpoint,
        } => {
            run_device_code_flow(
                &http,
                &device_authorization_endpoint,
                &token_endpoint,
                &client_id,
                &scopes,
            )
            .await?
        }
        OidcFlow::AuthorizationCode {
            authorization_endpoint,
            token_endpoint,
        } => {
            eprintln!(
                "  {} IdP does not support device flow; using authorization code + PKCE",
                "info:".cyan().bold()
            );
            run_auth_code_flow(
                &http,
                &authorization_endpoint,
                &token_endpoint,
                &client_id,
                &scopes,
                config.auth.redirect_port,
            )
            .await?
        }
    };

    eprintln!("  IdP authentication successful");

    // Step 3: Exchange IdP token for Fluree token
    //
    // Prefer id_token when available: its `aud` claim matches the client_id,
    // which is what the exchange endpoint validates. Cognito access_tokens
    // may have a different audience (resource server) that fails validation.
    eprintln!("Exchanging for Fluree token...");
    let (subject_token, subject_token_type) = if let Some(ref id_token) = idp_tokens.id_token {
        (
            id_token.as_str(),
            "urn:ietf:params:oauth:token-type:id_token",
        )
    } else {
        (
            idp_tokens.access_token.as_str(),
            "urn:ietf:params:oauth:token-type:access_token",
        )
    };
    let fluree_tokens =
        exchange_token(&http, &exchange_url, subject_token, subject_token_type).await?;

    // Step 4: Store tokens
    config.auth.token = Some(fluree_tokens.access_token);
    config.auth.refresh_token = fluree_tokens.refresh_token;

    Ok(())
}

/// Fetch OIDC discovery document and determine which flow to use.
///
/// Prefers device flow if available; falls back to authorization code.
async fn discover_oidc_endpoints(http: &reqwest::Client, issuer: &str) -> CliResult<OidcFlow> {
    let url = format!(
        "{}/.well-known/openid-configuration",
        issuer.trim_end_matches('/')
    );

    let resp = http
        .get(&url)
        .send()
        .await
        .map_err(|e| CliError::Remote(format!("OIDC discovery failed: {e}")))?;

    if !resp.status().is_success() {
        return Err(CliError::Remote(format!(
            "OIDC discovery returned {}",
            resp.status()
        )));
    }

    let body: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| CliError::Remote(format!("OIDC discovery invalid JSON: {e}")))?;

    let token_endpoint = body
        .get("token_endpoint")
        .and_then(|v| v.as_str())
        .ok_or_else(|| CliError::Remote("OIDC discovery missing token_endpoint".into()))?
        .to_string();

    // Prefer device flow if available
    if let Some(device_ep) = body
        .get("device_authorization_endpoint")
        .and_then(|v| v.as_str())
    {
        return Ok(OidcFlow::DeviceCode {
            device_authorization_endpoint: device_ep.to_string(),
            token_endpoint,
        });
    }

    // Fall back to authorization code (e.g., AWS Cognito)
    if let Some(auth_ep) = body.get("authorization_endpoint").and_then(|v| v.as_str()) {
        return Ok(OidcFlow::AuthorizationCode {
            authorization_endpoint: auth_ep.to_string(),
            token_endpoint,
        });
    }

    Err(CliError::Remote(
        "OIDC provider supports neither device_authorization_endpoint \
         nor authorization_endpoint"
            .into(),
    ))
}

// =============================================================================
// Device Authorization Grant (RFC 8628)
// =============================================================================

/// Run the Device Authorization Grant flow.
async fn run_device_code_flow(
    http: &reqwest::Client,
    device_auth_endpoint: &str,
    token_endpoint: &str,
    client_id: &str,
    scopes: &str,
) -> CliResult<IdpTokenResponse> {
    eprintln!("Requesting device authorization...");
    let device_resp = request_device_code(http, device_auth_endpoint, client_id, scopes).await?;

    eprintln!();
    eprintln!("  {} Open this URL and enter the code below:", ">>".bold());
    eprintln!("  URL:  {}", device_resp.verification_uri.cyan());
    if let Some(ref complete_uri) = device_resp.verification_uri_complete {
        eprintln!("  (or)  {}", complete_uri.cyan());
    }
    eprintln!("  Code: {}", device_resp.user_code.bold().green());
    eprintln!();

    let open_url = device_resp
        .verification_uri_complete
        .as_deref()
        .unwrap_or(&device_resp.verification_uri);
    if open::that(open_url).is_ok() {
        eprintln!("  (browser opened automatically)");
    }

    eprintln!("Waiting for authorization...");
    poll_for_token(
        http,
        token_endpoint,
        &device_resp.device_code,
        client_id,
        device_resp.interval,
    )
    .await
}

/// Device authorization response.
struct DeviceAuthResponse {
    device_code: String,
    user_code: String,
    verification_uri: String,
    verification_uri_complete: Option<String>,
    interval: u64,
}

/// Request a device code from the IdP.
async fn request_device_code(
    http: &reqwest::Client,
    device_auth_endpoint: &str,
    client_id: &str,
    scopes: &str,
) -> CliResult<DeviceAuthResponse> {
    let resp = http
        .post(device_auth_endpoint)
        .form(&[("client_id", client_id), ("scope", scopes)])
        .send()
        .await
        .map_err(|e| CliError::Remote(format!("device authorization request failed: {e}")))?;

    if !resp.status().is_success() {
        let body = resp.text().await.unwrap_or_default();
        return Err(CliError::Remote(format!(
            "device authorization failed: {body}"
        )));
    }

    let body: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| CliError::Remote(format!("device authorization invalid JSON: {e}")))?;

    let device_code = body
        .get("device_code")
        .and_then(|v| v.as_str())
        .ok_or_else(|| CliError::Remote("device response missing device_code".into()))?
        .to_string();

    let user_code = body
        .get("user_code")
        .and_then(|v| v.as_str())
        .ok_or_else(|| CliError::Remote("device response missing user_code".into()))?
        .to_string();

    let verification_uri = body
        .get("verification_uri")
        .and_then(|v| v.as_str())
        .ok_or_else(|| CliError::Remote("device response missing verification_uri".into()))?
        .to_string();

    let verification_uri_complete = body
        .get("verification_uri_complete")
        .and_then(|v| v.as_str())
        .map(String::from);

    let interval = body
        .get("interval")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(5);

    Ok(DeviceAuthResponse {
        device_code,
        user_code,
        verification_uri,
        verification_uri_complete,
        interval,
    })
}

/// IdP token response (from either device code polling or auth code exchange).
struct IdpTokenResponse {
    access_token: String,
    /// Present when the IdP returns an id_token (e.g., Cognito auth code flow).
    /// Preferred for exchange because its `aud` claim matches the client_id.
    id_token: Option<String>,
}

/// Poll the token endpoint until the user completes authorization.
async fn poll_for_token(
    http: &reqwest::Client,
    token_endpoint: &str,
    device_code: &str,
    client_id: &str,
    interval_secs: u64,
) -> CliResult<IdpTokenResponse> {
    let interval = std::time::Duration::from_secs(interval_secs.max(1));

    loop {
        tokio::time::sleep(interval).await;

        let resp = http
            .post(token_endpoint)
            .form(&[
                ("grant_type", "urn:ietf:params:oauth:grant-type:device_code"),
                ("device_code", device_code),
                ("client_id", client_id),
            ])
            .send()
            .await
            .map_err(|e| CliError::Remote(format!("token poll failed: {e}")))?;

        let status = resp.status();
        let body: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| CliError::Remote(format!("token poll invalid JSON: {e}")))?;

        if status.is_success() {
            let access_token = body
                .get("access_token")
                .and_then(|v| v.as_str())
                .ok_or_else(|| CliError::Remote("token response missing access_token".into()))?
                .to_string();
            let id_token = body
                .get("id_token")
                .and_then(|v| v.as_str())
                .map(String::from);

            return Ok(IdpTokenResponse {
                access_token,
                id_token,
            });
        }

        // Check error type
        let error = body.get("error").and_then(|v| v.as_str()).unwrap_or("");

        match error {
            "authorization_pending" => {
                eprint!(".");
                continue;
            }
            "slow_down" => {
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                continue;
            }
            "expired_token" => {
                return Err(CliError::Remote(
                    "device code expired. Run `fluree auth login` to try again.".into(),
                ));
            }
            "access_denied" => {
                return Err(CliError::Remote(
                    "authorization denied by user or IdP".into(),
                ));
            }
            _ => {
                let desc = body
                    .get("error_description")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown error");
                return Err(CliError::Remote(format!(
                    "device code grant failed: {error}: {desc}"
                )));
            }
        }
    }
}

// =============================================================================
// Authorization Code + PKCE (RFC 7636) — fallback for providers like Cognito
// =============================================================================

/// Default ports to try for the localhost callback listener.
/// All must be allowlisted in the IdP's redirect URI configuration.
const DEFAULT_CALLBACK_PORTS: &[u16] = &[8400, 8401, 8402, 8403, 8404, 8405];

/// Callback timeout (2 minutes).
const CALLBACK_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(120);

/// Run the Authorization Code + PKCE flow.
async fn run_auth_code_flow(
    http: &reqwest::Client,
    authorization_endpoint: &str,
    token_endpoint: &str,
    client_id: &str,
    scopes: &str,
    configured_port: Option<u16>,
) -> CliResult<IdpTokenResponse> {
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
    use sha2::{Digest, Sha256};

    // Step 1: Generate PKCE parameters
    let code_verifier = generate_code_verifier();
    let code_challenge = {
        let hash = Sha256::digest(code_verifier.as_bytes());
        URL_SAFE_NO_PAD.encode(hash)
    };
    let state = generate_random_state();

    // Step 2: Bind localhost listener
    let listener = bind_callback_listener(configured_port).await?;
    let port = listener
        .local_addr()
        .map_err(|e| CliError::Remote(format!("failed to get listener address: {e}")))?
        .port();
    let redirect_uri = format!("http://127.0.0.1:{port}/callback");

    // Step 3: Build authorization URL and open browser
    //
    // Use Url::parse + query_pairs_mut to safely append params even if
    // authorization_endpoint already contains query parameters.
    let auth_url = {
        let mut url = reqwest::Url::parse(authorization_endpoint)
            .map_err(|e| CliError::Remote(format!("invalid authorization_endpoint URL: {e}")))?;
        url.query_pairs_mut()
            .append_pair("response_type", "code")
            .append_pair("client_id", client_id)
            .append_pair("redirect_uri", &redirect_uri)
            .append_pair("scope", scopes)
            .append_pair("code_challenge", &code_challenge)
            .append_pair("code_challenge_method", "S256")
            .append_pair("state", &state);
        url.to_string()
    };

    eprintln!();
    eprintln!("  {} Opening browser for authentication...", ">>".bold());
    eprintln!("  URL: {}", auth_url.cyan());
    eprintln!();

    if open::that(&auth_url).is_ok() {
        eprintln!("  (browser opened automatically)");
    } else {
        eprintln!(
            "  {} Open the URL above in your browser.",
            "hint:".cyan().bold()
        );
    }

    // Step 4: Wait for callback
    eprintln!("Waiting for authorization callback on port {port}...");
    let (code, received_state) = accept_callback(&listener).await?;

    // Validate state (CSRF protection)
    if received_state != state {
        return Err(CliError::Remote(
            "OAuth state mismatch — possible CSRF attack. Try again.".into(),
        ));
    }

    // Step 5: Exchange authorization code for IdP tokens
    eprintln!("Exchanging authorization code for tokens...");
    let resp = http
        .post(token_endpoint)
        .form(&[
            ("grant_type", "authorization_code"),
            ("code", code.as_str()),
            ("redirect_uri", redirect_uri.as_str()),
            ("client_id", client_id),
            ("code_verifier", code_verifier.as_str()),
        ])
        .send()
        .await
        .map_err(|e| CliError::Remote(format!("token exchange failed: {e}")))?;

    if !resp.status().is_success() {
        let body = resp.text().await.unwrap_or_default();
        return Err(CliError::Remote(format!(
            "authorization code exchange failed: {body}"
        )));
    }

    let body: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| CliError::Remote(format!("token response invalid JSON: {e}")))?;

    let access_token = body
        .get("access_token")
        .and_then(|v| v.as_str())
        .ok_or_else(|| CliError::Remote("token response missing access_token".into()))?
        .to_string();
    let id_token = body
        .get("id_token")
        .and_then(|v| v.as_str())
        .map(String::from);

    Ok(IdpTokenResponse {
        access_token,
        id_token,
    })
}

/// Bind a localhost listener for the OAuth callback.
///
/// If a specific port is configured (via config or `FLUREE_AUTH_PORT` env var),
/// bind that port. Otherwise, try ports 8400..8405 and use the first available.
async fn bind_callback_listener(
    configured_port: Option<u16>,
) -> CliResult<tokio::net::TcpListener> {
    use tokio::net::TcpListener;

    // Check env var override
    let port_override = configured_port.or_else(|| {
        std::env::var("FLUREE_AUTH_PORT")
            .ok()
            .and_then(|s| s.parse::<u16>().ok())
    });

    if let Some(port) = port_override {
        return TcpListener::bind(format!("127.0.0.1:{port}"))
            .await
            .map_err(|e| {
                CliError::Remote(format!(
                    "failed to bind port {port} for auth callback: {e}\n  \
                     Ensure the port is free, or set FLUREE_AUTH_PORT to a different port."
                ))
            });
    }

    // Try default ports in order
    for &port in DEFAULT_CALLBACK_PORTS {
        match TcpListener::bind(format!("127.0.0.1:{port}")).await {
            Ok(listener) => return Ok(listener),
            Err(_) => continue,
        }
    }

    Err(CliError::Remote(format!(
        "could not bind any callback port ({}-{})\n  \
         Free one of these ports or set FLUREE_AUTH_PORT to an available port.\n  \
         The callback URI (e.g., http://127.0.0.1:8400/callback) must be \
         allowlisted in your IdP's app client configuration.",
        DEFAULT_CALLBACK_PORTS[0],
        DEFAULT_CALLBACK_PORTS[DEFAULT_CALLBACK_PORTS.len() - 1],
    )))
}

/// Accept a single OAuth callback on the localhost listener.
///
/// Loops accepting connections until a `GET /callback?...` request arrives
/// (ignoring browser requests to `/`, `/favicon.ico`, etc.).
/// Returns the authorization `code` and `state` from query parameters.
async fn accept_callback(listener: &tokio::net::TcpListener) -> CliResult<(String, String)> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let deadline = tokio::time::Instant::now() + CALLBACK_TIMEOUT;

    loop {
        // Accept with timeout
        let (mut stream, _) = match tokio::time::timeout_at(deadline, listener.accept()).await {
            Ok(Ok(conn)) => conn,
            Ok(Err(e)) => {
                return Err(CliError::Remote(format!(
                    "failed to accept callback connection: {e}"
                )));
            }
            Err(_) => {
                let port = listener.local_addr().map(|a| a.port()).unwrap_or(0);
                return Err(CliError::Remote(format!(
                    "authorization timed out after {} seconds.\n  \
                     Callback was listening on http://127.0.0.1:{port}/callback\n  \
                     Ensure this URI is allowlisted in your IdP's redirect URIs.",
                    CALLBACK_TIMEOUT.as_secs()
                )));
            }
        };

        // Read the HTTP request
        let mut buf = vec![0u8; 4096];
        let n = stream
            .read(&mut buf)
            .await
            .map_err(|e| CliError::Remote(format!("failed to read callback request: {e}")))?;
        let request = String::from_utf8_lossy(&buf[..n]);

        // Parse first line: "GET /callback?code=...&state=... HTTP/1.1"
        let first_line = request.lines().next().unwrap_or("");
        let path = first_line.split_whitespace().nth(1).unwrap_or("");

        // Ignore non-callback requests (/, /favicon.ico, etc.)
        if !path.starts_with("/callback") {
            let not_found = "HTTP/1.1 404 Not Found\r\n\
                             Content-Length: 0\r\n\
                             Connection: close\r\n\r\n";
            let _ = stream.write_all(not_found.as_bytes()).await;
            let _ = stream.shutdown().await;
            continue;
        }

        // Extract query parameters
        let query = path.split('?').nth(1).unwrap_or("");
        let params: std::collections::HashMap<&str, &str> = query
            .split('&')
            .filter_map(|pair| {
                let mut parts = pair.splitn(2, '=');
                Some((parts.next()?, parts.next()?))
            })
            .collect();

        // Check for error response from IdP
        if let Some(error) = params.get("error") {
            let desc = params.get("error_description").unwrap_or(&"unknown error");
            // Don't echo raw query params into HTML — serve a static error page
            let error_html = "HTTP/1.1 200 OK\r\n\
                 Content-Type: text/html\r\n\
                 Connection: close\r\n\r\n\
                 <html><body>\
                 <h2>Authorization Failed</h2>\
                 <p>The identity provider returned an error. \
                 Check your terminal for details.</p>\
                 <p>You can close this tab.</p>\
                 </body></html>";
            let _ = stream.write_all(error_html.as_bytes()).await;
            let _ = stream.shutdown().await;
            return Err(CliError::Remote(format!(
                "authorization failed: {error}: {desc}"
            )));
        }

        let code_raw = params
            .get("code")
            .ok_or_else(|| CliError::Remote("callback missing 'code' parameter".into()))?;
        let state_raw = params
            .get("state")
            .ok_or_else(|| CliError::Remote("callback missing 'state' parameter".into()))?;

        // URL-decode parameters
        let code = urlencoding::decode(code_raw)
            .map_err(|e| CliError::Remote(format!("failed to decode code: {e}")))?
            .into_owned();
        let received_state = urlencoding::decode(state_raw)
            .map_err(|e| CliError::Remote(format!("failed to decode state: {e}")))?
            .into_owned();

        // Serve success page
        let success_html = "HTTP/1.1 200 OK\r\n\
             Content-Type: text/html\r\n\
             Connection: close\r\n\r\n\
             <html><body>\
             <h2>Authentication Successful</h2>\
             <p>You can close this tab and return to the terminal.</p>\
             </body></html>";
        let _ = stream.write_all(success_html.as_bytes()).await;
        let _ = stream.shutdown().await;

        return Ok((code, received_state));
    }
}

/// Generate a random PKCE code_verifier (128 chars, RFC 7636 Section 4.1).
fn generate_code_verifier() -> String {
    use rand::Rng;
    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~";
    let mut rng = rand::thread_rng();
    (0..128)
        .map(|_| {
            let idx = rng.gen_range(0..CHARSET.len());
            CHARSET[idx] as char
        })
        .collect()
}

/// Generate a random state parameter for CSRF protection.
fn generate_random_state() -> String {
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
    use rand::Rng;
    let mut bytes = [0u8; 32];
    rand::thread_rng().fill(&mut bytes);
    URL_SAFE_NO_PAD.encode(bytes)
}

/// Fluree token exchange response.
struct ExchangeResponse {
    access_token: String,
    refresh_token: Option<String>,
}

/// Exchange an IdP token for a Fluree-scoped Bearer token.
///
/// `subject_token_type` should be `urn:ietf:params:oauth:token-type:id_token`
/// when sending an id_token, or `urn:ietf:params:oauth:token-type:access_token`
/// when sending an access_token.
async fn exchange_token(
    http: &reqwest::Client,
    exchange_url: &str,
    subject_token: &str,
    subject_token_type: &str,
) -> CliResult<ExchangeResponse> {
    let body = serde_json::json!({
        "grant_type": "urn:ietf:params:oauth:grant-type:token-exchange",
        "subject_token": subject_token,
        "subject_token_type": subject_token_type
    });

    let resp = http
        .post(exchange_url)
        .json(&body)
        .send()
        .await
        .map_err(|e| CliError::Remote(format!("token exchange failed: {e}")))?;

    if !resp.status().is_success() {
        let status = resp.status();
        let err_body: serde_json::Value = resp.json().await.unwrap_or_default();
        let desc = err_body
            .get("error_description")
            .and_then(|v| v.as_str())
            .or_else(|| err_body.get("error").and_then(|v| v.as_str()))
            .unwrap_or("exchange rejected");
        return Err(CliError::Remote(format!(
            "token exchange failed ({status}): {desc}"
        )));
    }

    let body: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| CliError::Remote(format!("exchange response invalid JSON: {e}")))?;

    let access_token = body
        .get("access_token")
        .and_then(|v| v.as_str())
        .ok_or_else(|| CliError::Remote("exchange response missing access_token".into()))?
        .to_string();

    let refresh_token = body
        .get("refresh_token")
        .and_then(|v| v.as_str())
        .map(String::from);

    Ok(ExchangeResponse {
        access_token,
        refresh_token,
    })
}

// =============================================================================
// Logout
// =============================================================================

async fn run_logout(store: &TomlSyncConfigStore, remote: Option<&str>) -> CliResult<()> {
    let name = resolve_remote_name(store, remote).await?;
    let remote_name = RemoteName::new(&name);
    let mut config = store
        .get_remote(&remote_name)
        .await
        .map_err(|e| CliError::Config(e.to_string()))?
        .ok_or_else(|| CliError::NotFound(format!("remote '{name}' not found")))?;

    if config.auth.token.is_none() && config.auth.refresh_token.is_none() {
        println!("No token stored for remote '{name}'");
        return Ok(());
    }

    config.auth.token = None;
    config.auth.refresh_token = None;
    store
        .set_remote(&config)
        .await
        .map_err(|e| CliError::Config(e.to_string()))?;

    println!("Token cleared for remote '{}'", name.green());
    Ok(())
}

// =============================================================================
// Token summary decoding (no verification)
// =============================================================================

struct TokenSummary {
    expiry: Option<String>,
    identity: Option<String>,
    issuer: Option<String>,
    subject: Option<String>,
}

/// Decode a JWT/JWS payload without verification to extract summary info.
fn decode_token_summary(token: &str) -> Result<TokenSummary, ()> {
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};

    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        return Err(());
    }

    let payload_bytes = URL_SAFE_NO_PAD.decode(parts[1]).map_err(|_| ())?;
    let claims: serde_json::Value = serde_json::from_slice(&payload_bytes).map_err(|_| ())?;

    let expiry = claims
        .get("exp")
        .and_then(serde_json::Value::as_u64)
        .map(|exp| {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            if exp < now {
                format!("{} (expired)", format_timestamp(exp))
                    .red()
                    .to_string()
            } else {
                format_timestamp(exp)
            }
        });

    let identity = claims
        .get("fluree.identity")
        .and_then(|v| v.as_str())
        .map(String::from);

    let issuer = claims.get("iss").and_then(|v| v.as_str()).map(String::from);

    let subject = claims.get("sub").and_then(|v| v.as_str()).map(String::from);

    Ok(TokenSummary {
        expiry,
        identity,
        issuer,
        subject,
    })
}

/// Format a unix timestamp as a human-readable string.
fn format_timestamp(ts: u64) -> String {
    let secs = ts;
    let days_since_epoch = secs / 86400;
    let time_of_day = secs % 86400;
    let hours = time_of_day / 3600;
    let minutes = (time_of_day % 3600) / 60;

    let mut y = 1970i64;
    let mut remaining = days_since_epoch as i64;
    loop {
        let days_in_year = if is_leap_year(y) { 366 } else { 365 };
        if remaining < days_in_year {
            break;
        }
        remaining -= days_in_year;
        y += 1;
    }
    let mut m = 1u32;
    let days_in_month = [
        31,
        28 + i64::from(is_leap_year(y)),
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    for &dim in &days_in_month {
        if remaining < dim {
            break;
        }
        remaining -= dim;
        m += 1;
    }
    let d = remaining + 1;

    format!("{y:04}-{m:02}-{d:02} {hours:02}:{minutes:02} UTC")
}

fn is_leap_year(y: i64) -> bool {
    (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0)
}
