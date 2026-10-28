//! `fluree login` / `fluree logout`: sign this machine in to (or out of) a
//! fluree.ai account.
//!
//! The protocol is the one `fluree auth login` speaks to a remote
//! (`docs/design/auth-contract.md`): discovery at
//! `<url>/.well-known/fluree.json`, the OAuth device authorization grant
//! (RFC 8628) at the discovered issuer, then the token exchange. What
//! differs is where the result lives: in the machine's `account.toml`
//! ([`crate::account`]), not in a project's remote config, because an
//! account is not a Fluree data API.

use crate::account::{self, AccountSummary, StoredAccount, DEFAULT_ACCOUNT_URL};
use crate::error::{CliError, CliResult};
use colored::Colorize;

/// What to suggest for signing in to `url` (used by `remote add`'s refusal).
pub(crate) fn login_command_for(url: &str) -> String {
    format!("fluree login{}", account::login_url_flag(url))
}

fn http_client() -> CliResult<reqwest::Client> {
    reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .map_err(|e| CliError::Remote(format!("failed to build HTTP client: {e}")))
}

pub async fn run_login(url: Option<String>, open_browser: bool) -> CliResult<()> {
    let url = url
        .unwrap_or_else(|| DEFAULT_ACCOUNT_URL.to_string())
        .trim_end_matches('/')
        .to_string();
    let http = http_client()?;

    let discovered = match super::remote::discover_remote(&url).await {
        Ok(Some(d)) => d,
        Ok(None) => {
            return Err(CliError::Remote(format!(
                "{url} offers no sign-in (no /.well-known/fluree.json)"
            )))
        }
        Err(e) => return Err(CliError::Remote(format!("discovery at {url} failed: {e}"))),
    };
    let Some(service) = discovered.account else {
        return Err(CliError::Usage(format!(
            "{url} is a Fluree server, not an account service\n  \
             hint: add it as a remote instead: fluree remote add <name> {url} && fluree auth login --remote <name>"
        )));
    };
    if let Some(ref min) = discovered.cli_min_version {
        if let Some(warning) =
            super::remote::cli_min_version_warning(env!("CARGO_PKG_VERSION"), min)
        {
            eprintln!("  {} {}", "warn:".yellow().bold(), warning);
        }
    }
    let auth = discovered
        .auth
        .ok_or_else(|| CliError::Remote(format!("{url} advertises no sign-in method")))?;
    let (Some(issuer), Some(client_id), Some(exchange_url)) =
        (auth.issuer, auth.client_id, auth.exchange_url)
    else {
        return Err(CliError::Remote(format!(
            "{url}'s discovery is missing issuer, client_id or exchange_url"
        )));
    };
    let scopes = auth
        .scopes
        .filter(|s| !s.is_empty())
        .map_or_else(|| "openid".to_string(), |s| s.join(" "));

    let super::auth::OidcFlow::DeviceCode {
        device_authorization_endpoint,
        token_endpoint,
    } = super::auth::discover_oidc_endpoints(&http, &issuer).await?
    else {
        return Err(CliError::Remote(format!(
            "{url} does not offer a device sign-in"
        )));
    };

    let host = hostname();
    let grant = super::auth::run_device_code_flow(
        &http,
        &device_authorization_endpoint,
        &token_endpoint,
        &client_id,
        &scopes,
        host.as_deref(),
        open_browser,
    )
    .await?;
    eprintln!();

    let exchanged = super::auth::exchange_token(
        &http,
        &exchange_url,
        &grant.access_token,
        "urn:ietf:params:oauth:token-type:access_token",
    )
    .await?;
    let refresh_token = exchanged.refresh_token.ok_or_else(|| {
        CliError::Remote(format!(
            "{url} signed in but issued no refresh token, so the sign-in would expire within the hour"
        ))
    })?;

    let previous = account::load().ok().flatten();
    let stored = StoredAccount {
        url: url.clone(),
        service: service.service,
        api_base_url: service.api_base_url,
        dashboard_url: service.dashboard_url,
        exchange_url,
        access_token: exchanged.access_token,
        refresh_token,
        deployment_id: exchanged
            .body
            .get("deployment_id")
            .and_then(|v| v.as_str())
            .map(String::from),
        account: AccountSummary::from_exchange(&exchanged.body),
    };
    let path = account::save(&stored)?;

    // Signing in again replaces this machine's previous sign-in; sign that
    // one out on the server too, so it doesn't linger as a live credential.
    if let Some(mut old) = previous {
        if old.refresh_token != stored.refresh_token {
            if let Err(e) = sign_out_on_server(&http, &mut old).await {
                eprintln!(
                    "  {} the previous sign-in could not be signed out on the server ({e}); \
                     disconnect it from the dashboard",
                    "note:".cyan().bold()
                );
            }
        }
    }

    println!("Signed in to {}", describe(&stored).green());
    println!("  Credentials: {}", path.display());
    Ok(())
}

pub async fn run_logout() -> CliResult<()> {
    let Some(mut stored) = account::load()? else {
        println!("Not signed in");
        return Ok(());
    };
    let http = http_client()?;
    let server = sign_out_on_server(&http, &mut stored).await;
    account::remove()?;
    match server {
        Ok(()) => println!("Signed out of {}", describe(&stored)),
        Err(e) => {
            println!("Signed out locally");
            eprintln!(
                "  {} the server did not confirm the sign-out ({e}); disconnect this machine from {}",
                "warn:".yellow().bold(),
                stored.dashboard_url.as_deref().unwrap_or(&stored.url)
            );
        }
    }
    Ok(())
}

/// Revoke the machine's credential on the server: `POST {api}/auth/disconnect`
/// with the access token (refreshed first if it has expired).
async fn sign_out_on_server(http: &reqwest::Client, stored: &mut StoredAccount) -> CliResult<()> {
    if !account::token_seconds_left(&stored.access_token).is_some_and(|s| s > 0) {
        account::refresh(http, stored).await?;
    }
    let url = format!("{}/auth/disconnect", stored.api_base_url);
    let resp = http
        .post(&url)
        .bearer_auth(&stored.access_token)
        .send()
        .await
        .map_err(|e| CliError::Remote(e.to_string()))?;
    if resp.status().is_success() {
        Ok(())
    } else {
        Err(CliError::Remote(format!(
            "{url} answered {}",
            resp.status()
        )))
    }
}

/// `owner@acme.test (Acme) on fluree.ai`, with whatever parts are known.
pub(crate) fn describe(stored: &StoredAccount) -> String {
    let service = stored.service.as_deref().unwrap_or(&stored.url);
    let summary = stored.account.clone().unwrap_or_default();
    match (summary.email, summary.org_name) {
        (Some(email), Some(org)) => format!("{service} as {email} ({org})"),
        (Some(email), None) => format!("{service} as {email}"),
        (None, Some(org)) => format!("{service} ({org})"),
        (None, None) => service.to_string(),
    }
}

/// This machine's host name, shown on the approval page so the person can
/// tell which terminal is asking.
fn hostname() -> Option<String> {
    #[cfg(unix)]
    {
        let mut buf = [0u8; 256];
        // SAFETY: buf is a valid, writable buffer of the length passed.
        let rc = unsafe { libc::gethostname(buf.as_mut_ptr().cast(), buf.len()) };
        if rc == 0 {
            let end = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
            let name = String::from_utf8_lossy(&buf[..end]).trim().to_string();
            if !name.is_empty() {
                return Some(name);
            }
        }
        None
    }
    #[cfg(not(unix))]
    {
        std::env::var("COMPUTERNAME").ok().filter(|n| !n.is_empty())
    }
}
