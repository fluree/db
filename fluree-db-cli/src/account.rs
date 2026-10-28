//! The fluree.ai account this machine is signed in to (`fluree login`).
//!
//! An account is not a remote: remotes are Fluree data APIs, and there is one
//! account per machine, kept in `account.toml` in the global config
//! directory (`$FLUREE_HOME`, else the platform config dir), never in a
//! project's `.fluree/`. The file holds credentials, so it is written 0600
//! and replaced atomically.
//!
//! The access token is short-lived (an hour on fluree.ai); [`session`]
//! returns one with time left, refreshing it through the account's exchange
//! endpoint (`grant_type=refresh_token`) and saving the result.

use crate::error::{CliError, CliResult};
use fluree_db_api::server_defaults::FlureeDir;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// File name inside the global config directory.
pub const ACCOUNT_FILE: &str = "account.toml";

/// Where `fluree login` signs in when no `--url` is given.
pub const DEFAULT_ACCOUNT_URL: &str = "https://fluree.ai";

/// Refresh when less than this much of the access token is left.
const REFRESH_MARGIN_SECS: i64 = 60;

/// What `fluree login` stores.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StoredAccount {
    /// The URL signed in to (discovery is `<url>/.well-known/fluree.json`).
    pub url: String,
    /// Display name of the service (`fluree.ai`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service: Option<String>,
    /// Base of the account's API, e.g. `https://fluree.ai/v1`.
    pub api_base_url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dashboard_url: Option<String>,
    /// Token endpoint for refresh (`grant_type=refresh_token`).
    pub exchange_url: String,
    pub access_token: String,
    pub refresh_token: String,
    /// The server-side record of this machine's sign-in, when reported.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deployment_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account: Option<AccountSummary>,
}

/// Who the machine is signed in as, as the exchange reported it.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct AccountSummary {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub org_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tier: Option<String>,
}

impl AccountSummary {
    /// The `account` object of a fluree.ai exchange response.
    pub fn from_exchange(body: &serde_json::Value) -> Option<Self> {
        let account = body.get("account")?;
        let field = |k: &str| account.get(k).and_then(|v| v.as_str()).map(String::from);
        Some(Self {
            account_id: field("account_id"),
            org_name: field("org_name"),
            email: field("email"),
            tier: field("tier"),
        })
    }
}

/// A usable access token for the signed-in account.
#[derive(Debug, Clone)]
pub struct AccountSession {
    pub api_base_url: String,
    pub access_token: String,
}

/// `account.toml` in the global config directory.
pub fn account_path() -> CliResult<PathBuf> {
    let dirs = FlureeDir::global()
        .ok_or_else(|| CliError::Config("no global config directory (set FLUREE_HOME)".into()))?;
    Ok(dirs.config_dir().join(ACCOUNT_FILE))
}

/// The stored sign-in, or `None` when this machine isn't signed in.
pub fn load() -> CliResult<Option<StoredAccount>> {
    load_from(&account_path()?)
}

pub fn load_from(path: &Path) -> CliResult<Option<StoredAccount>> {
    match std::fs::read_to_string(path) {
        Ok(text) => toml::from_str(&text).map(Some).map_err(|e| {
            CliError::Config(format!(
                "{} is unreadable ({e}); run `fluree logout` and sign in again",
                path.display()
            ))
        }),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(CliError::Config(format!(
            "failed to read {}: {e}",
            path.display()
        ))),
    }
}

/// Write the sign-in: owner-only permissions, replaced atomically so a
/// crash never leaves half a credential file.
pub fn save(account: &StoredAccount) -> CliResult<PathBuf> {
    let path = account_path()?;
    save_to(&path, account)?;
    Ok(path)
}

pub fn save_to(path: &Path, account: &StoredAccount) -> CliResult<()> {
    let dir = path
        .parent()
        .ok_or_else(|| CliError::Config(format!("bad account path {}", path.display())))?;
    std::fs::create_dir_all(dir)
        .map_err(|e| CliError::Config(format!("failed to create {}: {e}", dir.display())))?;
    let body = toml::to_string(account)
        .map_err(|e| CliError::Config(format!("failed to encode the sign-in: {e}")))?;
    let text = format!(
        "# Written by `fluree login`. It holds credentials for your fluree.ai account:\n\
         # keep it private, and sign out with `fluree logout`.\n\n{body}"
    );
    let tmp = dir.join(format!(".{ACCOUNT_FILE}.{}", std::process::id()));
    write_private(&tmp, text.as_bytes())?;
    std::fs::rename(&tmp, path).map_err(|e| {
        let _ = std::fs::remove_file(&tmp);
        CliError::Config(format!("failed to write {}: {e}", path.display()))
    })
}

#[cfg(unix)]
fn write_private(path: &Path, bytes: &[u8]) -> CliResult<()> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)
        .map_err(|e| CliError::Config(format!("failed to write {}: {e}", path.display())))?;
    file.write_all(bytes)
        .map_err(|e| CliError::Config(format!("failed to write {}: {e}", path.display())))
}

#[cfg(not(unix))]
fn write_private(path: &Path, bytes: &[u8]) -> CliResult<()> {
    std::fs::write(path, bytes)
        .map_err(|e| CliError::Config(format!("failed to write {}: {e}", path.display())))
}

/// Remove the stored sign-in. `false` when there was none.
pub fn remove() -> CliResult<bool> {
    let path = account_path()?;
    match std::fs::remove_file(&path) {
        Ok(()) => Ok(true),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(CliError::Config(format!(
            "failed to remove {}: {e}",
            path.display()
        ))),
    }
}

/// Seconds until a JWT's `exp`, read without verifying it (this only decides
/// whether to refresh). `None` for anything without a numeric `exp`.
pub fn token_seconds_left(token: &str) -> Option<i64> {
    use base64::Engine;
    let payload = token.split('.').nth(1)?;
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload)
        .ok()?;
    let claims: serde_json::Value = serde_json::from_slice(&bytes).ok()?;
    let exp = claims.get("exp")?.as_i64()?;
    Some(exp - chrono::Utc::now().timestamp())
}

/// Trade the refresh token for a new access token. Updates `account` in
/// place (including a rotated refresh token, if the server sends one).
pub async fn refresh(http: &reqwest::Client, account: &mut StoredAccount) -> CliResult<()> {
    let resp = http
        .post(&account.exchange_url)
        .json(&serde_json::json!({
            "grant_type": "refresh_token",
            "refresh_token": account.refresh_token,
        }))
        .send()
        .await
        .map_err(|e| CliError::Remote(format!("token refresh failed: {e}")))?;
    let status = resp.status();
    let body: serde_json::Value = resp.json().await.unwrap_or_default();
    if !status.is_success() {
        let desc = body
            .get("error_description")
            .or_else(|| body.get("error"))
            .and_then(|v| v.as_str())
            .unwrap_or("refresh rejected");
        return Err(CliError::Remote(format!(
            "your sign-in is no longer valid ({desc})\n  hint: fluree login{}",
            login_url_flag(&account.url)
        )));
    }
    let access_token = body
        .get("access_token")
        .and_then(|v| v.as_str())
        .ok_or_else(|| CliError::Remote("refresh response missing access_token".into()))?;
    account.access_token = access_token.to_string();
    if let Some(rotated) = body.get("refresh_token").and_then(|v| v.as_str()) {
        account.refresh_token = rotated.to_string();
    }
    if let Some(summary) = AccountSummary::from_exchange(&body) {
        account.account = Some(summary);
    }
    Ok(())
}

/// An access token with time left for the signed-in account, refreshing
/// (and saving) it first when it is about to expire. `Ok(None)` when this
/// machine isn't signed in.
pub async fn session(http: &reqwest::Client) -> CliResult<Option<AccountSession>> {
    let Some(mut account) = load()? else {
        return Ok(None);
    };
    let fresh = token_seconds_left(&account.access_token).is_some_and(|s| s > REFRESH_MARGIN_SECS);
    if !fresh {
        refresh(http, &mut account).await?;
        save(&account)?;
    }
    Ok(Some(AccountSession {
        api_base_url: account.api_base_url,
        access_token: account.access_token,
    }))
}

/// ` --url <url>` unless it is the default, for hints.
pub fn login_url_flag(url: &str) -> String {
    if url.trim_end_matches('/') == DEFAULT_ACCOUNT_URL {
        String::new()
    } else {
        format!(" --url {url}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> StoredAccount {
        StoredAccount {
            url: "https://fluree.ai".into(),
            service: Some("fluree.ai".into()),
            api_base_url: "https://fluree.ai/v1".into(),
            dashboard_url: Some("https://fluree.ai".into()),
            exchange_url: "https://fluree.ai/v1/oauth/exchange".into(),
            access_token: "a.b.c".into(),
            refresh_token: "flk_x_y".into(),
            deployment_id: Some("dep-1".into()),
            account: Some(AccountSummary {
                email: Some("owner@acme.test".into()),
                org_name: Some("Acme".into()),
                ..Default::default()
            }),
        }
    }

    #[test]
    fn round_trips_and_is_owner_only() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nested").join(ACCOUNT_FILE);
        save_to(&path, &sample()).unwrap();
        assert_eq!(load_from(&path).unwrap(), Some(sample()));
        let text = std::fs::read_to_string(&path).unwrap();
        assert!(text.starts_with("# Written by `fluree login`"));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600);
        }
        // Rewriting replaces the file rather than appending to it.
        let mut changed = sample();
        changed.access_token = "d.e.f".into();
        save_to(&path, &changed).unwrap();
        assert_eq!(load_from(&path).unwrap(), Some(changed));
    }

    #[test]
    fn missing_file_is_not_signed_in() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(load_from(&dir.path().join(ACCOUNT_FILE)).unwrap(), None);
    }

    #[test]
    fn reads_seconds_left_from_exp() {
        use base64::Engine;
        let exp = chrono::Utc::now().timestamp() + 600;
        let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(serde_json::json!({ "exp": exp }).to_string());
        let left = token_seconds_left(&format!("h.{payload}.s")).unwrap();
        assert!((595..=600).contains(&left));
        assert_eq!(token_seconds_left("flk_not_a_jwt"), None);
    }

    #[test]
    fn hints_name_the_url_only_when_not_default() {
        assert_eq!(login_url_flag("https://fluree.ai/"), "");
        assert_eq!(
            login_url_flag("https://dev.example"),
            " --url https://dev.example"
        );
    }
}
