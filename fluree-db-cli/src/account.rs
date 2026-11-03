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
//! endpoint (`grant_type=refresh_token`) and saving the result. A refresh
//! holds a lock on `account.toml.lock`, so two processes near expiry don't
//! both spend the same refresh token. [`send`] makes a call with the
//! session and, when the service says the token predates a configuration
//! change (401 `config_stale`), refreshes and sends it once more.

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

/// How a refresh refused with `invalid_grant` begins: the server revoked
/// this machine's sign-in (logout, or disconnected from the dashboard).
const SIGN_IN_REVOKED: &str = "your sign-in is no longer valid";

/// Whether `err` (from [`refresh`]) means the server revoked the sign-in,
/// as opposed to a network or server failure.
pub fn is_revoked(err: &CliError) -> bool {
    matches!(err, CliError::Remote(msg) if msg.starts_with(SIGN_IN_REVOKED))
}

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
                "{} is unreadable ({e}); `fluree login` signs in again (and `fluree logout` removes it)",
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
    let tmp = dir.join(temp_name());
    write_private(&tmp, text.as_bytes())?;
    std::fs::rename(&tmp, path).map_err(|e| {
        let _ = std::fs::remove_file(&tmp);
        CliError::Config(format!("failed to write {}: {e}", path.display()))
    })
}

/// A temp file name no other save shares: pid, a per-process counter and
/// the time, so two saves in one process (or two processes) never collide.
fn temp_name() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SAVES: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.subsec_nanos());
    format!(
        ".{ACCOUNT_FILE}.{}.{}.{nanos}",
        std::process::id(),
        SAVES.fetch_add(1, Ordering::Relaxed)
    )
}

/// Holds `account.toml.lock` exclusively until dropped: refreshes from two
/// processes take turns, and the second re-reads what the first saved.
struct RefreshLock(#[allow(dead_code)] std::fs::File);

fn lock_for(path: &Path) -> CliResult<RefreshLock> {
    let lock_path = path.with_file_name(format!("{ACCOUNT_FILE}.lock"));
    if let Some(dir) = lock_path.parent() {
        std::fs::create_dir_all(dir)
            .map_err(|e| CliError::Config(format!("failed to create {}: {e}", dir.display())))?;
    }
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&lock_path)
        .map_err(|e| CliError::Config(format!("failed to open {}: {e}", lock_path.display())))?;
    file.lock()
        .map_err(|e| CliError::Config(format!("failed to lock {}: {e}", lock_path.display())))?;
    Ok(RefreshLock(file))
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

/// What a `grant_type=refresh_token` call answered: its status and JSON
/// body. Shared by the account's refresh and a remote's.
pub(crate) struct RefreshAnswer {
    pub status: reqwest::StatusCode,
    pub body: serde_json::Value,
}

impl RefreshAnswer {
    pub fn access_token(&self) -> Option<&str> {
        self.body.get("access_token").and_then(|v| v.as_str())
    }

    /// A refresh token the server rotated in, if it sent one.
    pub fn refresh_token(&self) -> Option<&str> {
        self.body.get("refresh_token").and_then(|v| v.as_str())
    }
}

/// `POST exchange_url` with `grant_type=refresh_token`. Only transport
/// failures are errors; the caller reads the status.
pub(crate) async fn post_refresh(
    http: &reqwest::Client,
    exchange_url: &str,
    refresh_token: &str,
) -> Result<RefreshAnswer, reqwest::Error> {
    let resp = http
        .post(exchange_url)
        .json(&serde_json::json!({
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
        }))
        .send()
        .await?;
    let status = resp.status();
    let body = resp.json().await.unwrap_or_default();
    Ok(RefreshAnswer { status, body })
}

/// Trade the refresh token for a new access token. Updates `account` in
/// place (including a rotated refresh token, if the server sends one).
pub async fn refresh(http: &reqwest::Client, account: &mut StoredAccount) -> CliResult<()> {
    let answer = post_refresh(http, &account.exchange_url, &account.refresh_token)
        .await
        .map_err(|e| CliError::Remote(format!("token refresh failed: {e}")))?;
    let (status, body) = (answer.status, &answer.body);
    if !status.is_success() {
        let desc = body
            .get("error_description")
            .or_else(|| body.get("error"))
            .and_then(|v| v.as_str())
            .unwrap_or("refresh rejected");
        if body.get("error").and_then(|v| v.as_str()) == Some("invalid_grant") {
            return Err(CliError::Remote(format!(
                "{SIGN_IN_REVOKED} ({desc})\n  hint: fluree login{}",
                login_url_flag(&account.url)
            )));
        }
        return Err(CliError::Remote(format!(
            "token refresh failed ({status}): {desc}"
        )));
    }
    let access_token = answer
        .access_token()
        .ok_or_else(|| CliError::Remote("refresh response missing access_token".into()))?;
    account.access_token = access_token.to_string();
    if let Some(rotated) = answer.refresh_token() {
        account.refresh_token = rotated.to_string();
    }
    if let Some(summary) = AccountSummary::from_exchange(body) {
        account.account = Some(summary);
    }
    Ok(())
}

/// An access token with time left for the signed-in account, refreshing
/// (and saving) it first when it is about to expire, or always with
/// `force`. `Ok(None)` when this machine isn't signed in.
pub async fn session(http: &reqwest::Client, force: bool) -> CliResult<Option<AccountSession>> {
    session_at(http, &account_path()?, force).await
}

/// [`session`] for the sign-in stored at `path`.
pub async fn session_at(
    http: &reqwest::Client,
    path: &Path,
    force: bool,
) -> CliResult<Option<AccountSession>> {
    let fresh = |a: &StoredAccount| {
        token_seconds_left(&a.access_token).is_some_and(|s| s > REFRESH_MARGIN_SECS)
    };
    let Some(seen) = load_from(path)? else {
        return Ok(None);
    };
    if !force && fresh(&seen) {
        return Ok(Some(AccountSession::of(seen)));
    }
    // Waiting for another process's refresh blocks, so not on the runtime.
    let lock_path = path.to_path_buf();
    let _lock = tokio::task::spawn_blocking(move || lock_for(&lock_path))
        .await
        .map_err(|e| CliError::Config(format!("waiting for the sign-in lock: {e}")))??;
    // Re-read under the lock: another process may have refreshed already.
    let Some(mut account) = load_from(path)? else {
        return Ok(None);
    };
    let refreshed_meanwhile = account.access_token != seen.access_token;
    if !(fresh(&account) && (refreshed_meanwhile || !force)) {
        refresh(http, &mut account).await?;
        save_to(path, &account)?;
    }
    Ok(Some(AccountSession::of(account)))
}

impl AccountSession {
    fn of(account: StoredAccount) -> Self {
        Self {
            api_base_url: account.api_base_url,
            access_token: account.access_token,
        }
    }
}

/// Whether a response says the access token predates a change to the
/// account's configuration: 401 with `config_stale`, which a fresh token
/// cures. The code sits at `error.error` (or a top-level `error`).
pub fn is_config_stale(status: reqwest::StatusCode, body: &serde_json::Value) -> bool {
    status == reqwest::StatusCode::UNAUTHORIZED
        && body
            .pointer("/error/error")
            .or_else(|| body.get("error"))
            .and_then(|v| v.as_str())
            == Some("config_stale")
}

/// Send a request built by `build` with the signed-in account's token. On
/// a 401 `config_stale` the token is refreshed and the request sent once
/// more; any other 401 is an error. `Ok(None)` when this machine isn't
/// signed in.
pub async fn send<F>(http: &reqwest::Client, build: F) -> CliResult<Option<reqwest::Response>>
where
    F: Fn(&AccountSession) -> reqwest::RequestBuilder,
{
    send_at(http, &account_path()?, build).await
}

/// [`send`] for the sign-in stored at `path`.
pub async fn send_at<F>(
    http: &reqwest::Client,
    path: &Path,
    build: F,
) -> CliResult<Option<reqwest::Response>>
where
    F: Fn(&AccountSession) -> reqwest::RequestBuilder,
{
    let Some(session) = session_at(http, path, false).await? else {
        return Ok(None);
    };
    let resp = build(&session)
        .send()
        .await
        .map_err(|e| CliError::Remote(e.to_string()))?;
    if resp.status() != reqwest::StatusCode::UNAUTHORIZED {
        return Ok(Some(resp));
    }
    let status = resp.status();
    let url = resp.url().to_string();
    let body: serde_json::Value = resp.json().await.unwrap_or_default();
    if !is_config_stale(status, &body) {
        // A refused token a refresh won't cure.
        let message = body
            .pointer("/error/message")
            .or_else(|| body.get("error_description"))
            .and_then(|v| v.as_str())
            .unwrap_or("the sign-in was refused");
        return Err(CliError::Remote(format!(
            "{url} answered {status}: {message}"
        )));
    }
    let Some(session) = session_at(http, path, true).await? else {
        return Ok(None);
    };
    let resp = build(&session)
        .send()
        .await
        .map_err(|e| CliError::Remote(e.to_string()))?;
    Ok(Some(resp))
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
