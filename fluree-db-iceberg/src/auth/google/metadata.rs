//! Tokens from the GCE/GKE metadata server: the attached service account of a
//! workload running on Google (GKE Workload Identity, a GCE VM).
//!
//! The server is link-local (`metadata.google.internal` → 169.254.169.254), so
//! the SSRF-hardened client cannot reach it. Instead the URL is pinned — a
//! constant, overridable only by the operator through
//! [`METADATA_URL_ENV`](super::METADATA_URL_ENV), never by a source — and the
//! client follows no redirects.

use std::time::Duration;

use crate::auth::token::CachedToken;
use crate::error::{GoogleTokenStage, IcebergError, Result};

use super::wire;

/// A metadata-server token source.
pub(super) struct MetadataSource {
    url: String,
    http: reqwest::Client,
}

impl MetadataSource {
    pub(super) fn new(url: &str) -> Result<Self> {
        let parsed = reqwest::Url::parse(url).map_err(|_| metadata_url_error())?;
        if !matches!(parsed.scheme(), "http" | "https") || parsed.host_str().is_none() {
            return Err(metadata_url_error());
        }
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|e| IcebergError::Http(format!("Failed to build HTTP client: {e}")))?;
        Ok(Self {
            url: url.to_string(),
            http,
        })
    }

    pub(super) fn url(&self) -> &str {
        &self.url
    }

    pub(super) async fn fetch(&self, scopes: &[String]) -> Result<CachedToken> {
        let stage = GoogleTokenStage::Metadata;
        let response = self
            .http
            .get(&self.url)
            .query(&[("scopes", scopes.join(","))])
            // The metadata server requires this header on every request.
            .header("Metadata-Flavor", "Google")
            .send()
            .await
            .map_err(|e| wire::transport_error(stage, &e))?;
        if !response.status().is_success() {
            // The status only: the metadata server's body is not ours to echo.
            return Err(IcebergError::GoogleTokenExchange {
                stage,
                status: Some(response.status().as_u16()),
                reason: None,
            });
        }
        wire::read_token(stage, response).await
    }
}

fn metadata_url_error() -> IcebergError {
    IcebergError::Config(format!(
        "{} must be an http(s) URL",
        super::METADATA_URL_ENV
    ))
}

/// A stored `google_metadata.metadata_url` is accepted only when it names the
/// URL this process already uses (the pinned constant, or the operator's
/// override): a per-source value never chooses where a request goes, and one
/// that differs is refused rather than silently ignored.
pub(super) fn check_stored_url(stored: Option<&str>, effective: &str) -> Result<()> {
    match stored {
        None => Ok(()),
        Some(url) if url == effective => Ok(()),
        Some(_) => Err(IcebergError::GoogleCredentialsInvalid(format!(
            "google_metadata.metadata_url is not this instance's metadata server; the URL is \
             fixed and only the operator can override it ({})",
            super::METADATA_URL_ENV
        ))),
    }
}
