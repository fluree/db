//! HTTP origin fetchers for CID-based content retrieval.
//!
//! Provides [`HttpOriginFetcher`] (single origin) and [`MultiOriginFetcher`]
//! (priority-ordered fallback) for fetching CAS objects from remote Fluree
//! servers via `GET /storage/objects/{cid}`.
//!
//! Used by `fluree clone --origin` and `fluree pull` to walk commit chains
//! by CID without needing pre-configured storage credentials.

use crate::error::{Result, SyncError};
use fluree_db_core::pack::PackRequest;
use fluree_db_core::{ContentId, CODEC_FLUREE_COMMIT};
use fluree_db_nameservice::NsRecord;
use fluree_db_nameservice::{LedgerConfig, Origin};
use tracing::{debug, warn};

// ---------------------------------------------------------------------------
// Integrity verification
// ---------------------------------------------------------------------------

/// Verify fetched object bytes against a CID, with format-sniffing for commits.
///
/// - Commit blobs (`FCV2` magic): SHA-256 of full blob via `verify_commit_blob`
/// - All other kinds (txn, config, dict, index, etc.): full-bytes SHA-256
///
/// **Forward-compat note:** If a future commit format uses `CODEC_FLUREE_COMMIT`
/// but has different hashing rules, add its magic-byte check here — the
/// `id.verify(bytes)` fallback assumes full-bytes SHA-256.
pub fn verify_object_integrity(id: &ContentId, bytes: &[u8]) -> bool {
    const COMMIT_V2_MAGIC: &[u8] = b"FCV2";

    if id.codec() == CODEC_FLUREE_COMMIT && bytes.starts_with(COMMIT_V2_MAGIC) {
        match fluree_db_core::commit::codec::verify_commit_blob(bytes) {
            Ok(derived_id) => derived_id == *id,
            Err(_) => false,
        }
    } else {
        id.verify(bytes)
    }
}

/// Check whether an origin's auth requirement is satisfiable with the
/// available credentials.
fn is_auth_satisfiable(origin: &Origin, has_token: bool) -> bool {
    match origin.auth.mode.as_str() {
        "none" => true,
        "bearer" => has_token,
        _ => false, // oidc-device, mtls, signed-request — not yet supported
    }
}

// ---------------------------------------------------------------------------
// HttpOriginFetcher
// ---------------------------------------------------------------------------

/// Fetches CAS objects from a single HTTP origin via
/// `GET {base}/storage/objects/{cid}?ledger={ledger}`.
#[derive(Debug, Clone)]
pub struct HttpOriginFetcher {
    base_url: String,
    auth_token: Option<String>,
    http: reqwest::Client,
}

impl HttpOriginFetcher {
    /// Create a new fetcher for a single origin.
    ///
    /// `base_url` is normalized the same way as `HttpRemoteClient::new()`:
    /// trailing slashes are stripped, and `/fluree` is appended if absent.
    pub fn new(
        base_url: impl Into<String>,
        auth_token: Option<String>,
        http: reqwest::Client,
    ) -> Self {
        let raw = base_url.into();
        let trimmed = raw.trim_end_matches('/').to_string();
        let normalized = if trimmed.ends_with("/fluree") {
            trimmed
        } else {
            format!("{trimmed}/fluree")
        };
        Self {
            base_url: normalized,
            auth_token,
            http,
        }
    }

    /// The normalized base URL for this fetcher.
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    fn add_auth(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        if let Some(ref token) = self.auth_token {
            req.bearer_auth(token)
        } else {
            req
        }
    }

    /// Fetch raw bytes for a CAS object by CID.
    ///
    /// Does NOT verify integrity — the caller (`MultiOriginFetcher::fetch`)
    /// is responsible for calling `verify_object_integrity`.
    pub async fn fetch_object(&self, id: &ContentId, ledger: &str) -> Result<Vec<u8>> {
        let cid_str = id.to_string();
        let url = format!(
            "{}/storage/objects/{}?ledger={}",
            self.base_url,
            urlencoding::encode(&cid_str),
            urlencoding::encode(ledger),
        );

        debug!(url = %url, "fetching CAS object");
        let resp = self.add_auth(self.http.get(&url)).send().await?;

        match resp.status().as_u16() {
            200 => {
                let bytes = resp.bytes().await?;
                Ok(bytes.to_vec())
            }
            status => {
                let body = resp.text().await.unwrap_or_default();
                if body.trim().is_empty() {
                    Err(SyncError::Remote(format!(
                        "Object fetch failed with status {status} for {url}"
                    )))
                } else {
                    Err(SyncError::Remote(format!(
                        "Object fetch failed with status {status} for {url}: {body}"
                    )))
                }
            }
        }
    }

    /// Attempt to fetch a pack stream from this origin.
    ///
    /// Returns `Ok(Some(response))` on 200 — the caller feeds it to
    /// [`ingest_pack_stream`](crate::pack_client::ingest_pack_stream).
    /// Returns `Ok(None)` on 404/405/406/501 (server doesn't support pack).
    pub async fn fetch_pack_response(
        &self,
        ledger: &str,
        request: &PackRequest,
    ) -> Result<Option<reqwest::Response>> {
        let url = format!("{}/pack/{}", self.base_url, urlencoding::encode(ledger));

        debug!(url = %url, "requesting pack stream");
        let body = serde_json::to_vec(request).map_err(|e| {
            SyncError::PackProtocol(format!("failed to serialize pack request: {e}"))
        })?;

        let resp = self
            .add_auth(
                self.http
                    .post(&url)
                    .header("Content-Type", "application/json")
                    .header("Accept", "application/x-fluree-pack"),
            )
            .body(body)
            .send()
            .await?;

        match resp.status().as_u16() {
            200 => Ok(Some(resp)),
            404..=406 | 501 => Ok(None),
            status => {
                let body = resp.text().await.unwrap_or_default();
                Err(SyncError::Remote(format!(
                    "pack request failed ({status}): {body}"
                )))
            }
        }
    }

    /// Fetch the nameservice record for a ledger alias.
    ///
    /// Returns `None` if the ledger is not found (404).
    pub async fn fetch_ns_record(&self, alias: &str) -> Result<Option<NsRecord>> {
        let url = format!(
            "{}/storage/ns/{}",
            self.base_url,
            urlencoding::encode(alias),
        );

        debug!(url = %url, "fetching NsRecord");
        let resp = self.add_auth(self.http.get(&url)).send().await?;

        match resp.status().as_u16() {
            200 => {
                let record: NsRecord = resp.json().await?;
                Ok(Some(record))
            }
            404 => Ok(None),
            status => {
                let body = resp.text().await.unwrap_or_default();
                if body.trim().is_empty() {
                    Err(SyncError::Remote(format!(
                        "NS lookup failed with status {status} for {url}"
                    )))
                } else {
                    Err(SyncError::Remote(format!(
                        "NS lookup failed with status {status} for {url}: {body}"
                    )))
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// MultiOriginFetcher
// ---------------------------------------------------------------------------

/// Tries multiple HTTP origins in priority order until one succeeds.
///
/// Built from a [`LedgerConfig`] (filtering origins by auth satisfiability)
/// or from a single bootstrap URI.
#[derive(Debug, Clone)]
pub struct MultiOriginFetcher {
    fetchers: Vec<HttpOriginFetcher>,
    /// Number of origins skipped due to unsatisfied auth requirements.
    skipped_origins: usize,
}

impl MultiOriginFetcher {
    /// Build from a `LedgerConfig` and available credentials.
    ///
    /// Origins are iterated in **canonical order** (the `Origin` `Ord` impl:
    /// `priority, transport, auth.mode, ...`) to ensure deterministic retry
    /// order even for same-priority origins.
    ///
    /// Origins with unsatisfiable auth requirements are skipped and counted
    /// in `skipped_origins` for diagnostics.
    pub fn from_config(config: &LedgerConfig, auth_token: Option<String>) -> Self {
        let has_token = auth_token.is_some();
        let http = reqwest::Client::new();

        // Get origins in canonical (Ord) order, filtered to enabled
        let mut origins: Vec<&Origin> = config.origins.iter().filter(|o| o.enabled).collect();
        origins.sort();

        let mut fetchers = Vec::new();
        let mut skipped = 0usize;

        for origin in origins {
            if is_auth_satisfiable(origin, has_token) {
                fetchers.push(HttpOriginFetcher::new(
                    &origin.transport,
                    auth_token.clone(),
                    http.clone(),
                ));
            } else {
                debug!(
                    transport = %origin.transport,
                    auth_mode = %origin.auth.mode,
                    "skipping origin: auth not satisfiable"
                );
                skipped += 1;
            }
        }

        Self {
            fetchers,
            skipped_origins: skipped,
        }
    }

    /// Build from a single bootstrap URI (for `--origin`).
    pub fn from_bootstrap(uri: &str, auth_token: Option<String>) -> Self {
        let http = reqwest::Client::new();
        Self {
            fetchers: vec![HttpOriginFetcher::new(uri, auth_token, http)],
            skipped_origins: 0,
        }
    }

    /// Whether there are no eligible fetchers.
    pub fn is_empty(&self) -> bool {
        self.fetchers.is_empty()
    }

    /// Number of configured fetchers.
    pub fn len(&self) -> usize {
        self.fetchers.len()
    }

    /// Fetch a CAS object by CID with priority fallback and integrity verification.
    ///
    /// Tries each origin in order:
    /// - On **HTTP success**: verifies integrity. If verification **fails**,
    ///   returns `IntegrityFailed` immediately (terminal — corrupt/malicious
    ///   data must not be silently skipped).
    /// - On **HTTP error** (404, network, auth): records the error and tries
    ///   the next origin.
    /// - If all origins are exhausted, returns `FetchFailed` with per-origin
    ///   diagnostics.
    pub async fn fetch(&self, id: &ContentId, ledger: &str) -> Result<Vec<u8>> {
        let cid_str = id.to_string();

        if self.fetchers.is_empty() {
            return Err(SyncError::FetchFailed {
                cid: cid_str,
                details: format!(
                    "no eligible origins (skipped {} due to unsatisfied auth)",
                    self.skipped_origins
                ),
            });
        }

        let mut errors = Vec::new();

        for fetcher in &self.fetchers {
            match fetcher.fetch_object(id, ledger).await {
                Ok(bytes) => {
                    // Integrity check — terminal on failure
                    if !verify_object_integrity(id, &bytes) {
                        return Err(SyncError::IntegrityFailed(cid_str));
                    }
                    return Ok(bytes);
                }
                Err(e) => {
                    debug!(
                        origin = %fetcher.base_url,
                        error = %e,
                        "origin fetch failed, trying next"
                    );
                    errors.push(format!("{}: {}", fetcher.base_url, e));
                }
            }
        }

        // All origins exhausted
        let mut details = errors.join("; ");
        if self.skipped_origins > 0 {
            details.push_str(&format!(
                "; skipped {} origins due to unsatisfied auth",
                self.skipped_origins
            ));
        }

        Err(SyncError::FetchFailed {
            cid: cid_str,
            details,
        })
    }

    /// Fetch the nameservice record for a ledger alias, trying origins in order.
    ///
    /// Returns `Ok(Some(record))` on the first successful response.
    /// Returns `Ok(None)` only when **every** origin returns 404 (ledger not found).
    /// If any origin returns a non-404 error (401, 500, network failure, etc.),
    /// the overall result is `Err(FetchFailed)` — this surfaces misconfigurations
    /// or partial outages rather than silently treating them as "not found".
    pub async fn fetch_ns_record(&self, alias: &str) -> Result<Option<NsRecord>> {
        if self.fetchers.is_empty() {
            return Err(SyncError::FetchFailed {
                cid: format!("ns:{alias}"),
                details: format!(
                    "no eligible origins (skipped {} due to unsatisfied auth)",
                    self.skipped_origins
                ),
            });
        }

        let mut errors = Vec::new();
        let mut all_not_found = true;

        for fetcher in &self.fetchers {
            match fetcher.fetch_ns_record(alias).await {
                Ok(Some(record)) => return Ok(Some(record)),
                Ok(None) => {
                    // 404 — ledger not found on this origin, try next
                    debug!(
                        origin = %fetcher.base_url,
                        alias = %alias,
                        "NsRecord not found on origin, trying next"
                    );
                }
                Err(e) => {
                    all_not_found = false;
                    warn!(
                        origin = %fetcher.base_url,
                        error = %e,
                        "NsRecord fetch failed, trying next"
                    );
                    errors.push(format!("{}: {}", fetcher.base_url, e));
                }
            }
        }

        if all_not_found {
            Ok(None)
        } else {
            Err(SyncError::FetchFailed {
                cid: format!("ns:{alias}"),
                details: errors.join("; "),
            })
        }
    }

    /// Attempt to fetch a pack stream, trying origins in order.
    ///
    /// Returns `Ok(Some(response))` on the first origin that returns 200.
    /// Returns `Ok(None)` when **every** origin returns 404/405/406/501 (pack not supported).
    /// If any origin returns a non-404/405 error, the overall result is
    /// `Err(FetchFailed)`.
    pub async fn fetch_pack_response(
        &self,
        ledger: &str,
        request: &PackRequest,
    ) -> Result<Option<reqwest::Response>> {
        if self.fetchers.is_empty() {
            return Err(SyncError::FetchFailed {
                cid: format!("pack:{ledger}"),
                details: format!(
                    "no eligible origins (skipped {} due to unsatisfied auth)",
                    self.skipped_origins
                ),
            });
        }

        let mut errors = Vec::new();
        let mut all_not_supported = true;

        for fetcher in &self.fetchers {
            match fetcher.fetch_pack_response(ledger, request).await {
                Ok(Some(resp)) => return Ok(Some(resp)),
                Ok(None) => {
                    debug!(
                        origin = %fetcher.base_url,
                        "pack not supported on origin, trying next"
                    );
                }
                Err(e) => {
                    all_not_supported = false;
                    warn!(
                        origin = %fetcher.base_url,
                        error = %e,
                        "pack request failed, trying next"
                    );
                    errors.push(format!("{}: {}", fetcher.base_url, e));
                }
            }
        }

        if all_not_supported {
            Ok(None)
        } else {
            Err(SyncError::FetchFailed {
                cid: format!("pack:{ledger}"),
                details: errors.join("; "),
            })
        }
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::ContentKind;
    use fluree_db_nameservice::{AuthRequirement, ReplicationDefaults};

    // -----------------------------------------------------------------------
    // URL normalization
    // -----------------------------------------------------------------------

    #[test]
    fn test_url_normalization_bare_host() {
        let http = reqwest::Client::new();
        let f = HttpOriginFetcher::new("http://localhost:8090", None, http);
        assert_eq!(f.base_url(), "http://localhost:8090/fluree");
    }

    #[test]
    fn test_url_normalization_with_fluree_suffix() {
        let http = reqwest::Client::new();
        let f = HttpOriginFetcher::new("http://localhost:8090/v1/fluree", None, http);
        assert_eq!(f.base_url(), "http://localhost:8090/v1/fluree");
    }

    #[test]
    fn test_url_normalization_trailing_slash() {
        let http = reqwest::Client::new();
        let f = HttpOriginFetcher::new("http://localhost:8090/", None, http);
        assert_eq!(f.base_url(), "http://localhost:8090/fluree");
    }

    #[test]
    fn test_url_normalization_already_fluree() {
        let http = reqwest::Client::new();
        let f = HttpOriginFetcher::new("https://cdn.example.com/fluree", None, http);
        assert_eq!(f.base_url(), "https://cdn.example.com/fluree");
    }

    // -----------------------------------------------------------------------
    // Auth satisfiability
    // -----------------------------------------------------------------------

    #[test]
    fn test_auth_none_always_satisfiable() {
        let origin = Origin::http(10, "https://example.com");
        assert!(is_auth_satisfiable(&origin, false));
        assert!(is_auth_satisfiable(&origin, true));
    }

    #[test]
    fn test_auth_bearer_requires_token() {
        let origin = Origin::http_bearer(10, "https://example.com", None);
        assert!(!is_auth_satisfiable(&origin, false));
        assert!(is_auth_satisfiable(&origin, true));
    }

    #[test]
    fn test_auth_unknown_mode_not_satisfiable() {
        let origin = Origin {
            priority: 10,
            enabled: true,
            transport: "https://example.com".to_string(),
            auth: AuthRequirement {
                mode: "oidc-device".to_string(),
                audience: None,
                scopes: Vec::new(),
            },
        };
        assert!(!is_auth_satisfiable(&origin, false));
        assert!(!is_auth_satisfiable(&origin, true));
    }

    // -----------------------------------------------------------------------
    // from_config: filtering and ordering
    // -----------------------------------------------------------------------

    fn config_with_origins(origins: Vec<Origin>) -> LedgerConfig {
        LedgerConfig {
            origins,
            replication: ReplicationDefaults::default(),
        }
    }

    #[test]
    fn test_from_config_filters_by_auth() {
        let config = config_with_origins(vec![
            Origin::http(10, "https://a.example.com"), // none — ok
            Origin::http_bearer(20, "https://b.example.com", None), // bearer — needs token
            Origin {
                priority: 30,
                enabled: true,
                transport: "https://c.example.com".to_string(),
                auth: AuthRequirement {
                    mode: "mtls".to_string(),
                    audience: None,
                    scopes: Vec::new(),
                },
            }, // mtls — skip
        ]);

        // Without token: only the "none" origin
        let fetcher = MultiOriginFetcher::from_config(&config, None);
        assert_eq!(fetcher.len(), 1);
        assert_eq!(fetcher.skipped_origins, 2);
        assert_eq!(
            fetcher.fetchers[0].base_url(),
            "https://a.example.com/fluree"
        );

        // With token: "none" + "bearer" origins
        let fetcher = MultiOriginFetcher::from_config(&config, Some("tok".to_string()));
        assert_eq!(fetcher.len(), 2);
        assert_eq!(fetcher.skipped_origins, 1);
    }

    #[test]
    fn test_from_config_canonical_order() {
        // Two origins with same priority — should be sorted by transport
        let config = config_with_origins(vec![
            Origin::http(10, "https://zzz.example.com"),
            Origin::http(10, "https://aaa.example.com"),
        ]);

        let fetcher = MultiOriginFetcher::from_config(&config, None);
        assert_eq!(fetcher.len(), 2);
        assert_eq!(
            fetcher.fetchers[0].base_url(),
            "https://aaa.example.com/fluree"
        );
        assert_eq!(
            fetcher.fetchers[1].base_url(),
            "https://zzz.example.com/fluree"
        );
    }

    #[test]
    fn test_from_config_skips_disabled() {
        let config = config_with_origins(vec![
            Origin {
                priority: 10,
                enabled: false,
                transport: "https://disabled.example.com".to_string(),
                auth: AuthRequirement::default(),
            },
            Origin::http(20, "https://enabled.example.com"),
        ]);

        let fetcher = MultiOriginFetcher::from_config(&config, None);
        assert_eq!(fetcher.len(), 1);
        assert_eq!(
            fetcher.fetchers[0].base_url(),
            "https://enabled.example.com/fluree"
        );
    }

    #[test]
    fn test_from_config_no_eligible_origins() {
        let config =
            config_with_origins(vec![Origin::http_bearer(10, "https://example.com", None)]);

        let fetcher = MultiOriginFetcher::from_config(&config, None);
        assert!(fetcher.is_empty());
        assert_eq!(fetcher.skipped_origins, 1);
    }

    #[test]
    fn test_from_bootstrap_single_origin() {
        let fetcher =
            MultiOriginFetcher::from_bootstrap("http://localhost:8090", Some("tok".to_string()));
        assert_eq!(fetcher.len(), 1);
        assert_eq!(fetcher.skipped_origins, 0);
        assert_eq!(
            fetcher.fetchers[0].base_url(),
            "http://localhost:8090/fluree"
        );
    }

    // -----------------------------------------------------------------------
    // Integrity verification
    // -----------------------------------------------------------------------

    #[test]
    fn test_verify_txn_blob() {
        let data = b"some transaction data";
        let id = ContentId::new(ContentKind::Txn, data);
        assert!(verify_object_integrity(&id, data));
    }

    #[test]
    fn test_verify_config_blob() {
        let data = br#"{"f:origins":[],"f:replication":{"f:preferPack":true,"f:maxPackMiB":64}}"#;
        let id = ContentId::new(ContentKind::LedgerConfig, data);
        assert!(verify_object_integrity(&id, data));
    }

    #[test]
    fn test_verify_tampered_blob() {
        let data = b"original data";
        let id = ContentId::new(ContentKind::Txn, data);
        assert!(!verify_object_integrity(&id, b"tampered data"));
    }

    // -----------------------------------------------------------------------
    // Wiremock integration tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_fetch_success() {
        let server = wiremock::MockServer::start().await;

        let data = b"test txn data";
        let id = ContentId::new(ContentKind::Txn, data);

        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path_regex("/fluree/storage/objects/.*"))
            .respond_with(wiremock::ResponseTemplate::new(200).set_body_bytes(data.to_vec()))
            .mount(&server)
            .await;

        let fetcher = MultiOriginFetcher::from_bootstrap(&server.uri(), None);
        let result = fetcher.fetch(&id, "mydb:main").await.unwrap();
        assert_eq!(result, data);
    }

    #[tokio::test]
    async fn test_fetch_fallback_on_404() {
        let server1 = wiremock::MockServer::start().await;
        let server2 = wiremock::MockServer::start().await;

        let data = b"fallback txn data";
        let id = ContentId::new(ContentKind::Txn, data);

        // First origin: 404
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path_regex("/fluree/storage/objects/.*"))
            .respond_with(wiremock::ResponseTemplate::new(404))
            .mount(&server1)
            .await;

        // Second origin: 200
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path_regex("/fluree/storage/objects/.*"))
            .respond_with(wiremock::ResponseTemplate::new(200).set_body_bytes(data.to_vec()))
            .mount(&server2)
            .await;

        let http = reqwest::Client::new();
        let fetcher = MultiOriginFetcher {
            fetchers: vec![
                HttpOriginFetcher::new(server1.uri(), None, http.clone()),
                HttpOriginFetcher::new(server2.uri(), None, http),
            ],
            skipped_origins: 0,
        };

        let result = fetcher.fetch(&id, "mydb:main").await.unwrap();
        assert_eq!(result, data);
    }

    #[tokio::test]
    async fn test_fetch_all_fail() {
        let server1 = wiremock::MockServer::start().await;
        let server2 = wiremock::MockServer::start().await;

        let data = b"unreachable";
        let id = ContentId::new(ContentKind::Txn, data);

        // Both origins: 500
        for server in [&server1, &server2] {
            wiremock::Mock::given(wiremock::matchers::method("GET"))
                .and(wiremock::matchers::path_regex("/fluree/storage/objects/.*"))
                .respond_with(wiremock::ResponseTemplate::new(500).set_body_string("boom"))
                .mount(server)
                .await;
        }

        let http = reqwest::Client::new();
        let fetcher = MultiOriginFetcher {
            fetchers: vec![
                HttpOriginFetcher::new(server1.uri(), None, http.clone()),
                HttpOriginFetcher::new(server2.uri(), None, http),
            ],
            skipped_origins: 0,
        };

        let err = fetcher.fetch(&id, "mydb:main").await.unwrap_err();
        match err {
            SyncError::FetchFailed { details, .. } => {
                assert!(details.contains("500"));
                assert!(details.contains("boom"));
            }
            other => panic!("expected FetchFailed, got: {other}"),
        }
    }

    #[tokio::test]
    async fn test_fetch_integrity_failure_is_terminal() {
        let server1 = wiremock::MockServer::start().await;
        let server2 = wiremock::MockServer::start().await;

        let real_data = b"real txn data";
        let id = ContentId::new(ContentKind::Txn, real_data);

        // First origin: returns tampered bytes
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path_regex("/fluree/storage/objects/.*"))
            .respond_with(
                wiremock::ResponseTemplate::new(200).set_body_bytes(b"tampered!".to_vec()),
            )
            .mount(&server1)
            .await;

        // Second origin: would return correct bytes (but should never be tried)
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path_regex("/fluree/storage/objects/.*"))
            .respond_with(wiremock::ResponseTemplate::new(200).set_body_bytes(real_data.to_vec()))
            .expect(0) // Must NOT be called
            .mount(&server2)
            .await;

        let http = reqwest::Client::new();
        let fetcher = MultiOriginFetcher {
            fetchers: vec![
                HttpOriginFetcher::new(server1.uri(), None, http.clone()),
                HttpOriginFetcher::new(server2.uri(), None, http),
            ],
            skipped_origins: 0,
        };

        let err = fetcher.fetch(&id, "mydb:main").await.unwrap_err();
        assert!(
            matches!(err, SyncError::IntegrityFailed(_)),
            "expected IntegrityFailed, got: {err}"
        );
    }

    #[tokio::test]
    async fn test_fetch_ns_record() {
        let server = wiremock::MockServer::start().await;

        let ns_json = serde_json::json!({
            "ledger_id": "mydb:main",
            "name": "mydb",
            "branch": "main",
            "commit_head_id": null,
            "commit_t": 42,
            "index_head_id": null,
            "index_t": 40,
            "retracted": false
        });

        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path_regex("/fluree/storage/ns/.*"))
            .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(&ns_json))
            .mount(&server)
            .await;

        let fetcher = MultiOriginFetcher::from_bootstrap(&server.uri(), None);
        let record = fetcher
            .fetch_ns_record("mydb:main")
            .await
            .unwrap()
            .expect("should return Some");

        assert_eq!(record.commit_t, 42);
        assert_eq!(record.branch, "main");
    }

    #[tokio::test]
    async fn test_fetch_ns_record_not_found() {
        let server = wiremock::MockServer::start().await;

        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path_regex("/fluree/storage/ns/.*"))
            .respond_with(wiremock::ResponseTemplate::new(404))
            .mount(&server)
            .await;

        let fetcher = MultiOriginFetcher::from_bootstrap(&server.uri(), None);
        let result = fetcher.fetch_ns_record("nonexistent:main").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_fetch_empty_fetchers() {
        let data = b"anything";
        let id = ContentId::new(ContentKind::Txn, data);

        let fetcher = MultiOriginFetcher {
            fetchers: Vec::new(),
            skipped_origins: 3,
        };

        let err = fetcher.fetch(&id, "mydb:main").await.unwrap_err();
        match err {
            SyncError::FetchFailed { details, .. } => {
                assert!(details.contains("no eligible origins"));
                assert!(details.contains("skipped 3"));
            }
            other => panic!("expected FetchFailed, got: {other}"),
        }
    }
}
