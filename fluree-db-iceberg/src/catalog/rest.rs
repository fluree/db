//! Polaris-compatible REST catalog client.

use crate::auth::SendCatalogAuth;
use crate::catalog::{
    encode_namespace_for_rest, CatalogClient, LoadTableResponse, TableIdentifier,
};
use crate::credential::VendedCredentials;
use crate::error::{IcebergError, Result};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

/// Configuration for REST catalog client.
#[derive(Debug, Clone)]
pub struct RestCatalogConfig {
    /// Base URI for the REST catalog (e.g., "https://polaris.example.com")
    pub uri: String,
    /// Optional warehouse identifier
    pub warehouse: Option<String>,
    /// Connect timeout in seconds (default: 30)
    pub connect_timeout_secs: u64,
    /// Request timeout in seconds (default: 60)
    pub request_timeout_secs: u64,
}

impl Default for RestCatalogConfig {
    fn default() -> Self {
        Self {
            uri: String::new(),
            warehouse: None,
            connect_timeout_secs: 30,
            request_timeout_secs: 60,
        }
    }
}

/// Polaris-compatible REST catalog client.
///
/// Uses `SendCatalogAuth` for Send-safe futures, enabling use with
/// tokio::spawn and async_trait without ?Send.
pub struct RestCatalogClient {
    pub(crate) config: RestCatalogConfig,
    auth: Arc<dyn SendCatalogAuth>,
    http_client: reqwest::Client,
    /// Process-wide cap on concurrent catalog requests (a shared `Arc`, so every
    /// client built via `new` bounds one global pool). Injectable for tests.
    catalog_semaphore: Arc<tokio::sync::Semaphore>,
}

/// Default cap on concurrent catalog REST requests, process-wide.
const DEFAULT_CATALOG_CONCURRENCY: usize = 8;
/// Default max 429/503 retries before the error is surfaced.
const DEFAULT_CATALOG_MAX_RETRIES: u32 = 4;
/// Base backoff (doubles per attempt, jittered, capped at [`CATALOG_BACKOFF_CAP_MS`]).
const CATALOG_BACKOFF_BASE_MS: u64 = 250;
const CATALOG_BACKOFF_CAP_MS: u64 = 8_000;

/// The process-wide catalog-request semaphore, sized from
/// `FLUREE_ICEBERG_CATALOG_CONCURRENCY` (default 8) at first use. Returns a clone
/// of the shared `Arc` so all clients bound one pool.
fn global_catalog_semaphore() -> Arc<tokio::sync::Semaphore> {
    static SEM: std::sync::OnceLock<Arc<tokio::sync::Semaphore>> = std::sync::OnceLock::new();
    SEM.get_or_init(|| {
        let permits = std::env::var("FLUREE_ICEBERG_CATALOG_CONCURRENCY")
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
            .filter(|&n| n > 0)
            .unwrap_or(DEFAULT_CATALOG_CONCURRENCY);
        Arc::new(tokio::sync::Semaphore::new(permits))
    })
    .clone()
}

fn max_catalog_retries() -> u32 {
    std::env::var("FLUREE_ICEBERG_CATALOG_MAX_RETRIES")
        .ok()
        .and_then(|v| v.trim().parse::<u32>().ok())
        .unwrap_or(DEFAULT_CATALOG_MAX_RETRIES)
}

fn catalog_backoff_base_ms() -> u64 {
    std::env::var("FLUREE_ICEBERG_CATALOG_BACKOFF_BASE_MS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(CATALOG_BACKOFF_BASE_MS)
}

/// A `Retry-After` delta-seconds header as a (capped) `Duration`, if present and
/// parseable. HTTP-date form is not honored (delta-seconds is what Horizon sends).
fn retry_after(response: &reqwest::Response) -> Option<Duration> {
    let secs = response
        .headers()
        .get(reqwest::header::RETRY_AFTER)?
        .to_str()
        .ok()?
        .trim()
        .parse::<u64>()
        .ok()?;
    Some(Duration::from_secs(secs.min(30)))
}

/// Exponential backoff with full jitter for a 0-based `attempt`.
fn backoff_delay(attempt: u32) -> Duration {
    let exp = catalog_backoff_base_ms()
        .saturating_mul(1u64 << attempt.min(5))
        .min(CATALOG_BACKOFF_CAP_MS);
    Duration::from_millis(jitter_ms(exp))
}

/// Uniform pseudo-random in `[0, cap]` from the clock's sub-second bits — full
/// jitter without pulling in a RNG dependency.
fn jitter_ms(cap_ms: u64) -> u64 {
    if cap_ms == 0 {
        return 0;
    }
    let n = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| u64::from(d.subsec_nanos()))
        .unwrap_or(0);
    n % (cap_ms + 1)
}

impl std::fmt::Debug for RestCatalogClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RestCatalogClient")
            .field("uri", &self.config.uri)
            .field("warehouse", &self.config.warehouse)
            .finish()
    }
}

impl RestCatalogClient {
    /// Create a new REST catalog client.
    ///
    /// The HTTP client is SSRF-hardened (see [`crate::net::hardened_client_builder`]):
    /// it follows no redirects and resolves through the guard resolver, so a
    /// catalog request cannot be redirected or rebound to an internal address.
    pub fn new(config: RestCatalogConfig, auth: Arc<dyn SendCatalogAuth>) -> Result<Self> {
        let http_client = crate::net::hardened_client_builder()
            .connect_timeout(Duration::from_secs(config.connect_timeout_secs))
            .timeout(Duration::from_secs(config.request_timeout_secs))
            .build()
            .map_err(|e| IcebergError::Http(format!("Failed to build HTTP client: {e}")))?;

        Ok(Self {
            config,
            auth,
            http_client,
            catalog_semaphore: global_catalog_semaphore(),
        })
    }

    /// Make a GET request to the catalog API.
    ///
    /// Handles authentication headers and 401 retry with token refresh.
    async fn get(&self, path: &str, headers: &[(&str, &str)]) -> Result<serde_json::Value> {
        self.request_with_retry(path, headers, false).await
    }

    /// Internal request method with retry on 401.
    ///
    /// Uses Box::pin for the recursive call to avoid infinite type size.
    /// Returns a Send future for compatibility with tokio::spawn and async_trait.
    fn request_with_retry<'a>(
        &'a self,
        path: &'a str,
        headers: &'a [(&'a str, &'a str)],
        is_retry: bool,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value>> + Send + 'a>>
    {
        Box::pin(async move {
            let url = format!("{}{}", self.config.uri, path);

            // Bound concurrent catalog requests process-wide so a fan-out — the
            // slice-1 prefetch, a wildcard crawl, or a raised scan concurrency —
            // can't storm Horizon. The permit is held across the 429/503 backoff
            // below (a throttled request keeps its slot while it waits — that IS
            // the rate limiting), but released before the 401 recursion so a small
            // cap can't self-deadlock.
            let permit = self.catalog_semaphore.acquire().await;

            let mut attempt: u32 = 0;
            loop {
                let mut request = self
                    .http_client
                    .get(&url)
                    .header("Accept", "application/json");

                // Add auth header if available
                if let Some(auth_header) = self.auth.authorization_header().await? {
                    request = request.header("Authorization", auth_header);
                }

                // Add custom headers
                for (name, value) in headers {
                    request = request.header(*name, *value);
                }

                let response = request.send().await?;
                let status = response.status();

                if status == reqwest::StatusCode::UNAUTHORIZED && !is_retry {
                    // Refresh the token and retry once. Release the permit first —
                    // the recursive call re-acquires it, and holding it across the
                    // recursion would deadlock a single-permit semaphore.
                    tracing::debug!("Got 401, refreshing auth token and retrying");
                    drop(permit);
                    self.auth.refresh().await?;
                    return self.request_with_retry(path, headers, true).await;
                }

                // 429 Too Many Requests / 503 Service Unavailable: honor a
                // `Retry-After` header when present, else exponential backoff with
                // full jitter; bounded attempts, then surface the error.
                if (status == reqwest::StatusCode::TOO_MANY_REQUESTS
                    || status == reqwest::StatusCode::SERVICE_UNAVAILABLE)
                    && attempt < max_catalog_retries()
                {
                    let delay = retry_after(&response).unwrap_or_else(|| backoff_delay(attempt));
                    tracing::debug!(
                        status = %status,
                        attempt,
                        delay_ms = delay.as_millis() as u64,
                        "catalog throttled; backing off"
                    );
                    attempt += 1;
                    tokio::time::sleep(delay).await;
                    continue;
                }

                if status == reqwest::StatusCode::NOT_FOUND {
                    let body = response.text().await.unwrap_or_default();
                    return Err(IcebergError::TableNotFound(format!(
                        "Resource not found at {path}: {body}"
                    )));
                }

                if !status.is_success() {
                    let body = response.text().await.unwrap_or_default();
                    return Err(IcebergError::Catalog(format!(
                        "Catalog request failed ({status}): {body}"
                    )));
                }

                return response
                    .json()
                    .await
                    .map_err(|e| IcebergError::Catalog(format!("Failed to parse response: {e}")));
            }
        })
    }

    /// Build REST API path for a table.
    fn table_path(&self, table_id: &TableIdentifier) -> String {
        let encoded_ns = encode_namespace_for_rest(&table_id.namespace);
        let base = self.api_prefix();
        format!(
            "{}/namespaces/{}/tables/{}",
            base, encoded_ns, table_id.table
        )
    }

    /// Get the API prefix, optionally including the warehouse.
    ///
    /// Standard Iceberg REST: `/v1/namespaces/...`
    /// Polaris with warehouse: `/v1/{warehouse}/namespaces/...`
    fn api_prefix(&self) -> String {
        match &self.config.warehouse {
            Some(warehouse) => format!("/v1/{warehouse}"),
            None => "/v1".to_string(),
        }
    }
}

#[async_trait(?Send)]
impl CatalogClient for RestCatalogClient {
    async fn list_namespaces(&self) -> Result<Vec<String>> {
        let path = format!("{}/namespaces", self.api_prefix());
        let response = self.get(&path, &[]).await?;

        let namespaces = response
            .get("namespaces")
            .and_then(|v| v.as_array())
            .ok_or_else(|| IcebergError::Catalog("Invalid namespaces response".to_string()))?;

        Ok(namespaces
            .iter()
            .filter_map(|v| {
                // Namespaces are arrays of strings representing levels
                v.as_array().map(|parts| {
                    parts
                        .iter()
                        .filter_map(|p| p.as_str())
                        .collect::<Vec<_>>()
                        .join(".")
                })
            })
            .collect())
    }

    async fn list_tables(&self, namespace: &str) -> Result<Vec<String>> {
        let encoded_ns = encode_namespace_for_rest(namespace);
        let path = format!("{}/namespaces/{}/tables", self.api_prefix(), encoded_ns);
        let response = self.get(&path, &[]).await?;

        let identifiers = response
            .get("identifiers")
            .and_then(|v| v.as_array())
            .ok_or_else(|| IcebergError::Catalog("Invalid tables response".to_string()))?;

        Ok(identifiers
            .iter()
            .filter_map(|id| {
                let ns = id
                    .get("namespace")
                    .and_then(|v| v.as_array())
                    .map(|parts| {
                        parts
                            .iter()
                            .filter_map(|p| p.as_str())
                            .collect::<Vec<_>>()
                            .join(".")
                    })?;
                let table = id.get("name").and_then(|v| v.as_str())?;
                Some(format!("{ns}.{table}"))
            })
            .collect())
    }

    async fn load_table(
        &self,
        table_id: &TableIdentifier,
        request_credentials: bool,
    ) -> Result<LoadTableResponse> {
        let path = self.table_path(table_id);

        let headers = if request_credentials {
            vec![("X-Iceberg-Access-Delegation", "vended-credentials")]
        } else {
            vec![]
        };

        let response = self.get(&path, &headers).await?;

        let metadata_location = response
            .get("metadata-location")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                IcebergError::Catalog("Missing metadata-location in response".to_string())
            })?
            .to_string();

        // Extract config map
        let config: HashMap<String, serde_json::Value> =
            if let Some(config_obj) = response.get("config").and_then(|v| v.as_object()) {
                config_obj
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect()
            } else {
                HashMap::new()
            };

        // Parse vended credentials if present
        let credentials = VendedCredentials::from_config_map(&config)?;

        // Retain the inline `metadata` object the REST loadTable response carries
        // (Snowflake Horizon / Polaris include it). This lets metadata preview
        // read the full schema/snapshot with no extra S3 fetch. A present-but-
        // unparseable metadata object is logged and dropped, never fatal — the
        // metadata_location fetch path still works.
        let metadata = response.get("metadata").and_then(|m| {
            match serde_json::from_value::<crate::metadata::TableMetadata>(m.clone()) {
                Ok(md) => Some(md),
                Err(e) => {
                    tracing::debug!(
                        "REST loadTable inline metadata present but failed to parse: {e}"
                    );
                    None
                }
            }
        });

        Ok(LoadTableResponse {
            metadata_location,
            config,
            credentials,
            metadata,
        })
    }
}

#[async_trait]
impl super::SendCatalogClient for RestCatalogClient {
    async fn list_namespaces(&self) -> Result<Vec<String>> {
        let path = format!("{}/namespaces", self.api_prefix());
        let response = self.get(&path, &[]).await?;

        let namespaces = response
            .get("namespaces")
            .and_then(|v| v.as_array())
            .ok_or_else(|| IcebergError::Catalog("Invalid namespaces response".to_string()))?;

        Ok(namespaces
            .iter()
            .filter_map(|v| {
                v.as_array().map(|parts| {
                    parts
                        .iter()
                        .filter_map(|p| p.as_str())
                        .collect::<Vec<_>>()
                        .join(".")
                })
            })
            .collect())
    }

    async fn list_tables(&self, namespace: &str) -> Result<Vec<String>> {
        let encoded_ns = encode_namespace_for_rest(namespace);
        let path = format!("{}/namespaces/{}/tables", self.api_prefix(), encoded_ns);
        let response = self.get(&path, &[]).await?;

        let identifiers = response
            .get("identifiers")
            .and_then(|v| v.as_array())
            .ok_or_else(|| IcebergError::Catalog("Invalid tables response".to_string()))?;

        Ok(identifiers
            .iter()
            .filter_map(|id| {
                let ns = id
                    .get("namespace")
                    .and_then(|v| v.as_array())
                    .map(|parts| {
                        parts
                            .iter()
                            .filter_map(|p| p.as_str())
                            .collect::<Vec<_>>()
                            .join(".")
                    })?;
                let table = id.get("name").and_then(|v| v.as_str())?;
                Some(format!("{ns}.{table}"))
            })
            .collect())
    }

    async fn load_table(
        &self,
        table_id: &TableIdentifier,
        request_credentials: bool,
    ) -> Result<LoadTableResponse> {
        let path = self.table_path(table_id);

        let headers = if request_credentials {
            vec![("X-Iceberg-Access-Delegation", "vended-credentials")]
        } else {
            vec![]
        };

        let response = self.get(&path, &headers).await?;

        let metadata_location = response
            .get("metadata-location")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                IcebergError::Catalog("Missing metadata-location in response".to_string())
            })?
            .to_string();

        let config: HashMap<String, serde_json::Value> =
            if let Some(config_obj) = response.get("config").and_then(|v| v.as_object()) {
                config_obj
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect()
            } else {
                HashMap::new()
            };

        let credentials = VendedCredentials::from_config_map(&config)?;

        let metadata = response.get("metadata").and_then(|m| {
            match serde_json::from_value::<crate::metadata::TableMetadata>(m.clone()) {
                Ok(md) => Some(md),
                Err(e) => {
                    tracing::debug!(
                        "REST loadTable inline metadata present but failed to parse: {e}"
                    );
                    None
                }
            }
        });

        Ok(LoadTableResponse {
            metadata_location,
            config,
            credentials,
            metadata,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_path_single_namespace() {
        let client = RestCatalogClient {
            config: RestCatalogConfig {
                uri: "https://polaris.example.com".to_string(),
                ..Default::default()
            },
            auth: Arc::new(crate::auth::BearerTokenAuth::new("test".to_string())),
            http_client: reqwest::Client::new(),
            catalog_semaphore: global_catalog_semaphore(),
        };

        let table_id = TableIdentifier::new("openflights", "airlines");
        let path = client.table_path(&table_id);
        assert_eq!(path, "/v1/namespaces/openflights/tables/airlines");
    }

    #[test]
    fn test_table_path_multi_level_namespace() {
        let client = RestCatalogClient {
            config: RestCatalogConfig {
                uri: "https://polaris.example.com".to_string(),
                ..Default::default()
            },
            auth: Arc::new(crate::auth::BearerTokenAuth::new("test".to_string())),
            http_client: reqwest::Client::new(),
            catalog_semaphore: global_catalog_semaphore(),
        };

        let table_id = TableIdentifier::new("db.schema", "events");
        let path = client.table_path(&table_id);
        // Multi-level namespace should use unit separator encoding
        assert_eq!(path, "/v1/namespaces/db%1Fschema/tables/events");
    }

    // ---- PR-8 slice 3: 429 backoff + catalog-request semaphore ----

    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Instant;
    use wiremock::matchers::method;
    use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

    /// A test client pointed at a wiremock server. Uses a PLAIN reqwest client so
    /// the loopback mock is reachable (the production `new` hardens against
    /// loopback for SSRF), and injects `sem` as the catalog semaphore.
    fn wiremock_client(uri: &str, sem: Arc<tokio::sync::Semaphore>) -> RestCatalogClient {
        RestCatalogClient {
            config: RestCatalogConfig {
                uri: uri.to_string(),
                ..Default::default()
            },
            auth: Arc::new(crate::auth::NoAuth),
            http_client: reqwest::Client::new(),
            catalog_semaphore: sem,
        }
    }

    #[test]
    fn retry_after_parses_delta_seconds_and_backoff_is_bounded() {
        let resp: reqwest::Response = http::Response::builder()
            .header("Retry-After", "3")
            .body("")
            .map(reqwest::Response::from)
            .unwrap();
        assert_eq!(retry_after(&resp), Some(Duration::from_secs(3)));

        let no_header: reqwest::Response = http::Response::builder()
            .body("")
            .map(reqwest::Response::from)
            .unwrap();
        assert_eq!(retry_after(&no_header), None);

        // Backoff never exceeds the cap, at any attempt.
        for attempt in 0..12 {
            assert!(backoff_delay(attempt) <= Duration::from_millis(CATALOG_BACKOFF_CAP_MS));
        }
    }

    /// Responds 429 (Retry-After: 0) for the first `fail_times` requests, then 200.
    struct FlakyResponder {
        calls: Arc<AtomicUsize>,
        fail_times: usize,
    }
    impl Respond for FlakyResponder {
        fn respond(&self, _: &Request) -> ResponseTemplate {
            let n = self.calls.fetch_add(1, Ordering::SeqCst);
            if n < self.fail_times {
                ResponseTemplate::new(429).insert_header("Retry-After", "0")
            } else {
                ResponseTemplate::new(200).set_body_json(serde_json::json!({"ok": true}))
            }
        }
    }

    #[tokio::test]
    async fn retries_on_429_then_succeeds() {
        let server = MockServer::start().await;
        let calls = Arc::new(AtomicUsize::new(0));
        Mock::given(method("GET"))
            .respond_with(FlakyResponder {
                calls: Arc::clone(&calls),
                fail_times: 2,
            })
            .mount(&server)
            .await;

        let client = wiremock_client(&server.uri(), Arc::new(tokio::sync::Semaphore::new(4)));
        let out = client.get("/v1/namespaces", &[]).await;
        assert!(
            out.is_ok(),
            "should succeed after retrying past the 429s: {out:?}"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 3, "two 429s + one 200");
    }

    #[tokio::test]
    async fn gives_up_after_max_retries_and_surfaces_the_error() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(429).insert_header("Retry-After", "0"))
            .mount(&server)
            .await;

        let client = wiremock_client(&server.uri(), Arc::new(tokio::sync::Semaphore::new(4)));
        let err = client.get("/v1/namespaces", &[]).await.unwrap_err();
        match err {
            IcebergError::Catalog(msg) => assert!(
                msg.contains("429"),
                "terminal error names the status: {msg}"
            ),
            other => panic!("expected a Catalog error after giving up, got {other:?}"),
        }
        // 1 initial attempt + DEFAULT_CATALOG_MAX_RETRIES retries.
        assert_eq!(
            server.received_requests().await.unwrap().len(),
            1 + DEFAULT_CATALOG_MAX_RETRIES as usize
        );
    }

    #[tokio::test]
    async fn semaphore_bounds_concurrent_catalog_requests() {
        // Each response is delayed, so with a 2-permit semaphore, 6 requests run in
        // 3 waves ≈ 3×delay; an unbounded (6-permit) client finishes in ≈1×delay.
        let delay = Duration::from_millis(120);
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_delay(delay)
                    .set_body_json(serde_json::json!({})),
            )
            .mount(&server)
            .await;

        let fire = |permits: usize| {
            let uri = server.uri();
            async move {
                let client = Arc::new(wiremock_client(
                    &uri,
                    Arc::new(tokio::sync::Semaphore::new(permits)),
                ));
                let start = Instant::now();
                let futs = (0..6).map(|_| {
                    let c = Arc::clone(&client);
                    async move { c.get("/v1/namespaces", &[]).await }
                });
                let results = futures::future::join_all(futs).await;
                assert!(results.iter().all(std::result::Result::is_ok));
                start.elapsed()
            }
        };

        let bounded = fire(2).await;
        let unbounded = fire(6).await;
        assert!(
            bounded >= delay * 2,
            "2-permit semaphore should serialize 6 requests into ≥3 waves (got {bounded:?})"
        );
        assert!(
            unbounded < bounded,
            "6-permit (unbounded here) should be faster than 2-permit (bounded {bounded:?} vs unbounded {unbounded:?})"
        );
    }
}
