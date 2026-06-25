//! Native Google Cloud Storage reader for Iceberg data files (XML API + reqwest).
//!
//! Reading Iceberg data from GCS through the AWS S3 SDK pointed at the GCS
//! S3-interop endpoint fails on HTTP **range** GETs over HTTP/2: the smithy-rs
//! body layer rejects the partial response ("streaming error"), while full GETs
//! succeed. The Parquet reader is range-based, so data-file reads from a
//! GCS-backed Iceberg table fail through the AWS SDK path even though metadata
//! reads work.
//!
//! This backend reads GCS objects with `reqwest` against the Cloud Storage XML
//! API (`{endpoint}/{bucket}/{key}`). The client is pinned to **HTTP/1.1**
//! (`.http1_only()`), so the h2 range issue cannot occur regardless of which
//! reqwest features the workspace enables. Range/partial reads work unchanged
//! over HTTP/1.1 (the `Range` header is an HTTP/1.1 feature) — only the footer
//! and the column chunks a query needs are fetched, never the whole object.
//!
//! Requests are authenticated with **AWS SigV4** using GCS HMAC interop keys —
//! the same credentials the S3-interop path already uses, resolved from the
//! standard AWS credential chain (or catalog-vended credentials in REST mode).
//! No separate token type and no token-expiry handling.

use std::ops::Range;

use async_trait::async_trait;
use bytes::Bytes;

use crate::error::{IcebergError, Result};

/// Default Cloud Storage XML-API endpoint.
#[cfg(feature = "aws")]
const DEFAULT_GCS_ENDPOINT: &str = "https://storage.googleapis.com";

/// Connection-establishment timeout for GCS reads.
#[cfg(feature = "aws")]
const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// Overall per-request timeout for GCS reads (covers large column-chunk ranges).
#[cfg(feature = "aws")]
const REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(300);

/// Returns `true` when a configured S3 endpoint actually points at the GCS
/// S3-interoperability host, i.e. the data lives in Google Cloud Storage.
pub fn is_gcs_endpoint(endpoint: Option<&str>) -> bool {
    endpoint.is_some_and(|e| e.contains("storage.googleapis.com"))
}

/// Reads GCS objects over the Cloud Storage XML API, authenticating each request
/// with AWS SigV4 using GCS HMAC keys. The HTTP client is pinned to HTTP/1.1 to
/// avoid the AWS-SDK HTTP/2 range-read bug against the GCS S3-interop endpoint.
#[cfg(feature = "aws")]
#[derive(Clone)]
pub struct GcsXmlStorage {
    client: reqwest::Client,
    endpoint: String,
    /// SigV4 signing region (the bucket location, e.g. `europe-west1`).
    region: String,
    /// HMAC interop credentials used to sign requests.
    credentials: aws_credential_types::Credentials,
}

#[cfg(feature = "aws")]
impl std::fmt::Debug for GcsXmlStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Deliberately omit `credentials` so the secret access key never lands in
        // logs/traces via `{:?}`.
        f.debug_struct("GcsXmlStorage")
            .field("endpoint", &self.endpoint)
            .field("region", &self.region)
            .finish_non_exhaustive()
    }
}

#[cfg(feature = "aws")]
impl GcsXmlStorage {
    /// Build a reader, resolving credentials and region from the standard AWS
    /// credential chain — environment (`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`),
    /// shared config files, instance/role providers, … — exactly like the S3 path.
    /// For GCS these are the HMAC interoperability keys.
    pub async fn from_default_chain(region: Option<&str>, endpoint: Option<&str>) -> Result<Self> {
        let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest());
        if let Some(r) = region {
            loader = loader.region(aws_config::Region::new(r.to_string()));
        }
        let sdk_config = loader.load().await;

        let resolved_region = sdk_config
            .region()
            .map(|r| r.as_ref().to_string())
            .unwrap_or_else(|| "auto".to_string());

        let provider = sdk_config.credentials_provider().ok_or_else(|| {
            IcebergError::storage(
                "No AWS credentials resolved for GCS SigV4 signing; set AWS_ACCESS_KEY_ID / \
                 AWS_SECRET_ACCESS_KEY to your GCS HMAC interop keys",
            )
        })?;

        use aws_credential_types::provider::ProvideCredentials;
        let credentials = provider.provide_credentials().await.map_err(|e| {
            IcebergError::storage(format!("Failed to resolve credentials for GCS reads: {e}"))
        })?;

        Self::new(
            endpoint.unwrap_or(DEFAULT_GCS_ENDPOINT),
            resolved_region,
            credentials,
        )
    }

    /// Build a reader from catalog-vended credentials (REST mode), so a REST
    /// catalog that vends HMAC keys for a GCS-backed table is honored rather than
    /// bypassed.
    pub fn from_vended_credentials(
        creds: &crate::credential::VendedCredentials,
        endpoint: Option<&str>,
    ) -> Result<Self> {
        let credentials = aws_credential_types::Credentials::new(
            &creds.access_key_id,
            &creds.secret_access_key,
            creds.session_token.clone(),
            creds.expires_at.map(|dt| {
                std::time::SystemTime::UNIX_EPOCH
                    + std::time::Duration::from_secs(dt.timestamp() as u64)
            }),
            "vended-credentials",
        );
        let region = creds.region.clone().unwrap_or_else(|| "auto".to_string());
        let endpoint = creds
            .endpoint
            .as_deref()
            .or(endpoint)
            .unwrap_or(DEFAULT_GCS_ENDPOINT);
        Self::new(endpoint, region, credentials)
    }

    /// Build a reader with an explicit endpoint, signing region, and credentials.
    pub fn new(
        endpoint: &str,
        region: String,
        credentials: aws_credential_types::Credentials,
    ) -> Result<Self> {
        let client = reqwest::Client::builder()
            // Pin HTTP/1.1: range GETs against the GCS S3-interop endpoint trip a
            // partial-response body-streaming bug over HTTP/2. Range reads are an
            // HTTP/1.1 feature and work fine here; concurrency is served by the
            // connection pool rather than h2 multiplexing.
            .http1_only()
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(REQUEST_TIMEOUT)
            .build()
            .map_err(|e| IcebergError::storage(format!("Failed to build HTTP client: {e}")))?;
        Ok(Self {
            client,
            endpoint: endpoint.trim_end_matches('/').to_string(),
            region,
            credentials,
        })
    }

    /// Split a `gs://` / `s3://` / `s3a://` URI into `(bucket, key)`.
    fn split_uri(path: &str) -> Result<(&str, &str)> {
        let rest = path
            .strip_prefix("gs://")
            .or_else(|| path.strip_prefix("s3://"))
            .or_else(|| path.strip_prefix("s3a://"))
            .ok_or_else(|| {
                IcebergError::storage(format!(
                    "Invalid object-store URI (expected gs://, s3:// or s3a://): {path}"
                ))
            })?;
        rest.split_once('/')
            .ok_or_else(|| IcebergError::storage(format!("Object-store URI has no key: {path}")))
    }

    /// Build the path-style XML-API URL for an object.
    fn object_url(&self, path: &str) -> Result<String> {
        let (bucket, key) = Self::split_uri(path)?;
        Ok(format!("{}/{}/{}", self.endpoint, bucket, key))
    }

    /// Build a SigV4-signed `reqwest::Request` for `method url` with an optional
    /// `Range` header.
    ///
    /// The signature is computed over the canonical request (host + `x-amz-date` +
    /// `x-amz-content-sha256` are auto-included by the signer); the `Range` header
    /// is sent unsigned, which S3-style services accept.
    fn signed_request(
        &self,
        method: &str,
        url: &str,
        range: Option<&str>,
    ) -> Result<reqwest::Request> {
        use aws_sigv4::http_request::{
            sign, PayloadChecksumKind, SignableBody, SignableRequest, SigningSettings,
        };
        use aws_sigv4::sign::v4;

        let identity = self.credentials.clone().into();
        let mut settings = SigningSettings::default();
        // S3-style services expect the payload hash header on signed requests.
        settings.payload_checksum_kind = PayloadChecksumKind::XAmzSha256;

        let signing_params = v4::SigningParams::builder()
            .identity(&identity)
            .region(self.region.as_str())
            .name("s3")
            .time(std::time::SystemTime::now())
            .settings(settings)
            .build()
            .map_err(|e| IcebergError::storage(format!("Failed to build SigV4 params: {e}")))?
            .into();

        let signable = SignableRequest::new(
            method,
            url,
            std::iter::empty(), // host is derived from the URI by the signer
            SignableBody::Bytes(&[]),
        )
        .map_err(|e| IcebergError::storage(format!("Failed to build signable request: {e}")))?;

        let (instructions, _signature) = sign(signable, &signing_params)
            .map_err(|e| IcebergError::storage(format!("SigV4 signing failed: {e}")))?
            .into_parts();

        let mut builder = http::Request::builder().method(method).uri(url);
        if let Some(r) = range {
            builder = builder.header(http::header::RANGE, r);
        }
        let mut request = builder
            .body(Vec::<u8>::new())
            .map_err(|e| IcebergError::storage(format!("Failed to build HTTP request: {e}")))?;
        instructions.apply_to_request_http1x(&mut request);

        reqwest::Request::try_from(request)
            .map_err(|e| IcebergError::storage(format!("Failed to build reqwest request: {e}")))
    }

    async fn get_bytes(&self, path: &str, range: Option<Range<u64>>) -> Result<Bytes> {
        let url = self.object_url(path)?;
        let range_header = range.as_ref().map(|r| {
            // HTTP byte ranges are inclusive on both ends; our Range is half-open.
            format!("bytes={}-{}", r.start, r.end.saturating_sub(1))
        });

        let request = self.signed_request("GET", &url, range_header.as_deref())?;
        let resp = self
            .client
            .execute(request)
            .await
            .map_err(|e| IcebergError::storage(format!("GCS GET {url} failed: {e}")))?;

        let status = resp.status();
        if !status.is_success() {
            let body: String = resp
                .text()
                .await
                .unwrap_or_default()
                .chars()
                .take(300)
                .collect();
            return Err(IcebergError::storage(format!(
                "GCS GET {url} -> {status}: {body}"
            )));
        }

        // A range GET must come back `206 Partial Content`. A `200` means the
        // server ignored the `Range` header and returned the whole object — which
        // would feed wrong bytes to the range-based Parquet reader and corrupt the
        // result. Fail loudly instead.
        if range.is_some() && status != reqwest::StatusCode::PARTIAL_CONTENT {
            return Err(IcebergError::storage(format!(
                "GCS GET {url}: requested a byte range but server returned {status} \
                 (expected 206 Partial Content); range read was not honored"
            )));
        }

        resp.bytes()
            .await
            .map_err(|e| IcebergError::storage(format!("Failed to read GCS body for {url}: {e}")))
    }

    async fn head_size(&self, path: &str) -> Result<u64> {
        let url = self.object_url(path)?;
        let request = self.signed_request("HEAD", &url, None)?;
        let resp = self
            .client
            .execute(request)
            .await
            .map_err(|e| IcebergError::storage(format!("GCS HEAD {url} failed: {e}")))?;
        let status = resp.status();
        if !status.is_success() {
            return Err(IcebergError::storage(format!("GCS HEAD {url} -> {status}")));
        }
        resp.headers()
            .get(reqwest::header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok())
            .ok_or_else(|| IcebergError::storage(format!("GCS HEAD {url}: missing Content-Length")))
    }
}

#[cfg(feature = "aws")]
#[async_trait(?Send)]
impl crate::io::IcebergStorage for GcsXmlStorage {
    async fn read(&self, path: &str) -> Result<Bytes> {
        self.get_bytes(path, None).await
    }
    async fn read_range(&self, path: &str, range: Range<u64>) -> Result<Bytes> {
        self.get_bytes(path, Some(range)).await
    }
    async fn file_size(&self, path: &str) -> Result<u64> {
        self.head_size(path).await
    }
}

// Send-safe mirror (reqwest futures are Send), used by the server-side readers.
#[cfg(feature = "aws")]
#[async_trait]
impl crate::io::SendIcebergStorage for GcsXmlStorage {
    async fn read(&self, path: &str) -> Result<Bytes> {
        self.get_bytes(path, None).await
    }
    async fn read_range(&self, path: &str, range: Range<u64>) -> Result<Bytes> {
        self.get_bytes(path, Some(range)).await
    }
    async fn file_size(&self, path: &str) -> Result<u64> {
        self.head_size(path).await
    }
}

/// Storage backend chosen at table-load time: the AWS S3 SDK (S3 or any
/// S3-compatible endpoint) or the native GCS reader for `gs://`-backed tables.
///
/// Dispatches the storage trait so the generic Iceberg readers
/// (`SendParquetReader`, `SendScanPlanner`, `SendDirectCatalogClient`) work over
/// either backend without changing their signatures.
#[cfg(feature = "aws")]
#[derive(Debug, Clone)]
pub enum IcebergBackend {
    /// AWS S3 SDK (also used for S3-interop endpoints).
    S3(crate::io::S3IcebergStorage),
    /// Native GCS reader (reqwest, HTTP/1.1) — avoids the AWS-SDK h2 range bug.
    Gcs(GcsXmlStorage),
}

#[cfg(feature = "aws")]
#[async_trait]
impl crate::io::SendIcebergStorage for IcebergBackend {
    async fn read(&self, path: &str) -> Result<Bytes> {
        match self {
            Self::S3(s) => crate::io::SendIcebergStorage::read(s, path).await,
            Self::Gcs(g) => crate::io::SendIcebergStorage::read(g, path).await,
        }
    }
    async fn read_range(&self, path: &str, range: Range<u64>) -> Result<Bytes> {
        match self {
            Self::S3(s) => crate::io::SendIcebergStorage::read_range(s, path, range).await,
            Self::Gcs(g) => crate::io::SendIcebergStorage::read_range(g, path, range).await,
        }
    }
    async fn file_size(&self, path: &str) -> Result<u64> {
        match self {
            Self::S3(s) => crate::io::SendIcebergStorage::file_size(s, path).await,
            Self::Gcs(g) => crate::io::SendIcebergStorage::file_size(g, path).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_gcs_endpoint() {
        assert!(is_gcs_endpoint(Some("https://storage.googleapis.com")));
        assert!(!is_gcs_endpoint(Some("https://s3.amazonaws.com")));
        assert!(!is_gcs_endpoint(None));
    }

    #[cfg(feature = "aws")]
    fn test_storage() -> GcsXmlStorage {
        let creds = aws_credential_types::Credentials::new("ak", "sk", None, None, "test");
        GcsXmlStorage::new(
            "https://storage.googleapis.com/",
            "europe-west1".to_string(),
            creds,
        )
        .unwrap()
    }

    #[test]
    #[cfg(feature = "aws")]
    fn test_split_uri_handles_gs_s3_s3a() {
        assert_eq!(
            GcsXmlStorage::split_uri("gs://bucket/iceberg/t/metadata/v1.metadata.json").unwrap(),
            ("bucket", "iceberg/t/metadata/v1.metadata.json")
        );
        assert_eq!(GcsXmlStorage::split_uri("s3://b/k").unwrap(), ("b", "k"));
        assert!(GcsXmlStorage::split_uri("https://bucket/k").is_err());
        assert!(GcsXmlStorage::split_uri("gs://bucket-no-key").is_err());
    }

    #[test]
    #[cfg(feature = "aws")]
    fn test_object_url_is_path_style() {
        let s = test_storage();
        assert_eq!(
            s.object_url("gs://bkt/iceberg/t/data/part-0.parquet")
                .unwrap(),
            "https://storage.googleapis.com/bkt/iceberg/t/data/part-0.parquet"
        );
    }

    #[test]
    #[cfg(feature = "aws")]
    fn test_signed_request_sets_sigv4_auth_headers() {
        let s = test_storage();
        let url = s
            .object_url("gs://bkt/iceberg/t/data/part-0.parquet")
            .unwrap();

        // Plain GET: signed headers present, no Range.
        let req = s.signed_request("GET", &url, None).unwrap();
        let auth = req
            .headers()
            .get(reqwest::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default();
        assert!(
            auth.starts_with("AWS4-HMAC-SHA256"),
            "expected a SigV4 Authorization header, got: {auth:?}"
        );
        assert!(req.headers().contains_key("x-amz-date"));
        assert!(req.headers().contains_key("x-amz-content-sha256"));
        assert!(req.headers().get(reqwest::header::RANGE).is_none());

        // Range GET: Range header carried through to the wire.
        let ranged = s.signed_request("GET", &url, Some("bytes=0-127")).unwrap();
        assert_eq!(
            ranged
                .headers()
                .get(reqwest::header::RANGE)
                .and_then(|v| v.to_str().ok()),
            Some("bytes=0-127")
        );
    }
}
