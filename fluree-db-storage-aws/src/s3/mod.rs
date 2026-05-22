//! S3 storage backend implementation
//!
//! Provides `S3Storage` which implements the core `Storage` and `StorageWrite` traits
//! for reading and writing data to Amazon S3.
//!
//! Also implements the extended storage traits from `fluree-db-core`:
//! - `StorageDelete`
//! - `StorageList`
//! - `StorageCas`
//!
//! ## S3 Express One Zone Support
//!
//! S3 Express One Zone (directory buckets) is expected to be supported natively by
//! the AWS SDK (v1.x), which should automatically handle session-based authentication
//! for Express buckets. Use an Express bucket name (format: `bucket-name--zone-id--x-s3`).
//!
//! **Important**: This native Express support should be validated with real AWS
//! integration tests before production use. LocalStack does not fully emulate
//! Express session authentication.
//!
//! ## Timeout Configuration
//!
//! The `timeout_ms` setting bounds each S3 request send and response-body
//! collection. The SDK operation timeout applied to sends includes SDK retry
//! time. For Lambda environments, ensure this value accounts for your function's
//! remaining execution time.

pub mod address;

use crate::error::{AwsStorageError, Result};
use address::{address_to_key, key_to_address, normalize_etag};
use async_trait::async_trait;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use aws_smithy_types::retry::RetryConfig;
use aws_smithy_types::timeout::TimeoutConfig;
use fluree_db_core::error::Error as CoreError;
use fluree_db_core::{
    content_address, sha256_hex, CasAction, CasOutcome, ContentAddressedWrite, ContentKind,
    ContentWriteResult, ListResult as NsListResult, StorageCas, StorageDelete, StorageExtError,
    StorageExtResult, StorageList, StorageRead, StorageWrite,
};
use std::fmt::Debug;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

const DEFAULT_MAX_CONCURRENT_REQUESTS: usize = 32;

/// S3 storage configuration
#[derive(Debug, Clone, Default)]
pub struct S3Config {
    /// S3 bucket name (supports both standard S3 and S3 Express directory buckets)
    pub bucket: String,
    /// Optional key prefix
    pub prefix: Option<String>,
    /// Optional endpoint override (e.g. LocalStack/MinIO, or custom AWS endpoint)
    pub endpoint: Option<String>,
    /// Operation timeout in milliseconds (optional)
    pub timeout_ms: Option<u64>,
    /// Max retries (retries *after* the initial attempt)
    pub max_retries: Option<u32>,
    /// Initial backoff for retries in milliseconds (randomized with jitter by SDK)
    pub retry_base_delay_ms: Option<u64>,
    /// Max backoff for retries in milliseconds
    pub retry_max_delay_ms: Option<u64>,
    /// Maximum concurrent S3 SDK operations per storage instance.
    ///
    /// This bounds in-flight reads/writes/deletes so retry storms cannot create
    /// an unbounded queue inside the SDK HTTP layer.
    pub max_concurrent_requests: Option<usize>,
}

/// S3-based storage backend
///
/// Implements `Storage` and `StorageWrite` traits for Amazon S3.
/// Supports both standard S3 and S3 Express One Zone (directory buckets).
///
/// S3 Express One Zone authentication is handled automatically by the SDK
/// when using directory bucket names (format: `bucket-name--zone-id--x-s3`).
#[derive(Clone)]
pub struct S3Storage {
    /// S3 client (handles both standard and Express buckets)
    client: Client,
    /// S3 bucket name
    bucket: String,
    /// Optional key prefix
    prefix: Option<String>,
    /// Per-request send timeout (from `S3Config::timeout_ms`, or default 35s)
    send_timeout: Duration,
    /// Storage-level bulkhead for all S3 SDK operations from this instance.
    request_semaphore: Arc<Semaphore>,
    max_concurrent_requests: usize,
}

impl Debug for S3Storage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Storage")
            .field("bucket", &self.bucket)
            .field("prefix", &self.prefix)
            .field("is_express", &Self::is_express_bucket(&self.bucket))
            .field("max_concurrent_requests", &self.max_concurrent_requests)
            .finish()
    }
}

impl S3Storage {
    /// Create a new S3 storage backend
    ///
    /// For S3 Express buckets (detected by `--x-s3` suffix), the SDK
    /// automatically handles session-based authentication.
    ///
    /// # Arguments
    ///
    /// * `sdk_config` - AWS SDK configuration (from `aws_config::load_defaults()`)
    /// * `config` - S3-specific configuration (bucket, prefix, timeout)
    ///
    /// # Example
    ///
    /// ```ignore
    /// let sdk_config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    /// let s3_config = S3Config {
    ///     bucket: "my-bucket".to_string(),
    ///     prefix: Some("data".to_string()),
    ///     timeout_ms: Some(30000),
    ///     ..Default::default()
    /// };
    /// let storage = S3Storage::new(&sdk_config, s3_config).await?;
    /// ```
    pub async fn new(sdk_config: &aws_config::SdkConfig, config: S3Config) -> Result<Self> {
        // Verify region is configured
        if sdk_config.region().is_none() {
            return Err(AwsStorageError::MissingRegion);
        }

        // Build S3 config by inheriting from SdkConfig (preserves HTTP client, retry config,
        // endpoints, sleep impl, etc.) then apply our overrides
        let mut s3_config_builder = aws_sdk_s3::config::Builder::from(sdk_config);

        // Apply endpoint override if configured (e.g. LocalStack/MinIO)
        if let Some(endpoint) = &config.endpoint {
            s3_config_builder = s3_config_builder.endpoint_url(endpoint);
        }

        // Apply retry overrides
        if config.max_retries.is_some()
            || config.retry_base_delay_ms.is_some()
            || config.retry_max_delay_ms.is_some()
        {
            // AWS SDK uses "max attempts" = initial attempt + retries
            let max_attempts = config.max_retries.unwrap_or(0).saturating_add(1).max(1);

            let mut retry_config = RetryConfig::standard().with_max_attempts(max_attempts);

            if let Some(ms) = config.retry_base_delay_ms {
                retry_config = retry_config.with_initial_backoff(Duration::from_millis(ms));
            }
            if let Some(ms) = config.retry_max_delay_ms {
                retry_config = retry_config.with_max_backoff(Duration::from_millis(ms));
            }

            s3_config_builder = s3_config_builder.retry_config(retry_config);
        }

        // Apply timeout if configured
        if let Some(timeout_ms) = config.timeout_ms {
            let timeout_config = TimeoutConfig::builder()
                .operation_timeout(Duration::from_millis(timeout_ms))
                .build();
            s3_config_builder = s3_config_builder.timeout_config(timeout_config);
        }

        let client = Client::from_conf(s3_config_builder.build());

        let send_timeout = config
            .timeout_ms
            .map(Duration::from_millis)
            .unwrap_or(Duration::from_secs(35));
        let max_concurrent_requests = config
            .max_concurrent_requests
            .unwrap_or(DEFAULT_MAX_CONCURRENT_REQUESTS)
            .max(1);

        Ok(Self {
            client,
            bucket: config.bucket,
            prefix: config.prefix,
            send_timeout,
            request_semaphore: Arc::new(Semaphore::new(max_concurrent_requests)),
            max_concurrent_requests,
        })
    }

    /// Create S3Storage from pre-built client (for testing)
    pub fn from_client(client: Client, bucket: String, prefix: Option<String>) -> Self {
        Self {
            client,
            bucket,
            prefix,
            send_timeout: Duration::from_secs(35),
            request_semaphore: Arc::new(Semaphore::new(DEFAULT_MAX_CONCURRENT_REQUESTS)),
            max_concurrent_requests: DEFAULT_MAX_CONCURRENT_REQUESTS,
        }
    }

    /// Detect S3 Express directory bucket by naming convention
    ///
    /// Pattern: `*--{region-az}-az{digit}--x-s3`
    /// Examples: `my-bucket--use1-az1--x-s3`, `foo--apne1-az2--x-s3`
    pub fn is_express_bucket(bucket: &str) -> bool {
        // Must end with --x-s3 and have -az{digit}-- before it
        if !bucket.ends_with("--x-s3") {
            return false;
        }
        // Find the second-to-last "--" delimiter
        let without_suffix = &bucket[..bucket.len() - 6]; // Remove "--x-s3"
        if let Some(pos) = without_suffix.rfind("--") {
            let az_part = &without_suffix[pos + 2..];
            // Must contain "-az" followed by a digit
            if let Some(az_pos) = az_part.find("-az") {
                let after_az = &az_part[az_pos + 3..];
                return after_az
                    .chars()
                    .next()
                    .map(|c| c.is_ascii_digit())
                    .unwrap_or(false);
            }
        }
        false
    }

    /// Check if this storage is using an Express bucket
    pub fn is_express(&self) -> bool {
        Self::is_express_bucket(&self.bucket)
    }

    /// Get the bucket name
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    /// Get the key prefix
    pub fn prefix(&self) -> Option<&str> {
        self.prefix.as_deref()
    }

    async fn acquire_request_permit_core(
        &self,
    ) -> std::result::Result<OwnedSemaphorePermit, CoreError> {
        self.request_semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| CoreError::io("S3 request limiter closed"))
    }

    async fn acquire_request_permit_ext(&self) -> StorageExtResult<OwnedSemaphorePermit> {
        self.request_semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| StorageExtError::io("S3 request limiter closed"))
    }

    /// Convert a Fluree address to an S3 key
    fn to_key(&self, address: &str) -> std::result::Result<String, CoreError> {
        address_to_key(address, self.prefix.as_deref())
            .map_err(|e| CoreError::storage(format!("Invalid address: {e}")))
    }

    /// Convert an S3 key to a Fluree address
    fn to_address(&self, key: &str) -> String {
        key_to_address(key, self.prefix.as_deref())
    }
}

#[async_trait]
impl StorageRead for S3Storage {
    async fn read_bytes(&self, address: &str) -> std::result::Result<Vec<u8>, CoreError> {
        const SLOW_S3_SEND_WARN_MS: u64 = 1_000;
        const SLOW_S3_BODY_WARN_MS: u64 = 5_000;

        let send_timeout = self.send_timeout;
        let key = self.to_key(address)?;
        let permit = self.acquire_request_permit_core().await?;
        let total_started = Instant::now();

        let send_started = Instant::now();
        let request = self.client.get_object().bucket(&self.bucket).key(&key);
        let response = match tokio::time::timeout(send_timeout, request.send()).await {
            Ok(Ok(response)) => response,
            Ok(Err(e)) => return Err(map_s3_error_core(e, &key)),
            Err(_) => {
                let send_elapsed_ms = send_started.elapsed().as_millis() as u64;
                tracing::error!(
                    bucket = self.bucket.as_str(),
                    key = key.as_str(),
                    address,
                    send_elapsed_ms,
                    timeout_ms = send_timeout.as_millis() as u64,
                    is_express = Self::is_express_bucket(&self.bucket),
                    "s3 read_bytes: get_object send timed out"
                );
                return Err(CoreError::io(format!(
                    "S3 GetObject send timed out after {} ms for {}",
                    send_timeout.as_millis(),
                    key
                )));
            }
        };
        let send_elapsed_ms = send_started.elapsed().as_millis() as u64;

        if send_elapsed_ms >= SLOW_S3_SEND_WARN_MS {
            tracing::debug!(
                bucket = self.bucket.as_str(),
                key = key.as_str(),
                address,
                send_elapsed_ms,
                is_express = Self::is_express_bucket(&self.bucket),
                "s3 read_bytes: slow get_object send"
            );
        }

        let body_collect_started = Instant::now();
        let bytes = match tokio::time::timeout(send_timeout, response.body.collect()).await {
            Ok(Ok(body)) => body.into_bytes().to_vec(),
            Ok(Err(e)) => return Err(CoreError::io(format!("Failed to read S3 body: {e}"))),
            Err(_) => {
                let body_collect_elapsed_ms = body_collect_started.elapsed().as_millis() as u64;
                tracing::error!(
                    bucket = self.bucket.as_str(),
                    key = key.as_str(),
                    address,
                    body_collect_elapsed_ms,
                    timeout_ms = send_timeout.as_millis() as u64,
                    is_express = Self::is_express_bucket(&self.bucket),
                    "s3 read_bytes: body collect timed out"
                );
                return Err(CoreError::io(format!(
                    "S3 GetObject body timed out after {} ms for {}",
                    send_timeout.as_millis(),
                    key
                )));
            }
        };
        let body_collect_elapsed_ms = body_collect_started.elapsed().as_millis() as u64;
        let total_elapsed_ms = total_started.elapsed().as_millis() as u64;

        if body_collect_elapsed_ms >= SLOW_S3_BODY_WARN_MS {
            tracing::debug!(
                bucket = self.bucket.as_str(),
                key = key.as_str(),
                address,
                body_bytes = bytes.len(),
                body_collect_elapsed_ms,
                total_elapsed_ms,
                is_express = Self::is_express_bucket(&self.bucket),
                "s3 read_bytes: slow body collect"
            );
        }

        drop(permit);
        Ok(bytes)
    }

    async fn read_byte_range(
        &self,
        address: &str,
        range: std::ops::Range<u64>,
    ) -> std::result::Result<Vec<u8>, CoreError> {
        const SLOW_S3_RANGE_SEND_WARN_MS: u64 = 1_000;
        const SLOW_S3_RANGE_BODY_WARN_MS: u64 = 5_000;

        if range.start >= range.end {
            return Ok(Vec::new());
        }
        let key = self.to_key(address)?;
        let range_header = format!("bytes={}-{}", range.start, range.end - 1);
        let send_timeout = self.send_timeout;
        let permit = self.acquire_request_permit_core().await?;
        let total_started = Instant::now();
        let send_started = Instant::now();

        let request = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(&key)
            .range(range_header.clone());

        let response = match tokio::time::timeout(send_timeout, request.send()).await {
            Ok(Ok(response)) => response,
            Ok(Err(e)) => return Err(map_s3_error_core(e, &key)),
            Err(_) => {
                let send_elapsed_ms = send_started.elapsed().as_millis() as u64;
                tracing::error!(
                    bucket = self.bucket.as_str(),
                    key = key.as_str(),
                    address,
                    range = range_header.as_str(),
                    send_elapsed_ms,
                    timeout_ms = send_timeout.as_millis() as u64,
                    is_express = Self::is_express_bucket(&self.bucket),
                    "s3 read_byte_range: get_object send timed out"
                );
                return Err(CoreError::io(format!(
                    "S3 ranged GetObject timed out after {} ms for {}",
                    send_timeout.as_millis(),
                    key
                )));
            }
        };
        let send_elapsed_ms = send_started.elapsed().as_millis() as u64;

        if send_elapsed_ms >= SLOW_S3_RANGE_SEND_WARN_MS {
            tracing::debug!(
                bucket = self.bucket.as_str(),
                key = key.as_str(),
                address,
                range = range_header.as_str(),
                send_elapsed_ms,
                is_express = Self::is_express_bucket(&self.bucket),
                "s3 read_byte_range: slow get_object send"
            );
        }

        let body_started = Instant::now();
        let bytes = match tokio::time::timeout(send_timeout, response.body.collect()).await {
            Ok(Ok(body)) => body.into_bytes().to_vec(),
            Ok(Err(e)) => return Err(CoreError::io(format!("Failed to read S3 body: {e}"))),
            Err(_) => {
                let body_elapsed_ms = body_started.elapsed().as_millis() as u64;
                tracing::error!(
                    bucket = self.bucket.as_str(),
                    key = key.as_str(),
                    address,
                    range = range_header.as_str(),
                    body_elapsed_ms,
                    timeout_ms = send_timeout.as_millis() as u64,
                    is_express = Self::is_express_bucket(&self.bucket),
                    "s3 read_byte_range: body collect timed out"
                );
                return Err(CoreError::io(format!(
                    "S3 ranged GetObject body timed out after {} ms for {}",
                    send_timeout.as_millis(),
                    key
                )));
            }
        };
        let body_elapsed_ms = body_started.elapsed().as_millis() as u64;
        if body_elapsed_ms >= SLOW_S3_RANGE_BODY_WARN_MS {
            tracing::debug!(
                bucket = self.bucket.as_str(),
                key = key.as_str(),
                address,
                range = range_header.as_str(),
                body_bytes = bytes.len(),
                body_elapsed_ms,
                total_elapsed_ms = total_started.elapsed().as_millis() as u64,
                is_express = Self::is_express_bucket(&self.bucket),
                "s3 read_byte_range: slow body collect"
            );
        }

        drop(permit);
        Ok(bytes)
    }

    async fn exists(&self, address: &str) -> std::result::Result<bool, CoreError> {
        let key = self.to_key(address)?;
        let permit = self.acquire_request_permit_core().await?;

        let request = self.client.head_object().bucket(&self.bucket).key(&key);

        let result = match tokio::time::timeout(self.send_timeout, request.send()).await {
            Ok(result) => result,
            Err(_) => {
                tracing::error!(
                    bucket = self.bucket.as_str(),
                    key = key.as_str(),
                    address,
                    timeout_ms = self.send_timeout.as_millis() as u64,
                    is_express = Self::is_express_bucket(&self.bucket),
                    "s3 exists: head_object timed out"
                );
                return Err(CoreError::io(format!(
                    "S3 HeadObject timed out after {} ms for {}",
                    self.send_timeout.as_millis(),
                    key
                )));
            }
        };
        let exists = match result {
            Ok(_) => Ok(true),
            Err(e) => {
                // Pattern match on SdkError to avoid panic from into_service_error()
                use aws_sdk_s3::error::SdkError;
                match &e {
                    SdkError::ServiceError(service_err) => {
                        // Check HTTP status code for 404
                        if service_err.raw().status().as_u16() == 404 {
                            Ok(false)
                        } else {
                            Err(map_s3_error_core(e, &key))
                        }
                    }
                    // For non-service errors (timeout, dispatch, etc), propagate as storage error
                    _ => Err(map_s3_error_core(e, &key)),
                }
            }
        }?;
        drop(permit);
        Ok(exists)
    }

    async fn list_prefix(&self, prefix: &str) -> std::result::Result<Vec<String>, CoreError> {
        // Delegate to the nameservice trait impl and convert the error
        StorageList::list_prefix(self, prefix)
            .await
            .map_err(ext_error_to_core)
    }

    async fn list_prefix_with_metadata(
        &self,
        prefix: &str,
    ) -> std::result::Result<Vec<fluree_db_core::RemoteObject>, CoreError> {
        let mut objects = Vec::new();
        let mut continuation_token = None;

        let full_prefix = build_list_prefix(prefix, self.prefix.as_deref())
            .map_err(|e| CoreError::io(format!("Invalid list prefix: {e}")))?;

        loop {
            let mut request = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(&full_prefix);

            if let Some(token) = continuation_token.take() {
                request = request.continuation_token(token);
            }

            let permit = self.acquire_request_permit_core().await?;
            let response = match tokio::time::timeout(self.send_timeout, request.send()).await {
                Ok(Ok(response)) => response,
                Ok(Err(e)) => return Err(map_s3_error_core(e, &full_prefix)),
                Err(_) => {
                    tracing::error!(
                        bucket = self.bucket.as_str(),
                        prefix = full_prefix.as_str(),
                        timeout_ms = self.send_timeout.as_millis() as u64,
                        is_express = Self::is_express_bucket(&self.bucket),
                        "s3 list_prefix_with_metadata: list_objects_v2 timed out"
                    );
                    return Err(CoreError::io(format!(
                        "S3 ListObjectsV2 timed out after {} ms for {}",
                        self.send_timeout.as_millis(),
                        full_prefix
                    )));
                }
            };
            drop(permit);

            for object in response.contents() {
                if let Some(key) = object.key() {
                    let size = object.size().unwrap_or(0).max(0) as u64;
                    objects.push(fluree_db_core::RemoteObject {
                        address: self.to_address(key),
                        size_bytes: size,
                    });
                }
            }

            match response.next_continuation_token() {
                Some(token) => continuation_token = Some(token.to_string()),
                None => break,
            }
        }

        Ok(objects)
    }
}

#[async_trait]
impl StorageWrite for S3Storage {
    async fn write_bytes(&self, address: &str, bytes: &[u8]) -> std::result::Result<(), CoreError> {
        const SLOW_S3_PUT_WARN_MS: u64 = 1_000;

        let key = self.to_key(address)?;
        let send_timeout = self.send_timeout;
        let permit = self.acquire_request_permit_core().await?;
        let started = Instant::now();

        let request = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(&key)
            .body(ByteStream::from(bytes.to_vec()));

        match tokio::time::timeout(send_timeout, request.send()).await {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => return Err(map_s3_error_core(e, &key)),
            Err(_) => {
                let elapsed_ms = started.elapsed().as_millis() as u64;
                tracing::error!(
                    bucket = self.bucket.as_str(),
                    key = key.as_str(),
                    address,
                    bytes = bytes.len(),
                    elapsed_ms,
                    timeout_ms = send_timeout.as_millis() as u64,
                    is_express = Self::is_express_bucket(&self.bucket),
                    "s3 write_bytes: put_object timed out"
                );
                return Err(CoreError::io(format!(
                    "S3 PutObject timed out after {} ms for {}",
                    send_timeout.as_millis(),
                    key
                )));
            }
        }

        let elapsed_ms = started.elapsed().as_millis() as u64;
        if elapsed_ms >= SLOW_S3_PUT_WARN_MS {
            tracing::debug!(
                bucket = self.bucket.as_str(),
                key = key.as_str(),
                address,
                bytes = bytes.len(),
                elapsed_ms,
                is_express = Self::is_express_bucket(&self.bucket),
                "s3 write_bytes: slow put_object"
            );
        }

        drop(permit);
        Ok(())
    }

    async fn delete(&self, address: &str) -> std::result::Result<(), CoreError> {
        // Delegate to the nameservice trait impl and convert the error
        StorageDelete::delete(self, address)
            .await
            .map_err(ext_error_to_core)
    }
}

impl fluree_db_core::StorageMethod for S3Storage {
    fn storage_method(&self) -> &str {
        fluree_db_core::STORAGE_METHOD_S3
    }
}

#[async_trait]
impl ContentAddressedWrite for S3Storage {
    async fn content_write_bytes_with_hash(
        &self,
        kind: ContentKind,
        ledger_id: &str,
        content_hash_hex: &str,
        bytes: &[u8],
    ) -> std::result::Result<ContentWriteResult, CoreError> {
        let address = content_address(
            fluree_db_core::STORAGE_METHOD_S3,
            kind,
            ledger_id,
            content_hash_hex,
        );
        self.write_bytes(&address, bytes).await?;
        Ok(ContentWriteResult {
            address,
            content_hash: content_hash_hex.to_string(),
            size_bytes: bytes.len(),
        })
    }

    async fn content_write_bytes(
        &self,
        kind: ContentKind,
        ledger_id: &str,
        bytes: &[u8],
    ) -> std::result::Result<ContentWriteResult, CoreError> {
        let hash_hex = sha256_hex(bytes);
        self.content_write_bytes_with_hash(kind, ledger_id, &hash_hex, bytes)
            .await
    }
}

// Extended storage trait implementations

#[async_trait]
impl StorageDelete for S3Storage {
    async fn delete(&self, address: &str) -> StorageExtResult<()> {
        const SLOW_S3_DELETE_WARN_MS: u64 = 1_000;

        let key = address_to_key(address, self.prefix.as_deref())
            .map_err(|e| StorageExtError::io(format!("Invalid address: {e}")))?;
        let send_timeout = self.send_timeout;
        let permit = self.acquire_request_permit_ext().await?;
        let started = Instant::now();

        let request = self.client.delete_object().bucket(&self.bucket).key(&key);

        match tokio::time::timeout(send_timeout, request.send()).await {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => return Err(map_s3_error_ext(e, &key)),
            Err(_) => {
                let elapsed_ms = started.elapsed().as_millis() as u64;
                tracing::error!(
                    bucket = self.bucket.as_str(),
                    key = key.as_str(),
                    address,
                    elapsed_ms,
                    timeout_ms = send_timeout.as_millis() as u64,
                    is_express = Self::is_express_bucket(&self.bucket),
                    "s3 delete: delete_object timed out"
                );
                return Err(StorageExtError::io(format!(
                    "S3 DeleteObject timed out after {} ms for {}",
                    send_timeout.as_millis(),
                    key
                )));
            }
        }

        let elapsed_ms = started.elapsed().as_millis() as u64;
        if elapsed_ms >= SLOW_S3_DELETE_WARN_MS {
            tracing::debug!(
                bucket = self.bucket.as_str(),
                key = key.as_str(),
                address,
                elapsed_ms,
                is_express = Self::is_express_bucket(&self.bucket),
                "s3 delete: slow delete_object"
            );
        }

        drop(permit);
        Ok(())
    }
}

#[async_trait]
impl StorageList for S3Storage {
    async fn list_prefix(&self, prefix: &str) -> StorageExtResult<Vec<String>> {
        let mut addresses = Vec::new();
        let mut continuation_token = None;

        let full_prefix = build_list_prefix(prefix, self.prefix.as_deref())
            .map_err(|e| StorageExtError::io(format!("Invalid list prefix: {e}")))?;

        loop {
            let mut request = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(&full_prefix);

            if let Some(token) = continuation_token.take() {
                request = request.continuation_token(token);
            }

            let permit = self.acquire_request_permit_ext().await?;
            let response = match tokio::time::timeout(self.send_timeout, request.send()).await {
                Ok(Ok(response)) => response,
                Ok(Err(e)) => return Err(map_s3_error_ext(e, &full_prefix)),
                Err(_) => {
                    tracing::error!(
                        bucket = self.bucket.as_str(),
                        prefix = full_prefix.as_str(),
                        timeout_ms = self.send_timeout.as_millis() as u64,
                        is_express = Self::is_express_bucket(&self.bucket),
                        "s3 list_prefix: list_objects_v2 timed out"
                    );
                    return Err(StorageExtError::io(format!(
                        "S3 ListObjectsV2 timed out after {} ms for {}",
                        self.send_timeout.as_millis(),
                        full_prefix
                    )));
                }
            };
            drop(permit);

            for object in response.contents() {
                if let Some(key) = object.key() {
                    addresses.push(self.to_address(key));
                }
            }

            match response.next_continuation_token() {
                Some(token) => continuation_token = Some(token.to_string()),
                None => break,
            }
        }

        Ok(addresses)
    }

    async fn list_prefix_paginated(
        &self,
        prefix: &str,
        continuation_token: Option<String>,
        max_keys: usize,
    ) -> StorageExtResult<NsListResult> {
        let full_prefix = build_list_prefix(prefix, self.prefix.as_deref())
            .map_err(|e| StorageExtError::io(format!("Invalid list prefix: {e}")))?;

        let mut request = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(&full_prefix)
            .max_keys(max_keys as i32);

        if let Some(token) = continuation_token {
            request = request.continuation_token(token);
        }

        let permit = self.acquire_request_permit_ext().await?;
        let response = match tokio::time::timeout(self.send_timeout, request.send()).await {
            Ok(Ok(response)) => response,
            Ok(Err(e)) => return Err(map_s3_error_ext(e, &full_prefix)),
            Err(_) => {
                tracing::error!(
                    bucket = self.bucket.as_str(),
                    prefix = full_prefix.as_str(),
                    timeout_ms = self.send_timeout.as_millis() as u64,
                    is_express = Self::is_express_bucket(&self.bucket),
                    "s3 list_prefix_paginated: list_objects_v2 timed out"
                );
                return Err(StorageExtError::io(format!(
                    "S3 ListObjectsV2 timed out after {} ms for {}",
                    self.send_timeout.as_millis(),
                    full_prefix
                )));
            }
        };
        drop(permit);

        let addresses: Vec<String> = response
            .contents()
            .iter()
            .filter_map(|obj| obj.key().map(|k| self.to_address(k)))
            .collect();

        Ok(NsListResult {
            keys: addresses,
            continuation_token: response
                .next_continuation_token()
                .map(std::string::ToString::to_string),
            is_truncated: response.is_truncated().unwrap_or(false),
        })
    }
}

/// Maximum number of CAS retries for S3 optimistic concurrency.
const MAX_S3_CAS_RETRIES: u32 = 5;

impl S3Storage {
    /// S3 put with `If-None-Match: *` (create-if-absent).
    async fn put_if_absent(&self, key: &str, bytes: &[u8]) -> StorageExtResult<bool> {
        let permit = self.acquire_request_permit_ext().await?;
        let request = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(ByteStream::from(bytes.to_vec()))
            .if_none_match("*");

        let result = match tokio::time::timeout(self.send_timeout, request.send()).await {
            Ok(result) => result,
            Err(_) => {
                tracing::error!(
                    bucket = self.bucket.as_str(),
                    key,
                    bytes = bytes.len(),
                    timeout_ms = self.send_timeout.as_millis() as u64,
                    is_express = Self::is_express_bucket(&self.bucket),
                    "s3 cas insert: put_object timed out"
                );
                return Err(StorageExtError::io(format!(
                    "S3 conditional PutObject timed out after {} ms for {}",
                    self.send_timeout.as_millis(),
                    key
                )));
            }
        };
        drop(permit);

        match result {
            Ok(_) => Ok(true),
            Err(e) if is_precondition_failed_sdk(&e) => Ok(false),
            Err(e) => Err(map_s3_error_ext(e, key)),
        }
    }

    /// S3 put with `If-Match: <etag>` (conditional update).
    async fn put_if_match(&self, key: &str, bytes: &[u8], etag: &str) -> StorageExtResult<String> {
        let etag_quoted = if etag.starts_with('"') {
            etag.to_string()
        } else {
            format!("\"{etag}\"")
        };

        let permit = self.acquire_request_permit_ext().await?;
        let request = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(ByteStream::from(bytes.to_vec()))
            .if_match(&etag_quoted);

        let result = match tokio::time::timeout(self.send_timeout, request.send()).await {
            Ok(result) => result,
            Err(_) => {
                tracing::error!(
                    bucket = self.bucket.as_str(),
                    key,
                    bytes = bytes.len(),
                    timeout_ms = self.send_timeout.as_millis() as u64,
                    is_express = Self::is_express_bucket(&self.bucket),
                    "s3 cas update: put_object timed out"
                );
                return Err(StorageExtError::io(format!(
                    "S3 conditional PutObject timed out after {} ms for {}",
                    self.send_timeout.as_millis(),
                    key
                )));
            }
        };
        drop(permit);

        match result {
            Ok(output) => {
                let new_etag = output.e_tag().map(normalize_etag).unwrap_or_default();
                Ok(new_etag)
            }
            Err(e) if is_precondition_failed_sdk(&e) => {
                Err(StorageExtError::PreconditionFailed("ETag mismatch".into()))
            }
            Err(e) => Err(map_s3_error_ext(e, key)),
        }
    }

    /// S3 get with ETag extraction.
    async fn get_with_etag(&self, key: &str) -> StorageExtResult<(Vec<u8>, String)> {
        let permit = self.acquire_request_permit_ext().await?;
        let request = self.client.get_object().bucket(&self.bucket).key(key);

        let response = match tokio::time::timeout(self.send_timeout, request.send()).await {
            Ok(Ok(response)) => response,
            Ok(Err(e)) => return Err(map_s3_error_ext(e, key)),
            Err(_) => {
                tracing::error!(
                    bucket = self.bucket.as_str(),
                    key,
                    timeout_ms = self.send_timeout.as_millis() as u64,
                    is_express = Self::is_express_bucket(&self.bucket),
                    "s3 cas read: get_object timed out"
                );
                return Err(StorageExtError::io(format!(
                    "S3 CAS GetObject timed out after {} ms for {}",
                    self.send_timeout.as_millis(),
                    key
                )));
            }
        };

        let etag = response.e_tag().map(normalize_etag).unwrap_or_default();

        let bytes = match tokio::time::timeout(self.send_timeout, response.body.collect()).await {
            Ok(Ok(body)) => body.into_bytes().to_vec(),
            Ok(Err(e)) => return Err(StorageExtError::io(format!("Failed to read S3 body: {e}"))),
            Err(_) => {
                tracing::error!(
                    bucket = self.bucket.as_str(),
                    key,
                    timeout_ms = self.send_timeout.as_millis() as u64,
                    is_express = Self::is_express_bucket(&self.bucket),
                    "s3 cas read: body collect timed out"
                );
                return Err(StorageExtError::io(format!(
                    "S3 CAS GetObject body timed out after {} ms for {}",
                    self.send_timeout.as_millis(),
                    key
                )));
            }
        };

        drop(permit);
        Ok((bytes, etag))
    }
}

#[async_trait]
impl StorageCas for S3Storage {
    async fn insert(&self, address: &str, bytes: &[u8]) -> StorageExtResult<bool> {
        let key = address_to_key(address, self.prefix.as_deref())
            .map_err(|e| StorageExtError::io(format!("Invalid address: {e}")))?;
        self.put_if_absent(&key, bytes).await
    }

    async fn compare_and_swap<T, F>(&self, address: &str, f: F) -> StorageExtResult<CasOutcome<T>>
    where
        F: Fn(Option<&[u8]>) -> std::result::Result<CasAction<T>, StorageExtError> + Send + Sync,
        T: Send,
    {
        let key = address_to_key(address, self.prefix.as_deref())
            .map_err(|e| StorageExtError::io(format!("Invalid address: {e}")))?;

        for attempt in 0..MAX_S3_CAS_RETRIES {
            // Read current value with ETag
            let current = match self.get_with_etag(&key).await {
                Ok((bytes, etag)) => Some((bytes, etag)),
                Err(StorageExtError::NotFound(_)) => None,
                Err(e) => return Err(e),
            };

            // Call the closure
            let current_bytes = current.as_ref().map(|(b, _)| b.as_slice());
            match f(current_bytes)? {
                CasAction::Abort(t) => return Ok(CasOutcome::Aborted(t)),
                CasAction::Write(new_bytes) => {
                    // Write with appropriate condition
                    let write_result = match &current {
                        Some((_, etag)) => self.put_if_match(&key, &new_bytes, etag).await,
                        None => match self.put_if_absent(&key, &new_bytes).await {
                            Ok(true) => Ok(String::new()),
                            Ok(false) => Err(StorageExtError::PreconditionFailed(
                                "concurrent insert".into(),
                            )),
                            Err(e) => Err(e),
                        },
                    };

                    match write_result {
                        Ok(_) => return Ok(CasOutcome::Written),
                        Err(StorageExtError::PreconditionFailed(_)) => {
                            // Concurrent modification — retry with backoff
                            if attempt + 1 < MAX_S3_CAS_RETRIES {
                                let jitter = rand::random::<u64>() % 50;
                                let delay = Duration::from_millis(50 * (1u64 << attempt) + jitter);
                                tokio::time::sleep(delay).await;
                            }
                        }
                        Err(e) => return Err(e),
                    }
                }
            }
        }

        Err(StorageExtError::io(format!(
            "CAS update failed after {MAX_S3_CAS_RETRIES} retries for {address}"
        )))
    }
}

/// Result of a list operation (re-export for convenience)
pub type ListResult = NsListResult;

/// Build the full S3 key prefix for a list operation.
///
/// Mirrors the address parsing used by reads/writes/deletes so callers can
/// pass a fluree address (`fluree:s3://ledger/main/`) or a raw path
/// (`ledger/main/`) and have the configured bucket prefix correctly prepended.
/// An empty `prefix` lists everything under the bucket prefix (or the whole
/// bucket if no prefix is configured).
fn build_list_prefix(prefix: &str, bucket_prefix: Option<&str>) -> Result<String> {
    let has_trailing_slash = prefix.ends_with('/');
    let path = if prefix.is_empty() {
        ""
    } else {
        address::parse_fluree_address(prefix)?
    };

    let with_trailing = if has_trailing_slash && !path.ends_with('/') && !path.is_empty() {
        format!("{path}/")
    } else {
        path.to_string()
    };

    Ok(match bucket_prefix {
        Some(bp) => {
            let bp = bp.trim_end_matches('/');
            if with_trailing.is_empty() {
                format!("{bp}/")
            } else {
                format!("{bp}/{with_trailing}")
            }
        }
        None => with_trailing,
    })
}

// Error mapping helpers

/// Map an SDK error to CoreError, properly handling 404 as NotFound
fn map_s3_error_core<E: std::fmt::Debug>(
    err: aws_sdk_s3::error::SdkError<E>,
    key: &str,
) -> CoreError {
    use aws_sdk_s3::error::SdkError;

    match &err {
        SdkError::ServiceError(service_err) => {
            let status = service_err.raw().status().as_u16();
            match status {
                404 => CoreError::not_found(format!("Key not found: {key}")),
                403 => CoreError::storage(format!("Access denied for key '{key}': {err:?}")),
                _ => {
                    CoreError::storage(format!("S3 error for key '{key}' (HTTP {status}): {err:?}"))
                }
            }
        }
        SdkError::TimeoutError(_) => CoreError::io(format!("S3 timeout for key '{key}': {err:?}")),
        SdkError::DispatchFailure(_) => {
            CoreError::io(format!("S3 connection error for key '{key}': {err:?}"))
        }
        _ => CoreError::storage(format!("S3 error for key '{key}': {err:?}")),
    }
}

/// Map an SDK error to StorageExtError with proper HTTP status classification
fn map_s3_error_ext<E: std::fmt::Debug>(
    err: aws_sdk_s3::error::SdkError<E>,
    key: &str,
) -> StorageExtError {
    use aws_sdk_s3::error::SdkError;

    match &err {
        SdkError::ServiceError(service_err) => {
            let status = service_err.raw().status().as_u16();
            match status {
                404 => StorageExtError::not_found(format!("Key not found: {key}")),
                401 => StorageExtError::unauthorized(format!("Unauthorized for key: {key}")),
                403 => StorageExtError::forbidden(format!("Access denied for key: {key}")),
                412 => StorageExtError::PreconditionFailed(format!("key: {key}")),
                // Retryable server errors: throttling (429), server errors (500/502/503/504)
                429 | 500 | 502 | 503 | 504 => StorageExtError::throttled(format!(
                    "Retryable error for key '{key}' (HTTP {status})"
                )),
                _ => StorageExtError::io(format!(
                    "S3 error for key '{key}' (HTTP {status}): {err:?}"
                )),
            }
        }
        SdkError::TimeoutError(_) => {
            StorageExtError::io(format!("S3 timeout for key '{key}': {err:?}"))
        }
        SdkError::DispatchFailure(_) => {
            StorageExtError::io(format!("S3 connection error for key '{key}': {err:?}"))
        }
        _ => StorageExtError::io(format!("S3 error for key '{key}': {err:?}")),
    }
}

/// Check if an SDK error is a 412 Precondition Failed response
fn is_precondition_failed_sdk<E: std::fmt::Debug>(err: &aws_sdk_s3::error::SdkError<E>) -> bool {
    use aws_sdk_s3::error::SdkError;

    match err {
        SdkError::ServiceError(service_err) => service_err.raw().status().as_u16() == 412,
        _ => false,
    }
}

/// Convert StorageExtError to CoreError
fn ext_error_to_core(err: StorageExtError) -> CoreError {
    match err {
        StorageExtError::Io(msg) => CoreError::io(msg),
        StorageExtError::NotFound(msg) => CoreError::not_found(msg),
        StorageExtError::Unauthorized(msg) => CoreError::storage(format!("Unauthorized: {msg}")),
        StorageExtError::Forbidden(msg) => CoreError::storage(format!("Forbidden: {msg}")),
        StorageExtError::Throttled(msg) => CoreError::io(format!("Throttled: {msg}")),
        StorageExtError::PreconditionFailed(msg) => {
            CoreError::storage(format!("Precondition failed: {msg}"))
        }
        StorageExtError::Other(msg) => CoreError::other(msg),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_express_bucket() {
        // Valid Express bucket names
        assert!(S3Storage::is_express_bucket("my-bucket--use1-az1--x-s3"));
        assert!(S3Storage::is_express_bucket("my-bucket--usw2-az2--x-s3"));
        assert!(S3Storage::is_express_bucket("test--apne1-az3--x-s3"));

        // Invalid - not Express buckets
        assert!(!S3Storage::is_express_bucket("my-bucket"));
        assert!(!S3Storage::is_express_bucket("my-bucket--x-s3")); // Missing az pattern
        assert!(!S3Storage::is_express_bucket("my-bucket-az1--x-s3")); // Missing --

        // Edge cases
        assert!(!S3Storage::is_express_bucket(""));
        assert!(!S3Storage::is_express_bucket("--x-s3"));
    }

    #[test]
    fn test_s3_config_default() {
        let config = S3Config::default();
        assert!(config.bucket.is_empty());
        assert!(config.prefix.is_none());
        assert!(config.endpoint.is_none());
        assert!(config.timeout_ms.is_none());
    }

    #[test]
    fn test_build_list_prefix_strips_fluree_scheme() {
        assert_eq!(
            build_list_prefix("fluree:s3://ledger/main/", Some("fluree-data")).unwrap(),
            "fluree-data/ledger/main/"
        );
    }

    #[test]
    fn test_build_list_prefix_no_bucket_prefix() {
        assert_eq!(
            build_list_prefix("fluree:s3://ledger/main/commit/", None).unwrap(),
            "ledger/main/commit/"
        );
    }

    #[test]
    fn test_build_list_prefix_raw_path() {
        // Callers that already stripped the scheme should still work.
        assert_eq!(
            build_list_prefix("ledger/main/commit/", Some("fluree-data")).unwrap(),
            "fluree-data/ledger/main/commit/"
        );
    }

    #[test]
    fn test_build_list_prefix_preserves_trailing_slash() {
        assert_eq!(
            build_list_prefix("fluree:s3://ledger/main", Some("fluree-data")).unwrap(),
            "fluree-data/ledger/main"
        );
        assert_eq!(
            build_list_prefix("fluree:s3://ledger/main/", Some("fluree-data")).unwrap(),
            "fluree-data/ledger/main/"
        );
    }

    #[test]
    fn test_build_list_prefix_empty_lists_bucket_root() {
        assert_eq!(
            build_list_prefix("", Some("fluree-data")).unwrap(),
            "fluree-data/"
        );
        assert_eq!(build_list_prefix("", None).unwrap(), "");
    }

    #[test]
    fn test_build_list_prefix_trims_bucket_prefix_trailing_slash() {
        assert_eq!(
            build_list_prefix("fluree:s3://ledger/main/", Some("fluree-data/")).unwrap(),
            "fluree-data/ledger/main/"
        );
    }
}
