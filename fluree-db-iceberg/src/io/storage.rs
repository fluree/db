//! Storage abstraction for reading Iceberg files.
//!
//! This module provides the `IcebergStorage` trait for reading files from various
//! backends (S3, local filesystem, etc.) and the `S3IcebergStorage` implementation
//! for reading from S3 using vended credentials.
//!
//! # Design
//!
//! - **`?Send` trait pattern**: Core trait does not require `Send` for WASM compatibility
//! - **`Send` wrapper**: Separate `SendIcebergStorage` trait for AWS SDK integration
//! - **Bounded concurrency**: Optional parallel range reads with configurable limit

use async_trait::async_trait;
use bytes::Bytes;
use std::fmt::Debug;
use std::ops::Range;

use crate::error::{IcebergError, Result};

/// Storage trait for reading Iceberg files.
///
/// This trait is runtime-agnostic and does not require `Send + Sync` at the trait level.
/// For server-side usage with tokio::spawn, use `SendIcebergStorage` instead.
#[async_trait(?Send)]
pub trait IcebergStorage: Debug {
    /// Read an entire file.
    async fn read(&self, path: &str) -> Result<Bytes>;

    /// Read a byte range from a file.
    ///
    /// This is the key API for range-read Parquet access - used to read
    /// the footer and individual column chunks without downloading the whole file.
    async fn read_range(&self, path: &str, range: Range<u64>) -> Result<Bytes>;

    /// Get the size of a file in bytes.
    ///
    /// Used to determine where to read the Parquet footer from (last 8 bytes,
    /// then footer_length bytes before that).
    async fn file_size(&self, path: &str) -> Result<u64>;
}

/// Send-safe storage for AWS SDK integration.
///
/// This trait mirrors `IcebergStorage` but requires `Send + Sync` for use with
/// `tokio::spawn` and `Arc<dyn SendIcebergStorage>`.
#[cfg(feature = "aws")]
#[async_trait]
pub trait SendIcebergStorage: Debug + Send + Sync {
    /// Read an entire file.
    async fn read(&self, path: &str) -> Result<Bytes>;

    /// Read a byte range from a file.
    async fn read_range(&self, path: &str, range: Range<u64>) -> Result<Bytes>;

    /// Get the size of a file in bytes.
    async fn file_size(&self, path: &str) -> Result<u64>;
}

/// S3 storage implementation using vended credentials.
///
/// This type is `Clone` because the underlying AWS SDK `Client` uses `Arc`
/// internally, making clones cheap. This is useful for sharing storage
/// across async tasks or passing to blocking contexts.
#[cfg(feature = "aws")]
#[derive(Clone)]
pub struct S3IcebergStorage {
    client: aws_sdk_s3::Client,
    /// Credential expiration time (for is_expired check)
    credentials_expiry: Option<chrono::DateTime<chrono::Utc>>,
    /// Max concurrent range-read requests (default: 4)
    max_concurrent_reads: usize,
}

#[cfg(feature = "aws")]
impl Debug for S3IcebergStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3IcebergStorage")
            .field("credentials_expiry", &self.credentials_expiry)
            .field("max_concurrent_reads", &self.max_concurrent_reads)
            .finish()
    }
}

#[cfg(feature = "aws")]
impl S3IcebergStorage {
    /// Create a new S3 storage from vended credentials.
    pub async fn from_vended_credentials(
        creds: &crate::credential::VendedCredentials,
    ) -> Result<Self> {
        use aws_credential_types::Credentials;

        // Build AWS credentials from vended credentials
        let aws_creds = Credentials::new(
            &creds.access_key_id,
            &creds.secret_access_key,
            creds.session_token.clone(),
            creds.expires_at.map(|dt| {
                std::time::SystemTime::UNIX_EPOCH
                    + std::time::Duration::from_secs(dt.timestamp() as u64)
            }),
            "vended-credentials",
        );

        // Build config with vended credentials
        let mut config_loader = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .credentials_provider(aws_creds);

        // Set region if provided
        if let Some(region) = &creds.region {
            config_loader = config_loader.region(aws_config::Region::new(region.clone()));
        }

        let sdk_config = config_loader.load().await;

        // Build S3 client, optionally with endpoint override
        let mut s3_config = aws_sdk_s3::config::Builder::from(&sdk_config);

        if let Some(endpoint) = &creds.endpoint {
            s3_config = s3_config.endpoint_url(endpoint);
        }

        if creds.path_style {
            s3_config = s3_config.force_path_style(true);
        }

        let client = aws_sdk_s3::Client::from_conf(s3_config.build());

        Ok(Self {
            client,
            credentials_expiry: creds.expires_at,
            max_concurrent_reads: 4, // Default bounded concurrency
        })
    }

    /// Create S3 storage using the default AWS credential chain.
    ///
    /// This uses the standard AWS SDK credential resolution order:
    /// 1. Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    /// 2. Shared credentials file (~/.aws/credentials)
    /// 3. IAM role for EC2/ECS/Lambda
    ///
    /// Use this when vended credentials are not available from the catalog.
    pub async fn from_default_chain(
        region: Option<&str>,
        endpoint: Option<&str>,
        path_style: bool,
    ) -> Result<Self> {
        let mut config_loader = aws_config::defaults(aws_config::BehaviorVersion::latest());

        // Set region if provided, otherwise use SDK default resolution
        if let Some(r) = region {
            config_loader = config_loader.region(aws_config::Region::new(r.to_string()));
        }

        let sdk_config = config_loader.load().await;

        // Build S3 client with optional endpoint override
        let mut s3_config = aws_sdk_s3::config::Builder::from(&sdk_config);

        if let Some(ep) = endpoint {
            s3_config = s3_config.endpoint_url(ep);
        }

        if path_style {
            s3_config = s3_config.force_path_style(true);
        }

        let client = aws_sdk_s3::Client::from_conf(s3_config.build());

        Ok(Self {
            client,
            credentials_expiry: None, // Ambient creds don't have explicit expiry
            max_concurrent_reads: 4,
        })
    }

    /// Create with custom concurrency limit.
    pub fn with_max_concurrent_reads(mut self, max: usize) -> Self {
        self.max_concurrent_reads = max.max(1); // At least 1
        self
    }

    /// Check if credentials have expired.
    pub fn is_credentials_expired(&self) -> bool {
        if let Some(expiry) = self.credentials_expiry {
            chrono::Utc::now() + chrono::Duration::seconds(30) >= expiry
        } else {
            false
        }
    }

    /// Parse S3 URI into (bucket, key).
    ///
    /// Supports formats:
    /// - `s3://bucket/key/path`
    /// - `s3a://bucket/key/path` (Hadoop style)
    pub fn parse_s3_uri(path: &str) -> Result<(&str, &str)> {
        let path = path
            .strip_prefix("s3://")
            .or_else(|| path.strip_prefix("s3a://"))
            .ok_or_else(|| {
                IcebergError::storage(format!("Invalid S3 URI (must start with s3://): {path}"))
            })?;

        let (bucket, key) = path.split_once('/').ok_or_else(|| {
            IcebergError::storage(format!("Invalid S3 URI (no key path): s3://{path}"))
        })?;

        if bucket.is_empty() {
            return Err(IcebergError::storage("Empty bucket name in S3 URI"));
        }

        Ok((bucket, key))
    }

    /// Read a byte range from S3.
    async fn get_object_range(&self, bucket: &str, key: &str, range: Range<u64>) -> Result<Bytes> {
        let range_header = format!("bytes={}-{}", range.start, range.end.saturating_sub(1));

        let response = self
            .client
            .get_object()
            .bucket(bucket)
            .key(key)
            .range(range_header)
            .send()
            .await
            .map_err(|e| IcebergError::storage(format!("S3 GetObject failed: {e}")))?;

        let body = response
            .body
            .collect()
            .await
            .map_err(|e| IcebergError::storage(format!("Failed to read S3 body: {e}")))?;

        Ok(body.into_bytes())
    }

    /// Read multiple byte ranges concurrently with bounded parallelism.
    pub async fn read_ranges(&self, path: &str, ranges: Vec<Range<u64>>) -> Result<Vec<Bytes>> {
        use futures::stream::{self, StreamExt};
        use std::sync::Arc;
        use tokio::sync::Semaphore;

        let (bucket, key) = Self::parse_s3_uri(path)?;
        let semaphore = Arc::new(Semaphore::new(self.max_concurrent_reads));

        let results: Vec<Result<Bytes>> = stream::iter(ranges)
            .map(|range| {
                let sem = Arc::clone(&semaphore);
                let bucket = bucket.to_string();
                let key = key.to_string();
                async move {
                    let _permit = sem
                        .acquire()
                        .await
                        .map_err(|_| IcebergError::storage("Semaphore closed"))?;
                    self.get_object_range(&bucket, &key, range).await
                }
            })
            .buffer_unordered(self.max_concurrent_reads)
            .collect()
            .await;

        results.into_iter().collect()
    }
}

#[cfg(feature = "aws")]
#[async_trait(?Send)]
impl IcebergStorage for S3IcebergStorage {
    async fn read(&self, path: &str) -> Result<Bytes> {
        let (bucket, key) = Self::parse_s3_uri(path)?;

        let response = self
            .client
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| IcebergError::storage(format!("S3 GetObject failed: {e}")))?;

        let body = response
            .body
            .collect()
            .await
            .map_err(|e| IcebergError::storage(format!("Failed to read S3 body: {e}")))?;

        Ok(body.into_bytes())
    }

    async fn read_range(&self, path: &str, range: Range<u64>) -> Result<Bytes> {
        let (bucket, key) = Self::parse_s3_uri(path)?;
        self.get_object_range(bucket, key, range).await
    }

    async fn file_size(&self, path: &str) -> Result<u64> {
        let (bucket, key) = Self::parse_s3_uri(path)?;

        let response = self
            .client
            .head_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| IcebergError::storage(format!("S3 HeadObject failed: {e}")))?;

        response
            .content_length()
            .map(|l| l as u64)
            .ok_or_else(|| IcebergError::storage("No content-length in HEAD response"))
    }
}

#[cfg(feature = "aws")]
#[async_trait]
impl SendIcebergStorage for S3IcebergStorage {
    async fn read(&self, path: &str) -> Result<Bytes> {
        let (bucket, key) = Self::parse_s3_uri(path)?;

        let response = self
            .client
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| IcebergError::storage(format!("S3 GetObject failed: {e}")))?;

        let body = response
            .body
            .collect()
            .await
            .map_err(|e| IcebergError::storage(format!("Failed to read S3 body: {e}")))?;

        Ok(body.into_bytes())
    }

    async fn read_range(&self, path: &str, range: Range<u64>) -> Result<Bytes> {
        let (bucket, key) = Self::parse_s3_uri(path)?;
        self.get_object_range(bucket, key, range).await
    }

    async fn file_size(&self, path: &str) -> Result<u64> {
        let (bucket, key) = Self::parse_s3_uri(path)?;

        let response = self
            .client
            .head_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| IcebergError::storage(format!("S3 HeadObject failed: {e}")))?;

        response
            .content_length()
            .map(|l| l as u64)
            .ok_or_else(|| IcebergError::storage("No content-length in HEAD response"))
    }
}

/// In-memory storage for testing.
#[derive(Debug, Clone, Default)]
pub struct MemoryStorage {
    files: std::collections::HashMap<String, Bytes>,
}

impl MemoryStorage {
    /// Create a new empty memory storage.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a file to the storage.
    pub fn add_file(&mut self, path: impl Into<String>, content: impl Into<Bytes>) {
        self.files.insert(path.into(), content.into());
    }
}

#[async_trait(?Send)]
impl IcebergStorage for MemoryStorage {
    async fn read(&self, path: &str) -> Result<Bytes> {
        self.files
            .get(path)
            .cloned()
            .ok_or_else(|| IcebergError::storage(format!("File not found: {path}")))
    }

    async fn read_range(&self, path: &str, range: Range<u64>) -> Result<Bytes> {
        let content = self
            .files
            .get(path)
            .ok_or_else(|| IcebergError::storage(format!("File not found: {path}")))?;

        let start = range.start as usize;
        let end = (range.end as usize).min(content.len());

        if start >= content.len() {
            return Ok(Bytes::new());
        }

        Ok(content.slice(start..end))
    }

    async fn file_size(&self, path: &str) -> Result<u64> {
        self.files
            .get(path)
            .map(|c| c.len() as u64)
            .ok_or_else(|| IcebergError::storage(format!("File not found: {path}")))
    }
}

/// A storage wrapper that enforces range-only reads.
///
/// This is used for testing to ensure `ParquetReader` only uses `read_range()`
/// and never falls back to `read()` (whole-file download).
#[derive(Debug)]
pub struct RangeOnlyStorage<S: IcebergStorage> {
    inner: S,
    /// Track number of read() calls (should be 0 for proper range-read usage)
    read_calls: std::sync::atomic::AtomicUsize,
    /// Track number of read_range() calls
    range_read_calls: std::sync::atomic::AtomicUsize,
}

impl<S: IcebergStorage> RangeOnlyStorage<S> {
    /// Create a new range-only wrapper.
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            read_calls: std::sync::atomic::AtomicUsize::new(0),
            range_read_calls: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    /// Get the number of whole-file read() calls.
    pub fn read_calls(&self) -> usize {
        self.read_calls.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Get the number of range-read calls.
    pub fn range_read_calls(&self) -> usize {
        self.range_read_calls
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Assert that no whole-file reads occurred.
    pub fn assert_no_full_reads(&self) {
        let calls = self.read_calls();
        assert_eq!(
            calls, 0,
            "Expected 0 whole-file read() calls, but got {calls}. Range reads should be used instead."
        );
    }
}

#[async_trait(?Send)]
impl<S: IcebergStorage> IcebergStorage for RangeOnlyStorage<S> {
    async fn read(&self, path: &str) -> Result<Bytes> {
        self.read_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        // Still perform the read but track it
        self.inner.read(path).await
    }

    async fn read_range(&self, path: &str, range: Range<u64>) -> Result<Bytes> {
        self.range_read_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.read_range(path, range).await
    }

    async fn file_size(&self, path: &str) -> Result<u64> {
        self.inner.file_size(path).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[cfg(feature = "aws")]
    fn test_parse_s3_uri() {
        // Valid URIs
        let (bucket, key) =
            S3IcebergStorage::parse_s3_uri("s3://my-bucket/path/to/file.parquet").unwrap();
        assert_eq!(bucket, "my-bucket");
        assert_eq!(key, "path/to/file.parquet");

        // S3a prefix (Hadoop style)
        let (bucket, key) = S3IcebergStorage::parse_s3_uri("s3a://bucket/key").unwrap();
        assert_eq!(bucket, "bucket");
        assert_eq!(key, "key");

        // Invalid URIs
        assert!(S3IcebergStorage::parse_s3_uri("http://bucket/key").is_err());
        assert!(S3IcebergStorage::parse_s3_uri("s3://bucket").is_err());
        assert!(S3IcebergStorage::parse_s3_uri("s3:///key").is_err());
    }

    #[tokio::test]
    async fn test_memory_storage() {
        let mut storage = MemoryStorage::new();
        storage.add_file("test.txt", "Hello, World!");

        // Read full file
        let content = storage.read("test.txt").await.unwrap();
        assert_eq!(&content[..], b"Hello, World!");

        // Read range
        let partial = storage.read_range("test.txt", 0..5).await.unwrap();
        assert_eq!(&partial[..], b"Hello");

        let partial = storage.read_range("test.txt", 7..12).await.unwrap();
        assert_eq!(&partial[..], b"World");

        // File size
        let size = storage.file_size("test.txt").await.unwrap();
        assert_eq!(size, 13);

        // Not found
        assert!(storage.read("missing.txt").await.is_err());
    }

    #[tokio::test]
    async fn test_memory_storage_range_beyond_end() {
        let mut storage = MemoryStorage::new();
        storage.add_file("test.txt", "Hello");

        // Range extends past end - should return what's available
        let partial = storage.read_range("test.txt", 3..100).await.unwrap();
        assert_eq!(&partial[..], b"lo");

        // Start beyond end - should return empty
        let empty = storage.read_range("test.txt", 100..200).await.unwrap();
        assert!(empty.is_empty());
    }
}
