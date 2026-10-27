//! Google Cloud Storage reads over `object_store`'s GCS backend.
//!
//! The only module that touches `object_store`. Delta Kernel pins its version
//! (0.13, with the `gcp` backend already linked into every shipped binary), so
//! keeping it behind this shim makes that pin the only coupling.
//!
//! What this reader will and will not do:
//!
//! - It is built only with [`GoogleCloudStorageBuilder::new`] and an explicit
//!   credential provider, never `from_env`, which would fall back to the host's
//!   application-default credentials or metadata server: a source's storage
//!   identity is decided by its configuration
//!   ([`crate::config::StorageAuth`]), never borrowed from the host.
//! - The bearer token goes only to Google's storage host. The operator may point
//!   every GCS read at another endpoint for tests or private access
//!   ([`GCS_ENDPOINT_ENV`]); no source can.
//! - Tokens come from a [`GoogleIdentity`] (minted for
//!   [`DEVSTORAGE_READ_ONLY_SCOPE`]) or a catalog-vended token that renews
//!   itself ([`RenewingVendedCredentials`]). A request refused with 401 renews
//!   the token and is retried once; a 403 is
//!   [`IcebergError::StorageAccessDenied`].
//! - It serves `gs://` paths only ([`IcebergError::SchemeNotServed`] for any
//!   other), and a ranged read must come back as exactly that range (206 with a
//!   matching `Content-Range`): `object_store` refuses anything else.

use std::collections::HashMap;
use std::future::Future;
use std::ops::Range;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};

use async_trait::async_trait;
use bytes::Bytes;
use object_store::gcp::{GcpCredential, GoogleCloudStorage, GoogleCloudStorageBuilder};
use object_store::path::Path as ObjectPath;
use object_store::{ClientOptions, CredentialProvider, ObjectStore, ObjectStoreExt};

use crate::auth::GoogleIdentity;
use crate::credential::{RenewingVendedCredentials, VendedCredentials};
use crate::error::{IcebergError, Result};
use crate::io::storage::{IcebergStorage, SendIcebergStorage};

/// The OAuth scope storage tokens are minted for: read objects, nothing else.
pub const DEVSTORAGE_READ_ONLY_SCOPE: &str = "https://www.googleapis.com/auth/devstorage.read_only";

/// Operator override for the GCS endpoint every Google-identity read goes to
/// (tests, private service access). Read once; never settable per source.
pub const GCS_ENDPOINT_ENV: &str = "FLUREE_GCS_ENDPOINT";

/// Google's storage endpoint, where tokens go unless the operator overrides it.
pub const GCS_DEFAULT_ENDPOINT: &str = "https://storage.googleapis.com";

const QUOTA_PROJECT_HEADER: &str = "x-goog-user-project";

/// A whole-object GET may be a large data file; `object_store`'s 30 s default
/// covers the body too, which would cut a 256 MiB read on a modest link.
const REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(300);
const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// The endpoint GCS reads go to: [`GCS_ENDPOINT_ENV`] when the operator set
/// it, else Google's.
pub fn gcs_endpoint() -> &'static str {
    static ENDPOINT: OnceLock<String> = OnceLock::new();
    ENDPOINT.get_or_init(|| {
        std::env::var(GCS_ENDPOINT_ENV)
            .ok()
            .map(|e| e.trim().trim_end_matches('/').to_string())
            .filter(|e| !e.is_empty())
            .unwrap_or_else(|| GCS_DEFAULT_ENDPOINT.to_string())
    })
}

/// Where a GCS store's bearer tokens come from.
#[derive(Clone, Debug)]
enum TokenSource {
    /// A Google identity of the source's (or its catalog's) own.
    Identity(Arc<GoogleIdentity>),
    /// A catalog-vended GCS token, renewed from the catalog.
    Vended(Arc<RenewingVendedCredentials>),
}

impl TokenSource {
    async fn bearer(&self) -> Result<String> {
        match self {
            TokenSource::Identity(identity) => Ok(identity
                .access_token(&[DEVSTORAGE_READ_ONLY_SCOPE])
                .await?
                .token),
            TokenSource::Vended(vended) => match vended.current().await? {
                VendedCredentials::Gcs(gcs) => Ok(gcs.token),
                VendedCredentials::S3(_) => Err(IcebergError::Credential(
                    "the catalog vended S3 keys, which cannot read gs:// objects".to_string(),
                )),
            },
        }
    }

    /// The store refused the current token: get a fresh one.
    async fn refresh(&self) -> Result<()> {
        match self {
            TokenSource::Identity(identity) => {
                identity.refresh(&[DEVSTORAGE_READ_ONLY_SCOPE]).await
            }
            TokenSource::Vended(vended) => {
                vended.invalidate();
                vended.current().await.map(|_| ())
            }
        }
    }

    fn label(&self) -> String {
        match self {
            TokenSource::Identity(identity) => format!("Google ({})", identity.principal()),
            TokenSource::Vended(_) => "a catalog-vended GCS token".to_string(),
        }
    }
}

/// `object_store`'s view of a [`TokenSource`]: asked for a credential on every
/// request, which is when a token inside its refresh margin is renewed.
#[derive(Debug)]
struct BearerProvider {
    source: TokenSource,
}

#[async_trait]
impl CredentialProvider for BearerProvider {
    type Credential = GcpCredential;

    async fn get_credential(&self) -> object_store::Result<Arc<GcpCredential>> {
        let bearer = self
            .source
            .bearer()
            .await
            .map_err(|e| object_store::Error::Generic {
                store: "GCS",
                source: Box::new(e),
            })?;
        Ok(Arc::new(GcpCredential { bearer }))
    }
}

/// Reads `gs://` objects as a Google identity or with a vended token.
///
/// One `object_store` client per bucket (its GCS backend is bucket-scoped),
/// built on first use and sharing the token source. `Clone` is an `Arc` bump.
#[derive(Clone)]
pub struct GcsIcebergStorage {
    inner: Arc<Inner>,
}

struct Inner {
    source: TokenSource,
    user_project: Option<String>,
    endpoint: String,
    stores: Mutex<HashMap<String, Arc<GoogleCloudStorage>>>,
}

impl std::fmt::Debug for GcsIcebergStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GcsIcebergStorage")
            .field("source", &self.inner.source)
            .field("user_project", &self.inner.user_project)
            .field("endpoint", &self.inner.endpoint)
            .finish_non_exhaustive()
    }
}

impl GcsIcebergStorage {
    /// Read as `identity`, billing requests to `user_project` when given
    /// (requester pays; sent as `x-goog-user-project`). No network I/O: the
    /// first token is minted on first read.
    pub fn with_identity(
        identity: Arc<GoogleIdentity>,
        user_project: Option<String>,
    ) -> Result<Self> {
        Self::build(
            TokenSource::Identity(identity),
            user_project,
            gcs_endpoint(),
        )
    }

    /// Read with a catalog-vended token that renews itself; `user_project` is
    /// the vended `gcs.user-project`, if any.
    pub fn with_vended(
        credentials: Arc<RenewingVendedCredentials>,
        user_project: Option<String>,
    ) -> Result<Self> {
        Self::build(
            TokenSource::Vended(credentials),
            user_project,
            gcs_endpoint(),
        )
    }

    fn build(source: TokenSource, user_project: Option<String>, endpoint: &str) -> Result<Self> {
        if let Some(project) = &user_project {
            if !crate::auth::google::is_project_id(project) {
                return Err(IcebergError::GoogleCredentialsInvalid(
                    "the storage user project is not a Google Cloud project id".to_string(),
                ));
            }
        }
        Ok(Self {
            inner: Arc::new(Inner {
                source,
                user_project,
                endpoint: endpoint.to_string(),
                stores: Mutex::new(HashMap::new()),
            }),
        })
    }

    /// Read against `endpoint` (a mock) instead of the process's.
    #[cfg(test)]
    fn at_endpoint(source: TokenSource, user_project: Option<String>, endpoint: &str) -> Self {
        Self::build(source, user_project, endpoint).expect("a valid test store")
    }

    /// The `(bucket, object)` a `gs://` path names, or the scheme refusal.
    fn locate<'a>(&self, path: &'a str) -> Result<(&'a str, &'a str)> {
        let rest = path.strip_prefix("gs://").ok_or_else(|| {
            crate::io::location::scheme_not_served(path, &self.inner.source.label(), "gs://")
        })?;
        crate::io::location::split_bucket_key(path, rest)
    }

    /// The object path `object_store` addresses `key` by. Iceberg writes plain
    /// object names; one with an empty or `.`/`..` segment is refused rather
    /// than normalized into the name of another object.
    fn object_path(path: &str, key: &str) -> Result<ObjectPath> {
        ObjectPath::parse(key).map_err(|e| {
            IcebergError::storage(format!("GCS object name in {path} is not readable: {e}"))
        })
    }

    /// The bucket's client, built on first use.
    fn store(&self, bucket: &str) -> Result<Arc<GoogleCloudStorage>> {
        let mut stores = self
            .inner
            .stores
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some(store) = stores.get(bucket) {
            return Ok(Arc::clone(store));
        }
        let mut options = ClientOptions::new()
            .with_timeout(REQUEST_TIMEOUT)
            .with_connect_timeout(CONNECT_TIMEOUT);
        if let Some(project) = &self.inner.user_project {
            let mut headers = object_store::HeaderMap::new();
            let value = object_store::HeaderValue::from_str(project).map_err(|_| {
                IcebergError::GoogleCredentialsInvalid(
                    "the storage user project is not a Google Cloud project id".to_string(),
                )
            })?;
            headers.insert(QUOTA_PROJECT_HEADER, value);
            options = options.with_default_headers(headers);
        }
        let mut builder = GoogleCloudStorageBuilder::new()
            .with_bucket_name(bucket)
            .with_credentials(Arc::new(BearerProvider {
                source: self.inner.source.clone(),
            }));
        if self.inner.endpoint != GCS_DEFAULT_ENDPOINT {
            // Only the operator's override (or a test mock) gets here; plain
            // http is allowed for it alone.
            if self.inner.endpoint.starts_with("http://") {
                options = options.with_allow_http(true);
            }
            builder = builder.with_base_url(&self.inner.endpoint);
        }
        let store =
            Arc::new(builder.with_client_options(options).build().map_err(|e| {
                IcebergError::storage(format!("Failed to build the GCS client: {e}"))
            })?);
        stores.insert(bucket.to_string(), Arc::clone(&store));
        Ok(store)
    }

    /// Run `op`; when GCS refuses the token (401), renew it and run `op` once
    /// more. Every error comes back typed (see [`Self::error`]).
    async fn call<T, F, Fut>(&self, path: &str, bucket: &str, key: &str, op: F) -> Result<T>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = object_store::Result<T>>,
    {
        match op().await {
            Err(e) if refusal_status(&e) == Some(401) => {
                tracing::debug!(
                    path,
                    "GCS refused the token (401); renewing and retrying once"
                );
                self.inner.source.refresh().await?;
                op().await.map_err(|e| self.error(path, bucket, key, e))
            }
            other => other.map_err(|e| self.error(path, bucket, key, e)),
        }
    }

    /// A GCS failure as a typed error: a refusal (403, or a 401 that survived a
    /// renewed token) is [`IcebergError::StorageAccessDenied`]; a token that
    /// could not be minted keeps its own error; anything else is a storage
    /// error naming the path.
    fn error(&self, path: &str, bucket: &str, key: &str, err: object_store::Error) -> IcebergError {
        if refusal_status(&err).is_some() {
            return IcebergError::StorageAccessDenied {
                scheme: "gs",
                bucket: bucket.to_string(),
                key: key.to_string(),
                region: None,
                message: format!(
                    "GCS refused {} reading {path}: {}",
                    self.inner.source.label(),
                    crate::io::storage::error_chain(&err)
                ),
            };
        }
        match err {
            object_store::Error::Generic { source, .. } => {
                match source.downcast::<IcebergError>() {
                    Ok(token_error) => *token_error,
                    Err(source) => IcebergError::storage(format!(
                        "GCS read of {path} failed: {}",
                        crate::io::storage::error_chain(source.as_ref())
                    )),
                }
            }
            object_store::Error::NotFound { .. } => {
                IcebergError::storage(format!("GCS object not found: {path}"))
            }
            other => IcebergError::storage(format!(
                "GCS read of {path} failed: {}",
                crate::io::storage::error_chain(&other)
            )),
        }
    }

    async fn read_impl(&self, path: &str) -> Result<Bytes> {
        let (bucket, key) = self.locate(path)?;
        let location = Self::object_path(path, key)?;
        let store = self.store(bucket)?;
        self.call(path, bucket, key, || async {
            store.get(&location).await?.bytes().await
        })
        .await
    }

    async fn read_range_impl(&self, path: &str, range: Range<u64>) -> Result<Bytes> {
        if range.is_empty() {
            return Ok(Bytes::new());
        }
        let (bucket, key) = self.locate(path)?;
        let location = Self::object_path(path, key)?;
        let store = self.store(bucket)?;
        self.call(path, bucket, key, || {
            store.get_range(&location, range.clone())
        })
        .await
    }

    async fn read_ranges_impl(&self, path: &str, ranges: Vec<Range<u64>>) -> Result<Vec<Bytes>> {
        if !crate::io::storage::parallel_range_gets_enabled() || ranges.iter().any(Range::is_empty)
        {
            let mut out = Vec::with_capacity(ranges.len());
            for range in ranges {
                out.push(self.read_range_impl(path, range).await?);
            }
            return Ok(out);
        }
        let (bucket, key) = self.locate(path)?;
        let location = Self::object_path(path, key)?;
        let store = self.store(bucket)?;
        // `get_ranges` coalesces nearby ranges, fetches them concurrently, and
        // returns the bytes in input order (the sparse assembler's contract).
        self.call(path, bucket, key, || store.get_ranges(&location, &ranges))
            .await
    }

    async fn file_size_impl(&self, path: &str) -> Result<u64> {
        let (bucket, key) = self.locate(path)?;
        let location = Self::object_path(path, key)?;
        let store = self.store(bucket)?;
        let meta = self
            .call(path, bucket, key, || store.head(&location))
            .await?;
        Ok(meta.size)
    }

    /// One delimited listing under `prefix`: `(child directories, files)`,
    /// each by its last segment, sorted.
    async fn list_impl(&self, prefix: &str) -> Result<(Vec<String>, Vec<String>)> {
        let (bucket, key) = match prefix.strip_prefix("gs://") {
            // `gs://bucket` (the whole bucket) has no object part.
            Some(rest) if !rest.contains('/') && !rest.is_empty() => (rest, ""),
            _ => self.locate(prefix)?,
        };
        let key = key.trim_matches('/');
        let location = if key.is_empty() {
            None
        } else {
            Some(Self::object_path(prefix, key)?)
        };
        let store = self.store(bucket)?;
        let listing = self
            .call(prefix, bucket, key, || {
                store.list_with_delimiter(location.as_ref())
            })
            .await?;
        let mut dirs: Vec<String> = listing
            .common_prefixes
            .iter()
            .filter_map(|p| p.filename().map(str::to_string))
            .collect();
        let mut files: Vec<String> = listing
            .objects
            .iter()
            .filter_map(|o| o.location.filename().map(str::to_string))
            .collect();
        dirs.sort();
        files.sort();
        Ok((dirs, files))
    }
}

/// The status of a refusal (401 or 403) `err` reports, if it is one.
///
/// GET and HEAD failures arrive typed (`Unauthenticated`, `PermissionDenied`),
/// but `object_store` wraps a failed LIST in a generic error whose request
/// error type is private, so a LIST refusal is recognized by the status line
/// its request error renders (`Server returned non-2xx status code: 403 …`).
/// The `object_store` version is pinned exactly, and the listing tests pin this.
fn refusal_status(err: &object_store::Error) -> Option<u16> {
    match err {
        object_store::Error::Unauthenticated { .. } => Some(401),
        object_store::Error::PermissionDenied { .. } => Some(403),
        object_store::Error::Generic { source, .. } => {
            let mut next: Option<&(dyn std::error::Error + 'static)> = Some(source.as_ref());
            while let Some(e) = next {
                let rendered = e.to_string();
                for status in [401_u16, 403] {
                    if rendered
                        .starts_with(&format!("Server returned non-2xx status code: {status} "))
                    {
                        return Some(status);
                    }
                }
                next = e.source();
            }
            None
        }
        _ => None,
    }
}

#[async_trait]
impl SendIcebergStorage for GcsIcebergStorage {
    async fn read(&self, path: &str) -> Result<Bytes> {
        self.read_impl(path).await
    }

    async fn read_range(&self, path: &str, range: Range<u64>) -> Result<Bytes> {
        self.read_range_impl(path, range).await
    }

    async fn file_size(&self, path: &str) -> Result<u64> {
        self.file_size_impl(path).await
    }

    async fn read_ranges(&self, path: &str, ranges: Vec<Range<u64>>) -> Result<Vec<Bytes>> {
        self.read_ranges_impl(path, ranges).await
    }

    async fn list_dir(&self, prefix: &str) -> Result<Vec<String>> {
        Ok(self.list_impl(prefix).await?.0)
    }

    async fn list_files(&self, prefix: &str) -> Result<Vec<String>> {
        Ok(self.list_impl(prefix).await?.1)
    }
}

#[async_trait(?Send)]
impl IcebergStorage for GcsIcebergStorage {
    async fn read(&self, path: &str) -> Result<Bytes> {
        self.read_impl(path).await
    }

    async fn read_range(&self, path: &str, range: Range<u64>) -> Result<Bytes> {
        self.read_range_impl(path, range).await
    }

    async fn file_size(&self, path: &str) -> Result<u64> {
        self.file_size_impl(path).await
    }

    async fn read_ranges(&self, path: &str, ranges: Vec<Range<u64>>) -> Result<Vec<Bytes>> {
        self.read_ranges_impl(path, ranges).await
    }

    async fn list_files(&self, prefix: &str) -> Result<Vec<String>> {
        Ok(self.list_impl(prefix).await?.1)
    }
}

#[cfg(test)]
mod tests;
