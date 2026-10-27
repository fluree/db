//! Object stores for Delta tables.
//!
//! Kernel's default engine drives whatever store it is handed, and a Delta log
//! may carry absolute data-file paths. Every store therefore goes through
//! [`ReadOnlyStore`], which refuses writes and, for the local filesystem, keeps
//! each read under the operator's local-root allowlist.

use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;
use delta_kernel::object_store::{
    self as object_store, aws::AmazonS3Builder, azure::MicrosoftAzureBuilder,
    local::LocalFileSystem, path::Path, CopyOptions, GetOptions, GetResult, ListResult,
    MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload,
    PutResult,
};
use futures::stream::{self, BoxStream, StreamExt};
use url::Url;

use fluree_db_iceberg::{HostIdentityKind, HostIdentityPolicy, IcebergError, IdentityContext};

use crate::config::{AzureAuth, DeltaIoConfig, LocationKind};
use crate::error::{DeltaError, Result};
use crate::unity::{UnityClient, UnityTable};

/// Whose credentials read a table.
pub(crate) enum Credentials {
    /// The process's own, or the service principal `io` names.
    Ambient,
    /// Unity Catalog's, issued for this one table.
    Unity(Arc<UnityClient>, UnityTable),
}

/// Parse a table location into the directory URL Kernel expects (trailing
/// slash) and open a store for it, under `identity`'s host-identity policy.
///
/// The host's own credentials are a host identity the operator governs
/// ([`HostIdentityPolicy`]): ambient AWS (`aws`, and its bucket allowlist) for
/// an `s3://` table read with [`Credentials::Ambient`], and ambient Azure
/// (`azure`) for an Azure table with no `io.azure` of its own. A service
/// principal `io.azure` names, and Unity's per-table credentials, are not.
///
/// The bucket checked is the table's: a store is bound to one bucket (or
/// container) here, and Kernel addresses every file it reads, including an
/// absolute data-file URL from the log, as a path within that store
/// (`Path::from_url_path(url.path())`, which drops the URL's host), so no
/// read can leave the bucket that was checked.
pub(crate) fn open(
    location: &str,
    io: &DeltaIoConfig,
    credentials: Credentials,
    identity: &IdentityContext,
) -> Result<(Url, Arc<dyn ObjectStore>)> {
    let bad = |e: &dyn std::fmt::Display| DeltaError::Config(format!("{location}: {e}"));

    // Where a catalog placed the table is the catalog's doing, not the
    // config's: refusing it is about that one table, like a view or a table
    // with an access rule, not a registration error.
    if let Credentials::Unity(_, table) = &credentials {
        let refuse = |message: &str| DeltaError::Catalog {
            table: table.full_name.clone(),
            message: format!("placed at {location}: {message}"),
            denied: false,
        };
        // One that names this host's disk is not followed there, whatever the
        // local-root allowlist permits.
        if fluree_db_iceberg::is_local_location(location) {
            return Err(refuse(
                "a catalog may not place a table on the local filesystem",
            ));
        }
        // Databricks on Google Cloud places tables here; say so rather than
        // list the schemes a path source may use.
        if location.starts_with("gs://") {
            return Err(refuse(
                "Unity Catalog placed this table on Google Cloud Storage, which the Delta \
                 reader does not read yet (Databricks on AWS and Azure is supported)",
            ));
        }
    }
    let kind = crate::config::validate_location(location)?;

    if kind == LocationKind::Azure {
        let url =
            Url::parse(&format!("{}/", location.trim_end_matches('/'))).map_err(|e| bad(&e))?;
        let builder = match (credentials, &io.azure) {
            (Credentials::Unity(unity, table), _) => MicrosoftAzureBuilder::new()
                .with_credentials(crate::unity::Vending::azure(unity, table)),
            (Credentials::Ambient, None) => {
                identity
                    .policy
                    .ensure(HostIdentityKind::Azure)
                    .map_err(DeltaError::Identity)?;
                MicrosoftAzureBuilder::from_env()
            }
            // An unresolved secret reference fails here: `io` must be hydrated.
            (
                Credentials::Ambient,
                Some(AzureAuth::ClientSecret {
                    tenant_id,
                    client_id,
                    client_secret,
                }),
            ) => MicrosoftAzureBuilder::new().with_client_secret_authorization(
                client_id,
                client_secret
                    .resolve()
                    .map_err(|e| bad(&format!("Azure client secret: {e}")))?,
                tenant_id,
            ),
        };
        let store = ReadOnlyStore {
            inner: Arc::new(
                builder
                    .with_url(url.as_str())
                    .build()
                    .map_err(|e| bad(&e))?,
            ),
            local: false,
        };
        return Ok((url, Arc::new(store)));
    }

    if kind == LocationKind::Local {
        let path = fluree_db_iceberg::resolve_local_path(location).map_err(|e| bad(&e))?;
        let url =
            Url::from_directory_path(&path).map_err(|()| bad(&"not an absolute directory path"))?;
        let store = ReadOnlyStore {
            inner: Arc::new(LocalFileSystem::new()),
            local: true,
        };
        return Ok((url, Arc::new(store)));
    }

    // `s3a://` is the Hadoop spelling of the same bucket address.
    let normalized = location
        .strip_prefix("s3a://")
        .map_or_else(|| location.to_string(), |rest| format!("s3://{rest}"));
    let url = Url::parse(&format!("{}/", normalized.trim_end_matches('/'))).map_err(|e| bad(&e))?;
    if let Credentials::Ambient = credentials {
        ensure_ambient_s3(&url, io, &identity.policy)?;
    }
    let mut builder = AmazonS3Builder::from_env().with_url(url.as_str());
    if let Credentials::Unity(unity, table) = credentials {
        builder = builder.with_credentials(crate::unity::Vending::aws(unity, table));
    }
    if let Some(region) = &io.s3_region {
        builder = builder.with_region(region);
    }
    if let Some(endpoint) = &io.s3_endpoint {
        // Checked where the client is built, not only at the HTTP route: a
        // record can also arrive through the Rust API, the CLI or replication.
        fluree_db_iceberg::net::validate_s3_endpoint(endpoint).map_err(|e| bad(&e))?;
        builder = builder
            .with_endpoint(endpoint)
            .with_allow_http(endpoint.starts_with("http://"));
    }
    if io.s3_path_style {
        builder = builder.with_virtual_hosted_style_request(false);
    }
    let store = ReadOnlyStore {
        inner: Arc::new(builder.build().map_err(|e| bad(&e))?),
        local: false,
    };
    Ok((url, Arc::new(store)))
}

/// The host's ambient AWS identity may read the bucket `url` names: the `aws`
/// kind is allowed and, under a bucket allowlist, the bucket is on it and no
/// endpoint override can redirect the read to another bucket.
fn ensure_ambient_s3(url: &Url, io: &DeltaIoConfig, policy: &HostIdentityPolicy) -> Result<()> {
    policy
        .ensure(HostIdentityKind::Aws)
        .map_err(DeltaError::Identity)?;
    if let Some(allowed) = &policy.ambient_aws_buckets {
        if let Some(endpoint) = &io.s3_endpoint {
            return Err(DeltaError::Identity(IcebergError::AmbientEndpointRefused {
                endpoint: endpoint.clone(),
            }));
        }
        let bucket = url.host_str().unwrap_or_default();
        if !allowed.iter().any(|b| b == bucket) {
            return Err(DeltaError::Identity(IcebergError::AmbientBucketRefused {
                bucket: bucket.to_string(),
            }));
        }
    }
    Ok(())
}

#[derive(Debug)]
struct ReadOnlyStore {
    inner: Arc<dyn ObjectStore>,
    local: bool,
}

impl std::fmt::Display for ReadOnlyStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ReadOnly({})", self.inner)
    }
}

impl ReadOnlyStore {
    fn check(&self, location: &Path) -> object_store::Result<()> {
        if !self.local {
            return Ok(());
        }
        fluree_db_iceberg::resolve_local_path(&format!("/{location}"))
            .map(|_| ())
            .map_err(|e| object_store::Error::PermissionDenied {
                path: location.to_string(),
                source: e.to_string().into(),
            })
    }
}

fn read_only(path: &Path) -> object_store::Error {
    object_store::Error::NotSupported {
        source: format!("Delta graph sources are read-only; refused write to {path}").into(),
    }
}

#[async_trait]
impl ObjectStore for ReadOnlyStore {
    async fn put_opts(
        &self,
        location: &Path,
        _payload: PutPayload,
        _opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        Err(read_only(location))
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        _opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        Err(read_only(location))
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.check(location)?;
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> object_store::Result<Vec<bytes::Bytes>> {
        self.check(location)?;
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        locations
            .map(|location| location.and_then(|path| Err(read_only(&path))))
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        match prefix.map(|p| self.check(p)).transpose() {
            Ok(_) => self.inner.list(prefix),
            Err(e) => stream::once(async move { Err(e) }).boxed(),
        }
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        match prefix.map(|p| self.check(p)).transpose() {
            Ok(_) => self.inner.list_with_offset(prefix, offset),
            Err(e) => stream::once(async move { Err(e) }).boxed(),
        }
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        if let Some(prefix) = prefix {
            self.check(prefix)?;
        }
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        _from: &Path,
        to: &Path,
        _options: CopyOptions,
    ) -> object_store::Result<()> {
        Err(read_only(to))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn with_endpoint(endpoint: &str) -> DeltaIoConfig {
        DeltaIoConfig {
            s3_region: Some("us-east-1".to_string()),
            s3_endpoint: Some(endpoint.to_string()),
            ..DeltaIoConfig::default()
        }
    }

    #[test]
    fn an_s3_endpoint_in_the_metadata_range_is_refused() {
        let err = match open(
            "s3://bucket/table",
            &with_endpoint("http://169.254.169.254"),
            Credentials::Ambient,
            &IdentityContext::default(),
        ) {
            Ok(_) => panic!("a metadata-range endpoint must be refused"),
            Err(e) => e.to_string(),
        };
        assert!(err.contains("SSRF guard"), "unexpected error: {err}");
        // MinIO / LocalStack on loopback stay allowed.
        assert!(open(
            "s3://bucket/table",
            &with_endpoint("http://127.0.0.1:4566"),
            Credentials::Ambient,
            &IdentityContext::default(),
        )
        .is_ok());
    }

    fn under(policy: HostIdentityPolicy) -> IdentityContext {
        IdentityContext {
            policy,
            ..IdentityContext::default()
        }
    }

    fn refusal(result: Result<(Url, Arc<dyn ObjectStore>)>) -> IcebergError {
        match result {
            Err(DeltaError::Identity(refusal)) => refusal,
            Err(other) => panic!("expected a host-identity refusal, got {other:?}"),
            Ok(_) => panic!("expected a host-identity refusal, got a store"),
        }
    }

    #[test]
    fn an_ambient_s3_table_is_the_aws_host_identity() {
        let io = DeltaIoConfig {
            s3_region: Some("us-east-1".to_string()),
            ..DeltaIoConfig::default()
        };
        let no_aws = under(HostIdentityPolicy::parse(Some("azure"), None).unwrap());
        assert!(matches!(
            refusal(open("s3://lake/t", &io, Credentials::Ambient, &no_aws)),
            IcebergError::HostIdentityRefused {
                kind: HostIdentityKind::Aws
            }
        ));
        assert!(matches!(
            refusal(open("s3a://lake/t", &io, Credentials::Ambient, &no_aws)),
            IcebergError::HostIdentityRefused { .. }
        ));

        let lake_only = under(HostIdentityPolicy::parse(None, Some("lake")).unwrap());
        assert!(open("s3://lake/t", &io, Credentials::Ambient, &lake_only).is_ok());
        match refusal(open(
            "s3://stack-internal/t",
            &io,
            Credentials::Ambient,
            &lake_only,
        )) {
            IcebergError::AmbientBucketRefused { bucket } => assert_eq!(bucket, "stack-internal"),
            other => panic!("expected AmbientBucketRefused, got {other:?}"),
        }
        // An endpoint override could name a bucket of its own.
        let redirected = DeltaIoConfig {
            s3_endpoint: Some("https://stack-internal.s3.us-east-1.amazonaws.com".to_string()),
            s3_path_style: true,
            ..io.clone()
        };
        assert!(matches!(
            refusal(open(
                "s3://lake/t",
                &redirected,
                Credentials::Ambient,
                &lake_only
            )),
            IcebergError::AmbientEndpointRefused { .. }
        ));
    }

    #[test]
    fn an_ambient_azure_table_is_the_azure_host_identity_but_a_service_principal_is_not() {
        let location = "abfss://container@account.dfs.core.windows.net/t";
        let no_azure = under(HostIdentityPolicy::parse(Some("aws"), None).unwrap());
        assert!(matches!(
            refusal(open(
                location,
                &DeltaIoConfig::default(),
                Credentials::Ambient,
                &no_azure
            )),
            IcebergError::HostIdentityRefused {
                kind: HostIdentityKind::Azure
            }
        ));
        // The source's own service principal is not a host identity.
        let own = DeltaIoConfig {
            azure: Some(AzureAuth::ClientSecret {
                tenant_id: "00000000-0000-0000-0000-000000000000".to_string(),
                client_id: "11111111-1111-1111-1111-111111111111".to_string(),
                client_secret: fluree_db_iceberg::ConfigValue::literal("s3cr3t"),
            }),
            ..DeltaIoConfig::default()
        };
        assert!(open(
            location,
            &own,
            Credentials::Ambient,
            &under(HostIdentityPolicy::deny_all())
        )
        .is_ok());
    }
}
