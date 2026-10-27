//! The one place an Iceberg graph source's storage is decided and built.
//!
//! Which identity reads a source's files is decided by its configuration
//! (`io.storage_auth`, else what `io.vended_credentials` has always meant),
//! never by what a path or a catalog response happens to allow:
//!
//! | identity | reads | built as |
//! |---|---|---|
//! | `vended` | the catalog's per-table credentials (S3 keys or a GCS token), required | S3 / GCS, renewing |
//! | `ambient` | the host's AWS credential chain (a host identity) | S3 |
//! | `catalog` | the REST catalog's own Google identity | GCS |
//! | `google` | a Google identity of the source's own | GCS |
//! | (any non-Google, `file://` location) | the local filesystem | file |
//!
//! Each identity serves a fixed set of schemes (`gs` for Google and vended GCS
//! tokens; `s3`/`s3a` for AWS, plus `gs` through a GCS interop endpoint;
//! `file` for local); anything else is refused (`err:storage/SchemeNotServed`).
//! The instance's [`HostIdentityPolicy`](fluree_db_iceberg::HostIdentityPolicy)
//! governs the host identities: `aws` and its bucket allowlist here, the
//! Google host kinds where a Google identity is built.
//!
//! Deciding is split from building. [`plan_storage`] decides from the source's
//! configuration and the location a read starts at, applies the policy, proves
//! a Google identity (a token for it mints), and fixes the [`StorageScope`]
//! every location-keyed cache is keyed by — all before any cache is consulted,
//! so a cached object is only ever served to the identity that read it.
//! [`StoragePlan::build`] then builds the store from the catalog's grant;
//! the query path defers that until a read actually misses every cache.

use std::sync::Arc;

use fluree_db_iceberg::auth::GoogleIdentity;
use fluree_db_iceberg::catalog::{LoadTableResponse, SendCatalogClient, TableIdentifier};
use fluree_db_iceberg::config::{CatalogConfig, IcebergGsConfig, IoConfig, StorageAuth};
use fluree_db_iceberg::credential::{
    CatalogVendedSource, RenewingVendedCredentials, VendedCredentialSource, VendedCredentials,
};
use fluree_db_iceberg::io::gcs::{gcs_endpoint, DEVSTORAGE_READ_ONLY_SCOPE};
use fluree_db_iceberg::io::location::{s3_serves, LocationScheme};
use fluree_db_iceberg::io::{FileIcebergStorage, GcsIcebergStorage, IcebergStorageBackend};
use fluree_db_iceberg::{
    HostIdentityKind, IcebergError, IdentityContext, Result as IcebergResult, S3IcebergStorage,
    StorageScope,
};

use crate::graph_source::config::{CatalogMode, IcebergConnectionConfig};

/// What the catalog granted a read of one table: the table's metadata
/// location and whatever storage credentials it vended (with the way to vend
/// them again), or, for a catalog-less (Direct) source, nothing.
///
/// Opaque: a catalog grant is built only by this crate's own `loadTable`
/// paths, so storage built through [`crate::Fluree::graph_source_storage`]
/// can only use credentials a catalog actually vended for the table.
#[derive(Clone)]
pub struct CatalogGrant {
    kind: GrantKind,
}

#[derive(Clone)]
enum GrantKind {
    Direct,
    Catalog {
        metadata_location: String,
        credentials: Option<VendedCredentials>,
        renewal: Option<Arc<dyn VendedCredentialSource>>,
    },
}

/// Redacting `Debug`: whether credentials were vended, never what they are.
impl std::fmt::Debug for CatalogGrant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.kind {
            GrantKind::Direct => f.write_str("CatalogGrant::Direct"),
            GrantKind::Catalog {
                metadata_location,
                credentials,
                renewal,
            } => f
                .debug_struct("CatalogGrant::Catalog")
                .field("metadata_location", metadata_location)
                .field("vended", &credentials.as_ref().map(VendedCredentials::kind))
                .field("renews", &renewal.is_some())
                .finish(),
        }
    }
}

impl CatalogGrant {
    /// The grant of a catalog-less (Direct) source: no catalog, so no vended
    /// credentials; its files are read with its configured identity.
    pub fn direct() -> Self {
        Self {
            kind: GrantKind::Direct,
        }
    }

    /// The grant a `loadTable` response carries, renewed through `catalog`
    /// when the credentials near expiry.
    pub(crate) fn from_load(
        load: &LoadTableResponse,
        catalog: Option<(&Arc<dyn SendCatalogClient>, &TableIdentifier)>,
    ) -> Self {
        let renewal = load
            .credentials
            .as_ref()
            .and(catalog)
            .map(|(catalog, table)| {
                Arc::new(CatalogVendedSource::new(
                    Arc::clone(catalog),
                    table.clone(),
                    load.metadata_location.clone(),
                )) as Arc<dyn VendedCredentialSource>
            });
        Self {
            kind: GrantKind::Catalog {
                metadata_location: load.metadata_location.clone(),
                credentials: load.credentials.clone(),
                renewal,
            },
        }
    }

    /// A catalog grant of `metadata_location` with no vended credentials
    /// (catalogs that never vend: Glue, S3 Tables, or a REST catalog asked for
    /// none).
    pub(crate) fn unvended(metadata_location: impl Into<String>) -> Self {
        Self {
            kind: GrantKind::Catalog {
                metadata_location: metadata_location.into(),
                credentials: None,
                renewal: None,
            },
        }
    }

    fn credentials(&self) -> Option<&VendedCredentials> {
        match &self.kind {
            GrantKind::Direct => None,
            GrantKind::Catalog { credentials, .. } => credentials.as_ref(),
        }
    }
}

/// Which identity reads a source's files, as [`crate::Fluree::graph_source_storage`]
/// decided it.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageDecision {
    /// The catalog's vended credentials for the table, renewed from it.
    Vended {
        /// The catalog that vended them.
        catalog: String,
    },
    /// The host's ambient AWS credential chain.
    Ambient,
    /// The REST catalog's own Google identity (`io.storage_auth = catalog`).
    CatalogIdentity {
        /// Its non-secret principal (an SA email, `wif:…`, `metadata`, …).
        principal: String,
    },
    /// The source's own Google identity (`io.storage_auth = google`).
    SourceIdentity {
        /// Its non-secret principal.
        principal: String,
    },
    /// The local filesystem (a catalog-less local table).
    LocalFile,
}

impl StorageDecision {
    /// The short name the onboarding probe reports
    /// (`StorageAccessReport::credential_source`): `vended`, `ambient`,
    /// `catalog`, `google` or `local`.
    pub fn source_name(&self) -> &'static str {
        match self {
            StorageDecision::Vended { .. } => "vended",
            StorageDecision::Ambient => "ambient",
            StorageDecision::CatalogIdentity { .. } => "catalog",
            StorageDecision::SourceIdentity { .. } => "google",
            StorageDecision::LocalFile => "local",
        }
    }
}

/// A source's storage, with the decision that built it and the scope its
/// cached objects are keyed by.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct ResolvedStorage {
    /// The store. Every backend refuses a path its identity does not serve.
    pub backend: IcebergStorageBackend,
    /// Which identity reads.
    pub decision: StorageDecision,
    /// The identity's cache scope: pass it to
    /// [`fluree_db_iceberg::SendParquetReader::with_caches`].
    pub scope: StorageScope,
}

impl crate::Fluree {
    /// The storage a graph source reads its Iceberg files with, decided from
    /// `config` under this instance's identity context (host-identity policy,
    /// AWS subject, secret resolver) and built from what the catalog granted.
    ///
    /// This is the only way graph-source storage should be built: it applies
    /// the operator's host-identity policy (ambient AWS and its bucket
    /// allowlist, the Google host identities), binds the store to the schemes
    /// its identity serves, and renews vended credentials inside the store.
    /// Use [`CatalogGrant::direct`] for a catalog-less source.
    pub async fn graph_source_storage(
        &self,
        config: &IcebergGsConfig,
        grant: &CatalogGrant,
    ) -> crate::Result<ResolvedStorage> {
        let location = match (&grant.kind, &config.catalog) {
            (GrantKind::Direct, CatalogConfig::Direct { table_location }) => table_location.clone(),
            (
                GrantKind::Catalog {
                    metadata_location, ..
                },
                CatalogConfig::Direct { .. },
            ) => {
                // A Direct source has no catalog to grant anything.
                let _ = metadata_location;
                return Err(crate::ApiError::config(
                    "a catalog grant cannot be used for a Direct (catalog-less) source",
                ));
            }
            (
                GrantKind::Catalog {
                    metadata_location, ..
                },
                _,
            ) => metadata_location.clone(),
            (GrantKind::Direct, _) => {
                return Err(crate::ApiError::config(
                    "a catalog-backed source's storage needs the catalog's grant for the table \
                     (CatalogGrant::direct() is for Direct sources only)",
                ))
            }
        };
        let plan = plan_storage(
            &config.catalog,
            &config.storage_io(),
            &location,
            self.identity_context(),
        )
        .await
        .map_err(|e| super::iceberg_catalog::storage_api_error("Failed to decide storage", e))?;
        plan.build(grant)
            .await
            .map_err(|e| super::iceberg_catalog::storage_api_error("Failed to build storage", e))
    }
}

/// A source's storage identity, decided and (for a Google identity) proven,
/// with its cache scope. Built by [`plan_storage`]; cheap to clone into a
/// deferred storage builder.
#[derive(Clone, Debug)]
pub(crate) struct StoragePlan {
    identity: Planned,
    scope: StorageScope,
    io: IoConfig,
}

#[derive(Clone, Debug)]
enum Planned {
    Local,
    Ambient {
        buckets: Option<Vec<String>>,
    },
    Vended {
        catalog: String,
    },
    Google {
        identity: Arc<GoogleIdentity>,
        /// The billing project storage reads name (`x-goog-user-project`):
        /// only one configured for storage (`StorageAuth::Google`'s
        /// `quota_project`), never the catalog's.
        user_project: Option<String>,
        from_catalog: bool,
    },
}

impl StoragePlan {
    /// The scope every location-keyed cache of this source is keyed by.
    pub(crate) fn scope(&self) -> &StorageScope {
        &self.scope
    }

    /// The identity, as the public decision names it.
    pub(crate) fn decision(&self) -> StorageDecision {
        match &self.identity {
            Planned::Local => StorageDecision::LocalFile,
            Planned::Ambient { .. } => StorageDecision::Ambient,
            Planned::Vended { catalog } => StorageDecision::Vended {
                catalog: catalog.clone(),
            },
            Planned::Google {
                identity,
                from_catalog: true,
                ..
            } => StorageDecision::CatalogIdentity {
                principal: identity.principal().to_string(),
            },
            Planned::Google { identity, .. } => StorageDecision::SourceIdentity {
                principal: identity.principal().to_string(),
            },
        }
    }

    /// §2 fail-closed: a source that reads with vended credentials, granted
    /// none, is refused — never read with any other identity. Checked before a
    /// query reuses a store it built earlier, so a cached store can never stand
    /// in for a refusal.
    pub(crate) fn check_grant(&self, grant: &CatalogGrant) -> IcebergResult<()> {
        match &self.identity {
            Planned::Vended { catalog } if grant.credentials().is_none() => {
                Err(IcebergError::CatalogCredentialsNotVended {
                    catalog_uri: catalog.clone(),
                })
            }
            _ => Ok(()),
        }
    }

    /// Build the store this plan decided, from what the catalog granted.
    pub(crate) async fn build(&self, grant: &CatalogGrant) -> IcebergResult<ResolvedStorage> {
        self.check_grant(grant)?;
        let io = &self.io;
        let backend = match &self.identity {
            Planned::Local => IcebergStorageBackend::File(FileIcebergStorage::new()),
            Planned::Ambient { buckets } => {
                tracing::info!(region = ?io.s3_region, endpoint = ?io.s3_endpoint, "Using ambient AWS credentials");
                // The one place graph-source storage reads with the host's
                // own AWS chain; the policy allowed it in `plan_storage`.
                #[allow(deprecated)]
                let s3 = S3IcebergStorage::from_default_chain(
                    io.s3_region.as_deref(),
                    io.s3_endpoint.as_deref(),
                    io.s3_path_style,
                )
                .await?;
                IcebergStorageBackend::S3(s3.with_bucket_allowlist(buckets.clone()))
            }
            Planned::Vended { .. } => {
                let GrantKind::Catalog {
                    credentials: Some(credentials),
                    renewal,
                    ..
                } = &grant.kind
                else {
                    unreachable!("check_grant refused a vended plan without credentials")
                };
                tracing::info!(kind = credentials.kind(), region = ?io.s3_region, endpoint = ?io.s3_endpoint, "Using vended credentials from catalog");
                let renewing = Arc::new(RenewingVendedCredentials::new(
                    credentials.clone(),
                    renewal.clone(),
                ));
                match credentials {
                    VendedCredentials::S3(s3) => IcebergStorageBackend::S3(
                        S3IcebergStorage::from_renewing_vended(
                            renewing,
                            s3,
                            io.s3_region.as_deref(),
                            io.s3_endpoint.as_deref(),
                            io.s3_path_style,
                        )
                        .await?,
                    ),
                    VendedCredentials::Gcs(gcs) => IcebergStorageBackend::Gcs(
                        GcsIcebergStorage::with_vended(renewing, gcs.user_project.clone())?,
                    ),
                }
            }
            Planned::Google {
                identity,
                user_project,
                ..
            } => IcebergStorageBackend::Gcs(GcsIcebergStorage::with_identity(
                Arc::clone(identity),
                user_project.clone(),
            )?),
        };
        Ok(ResolvedStorage {
            backend,
            decision: self.decision(),
            scope: self.scope.clone(),
        })
    }
}

/// The identity a source's configuration asks for: `io.storage_auth`, else
/// what `io.vended_credentials` has always meant (vended on a REST catalog,
/// ambient elsewhere: no other catalog vends).
fn effective_storage_auth(catalog: &CatalogConfig, io: &IoConfig) -> StorageAuth {
    match &io.storage_auth {
        Some(storage_auth) => storage_auth.clone(),
        None if io.vended_credentials && matches!(catalog, CatalogConfig::Rest { .. }) => {
            StorageAuth::Vended
        }
        None => StorageAuth::Ambient,
    }
}

/// The cache-scope rendering of `ctx`, or `None` for the default context
/// (every host identity allowed, the host's own AWS chain), which leaves the
/// pre-scope keys unchanged.
pub(crate) fn context_discriminant(ctx: &IdentityContext) -> Option<String> {
    static DEFAULT: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    let scope = ctx.cache_scope();
    (scope != *DEFAULT.get_or_init(|| IdentityContext::default().cache_scope())).then_some(scope)
}

/// A stable, non-secret fingerprint of a REST catalog's auth config, as stored
/// (references, not resolved secrets): two sources with different catalog
/// principals never share a vended read's cache.
fn auth_fingerprint(catalog: &CatalogConfig) -> String {
    use sha2::{Digest, Sha256};
    let CatalogConfig::Rest { auth, .. } = catalog else {
        return "-".to_string();
    };
    // Serialization of a config this process already holds cannot fail.
    let stored = serde_json::to_vec(auth).unwrap_or_default();
    let digest = Sha256::digest(&stored);
    digest[..12].iter().map(|b| format!("{b:02x}")).collect()
}

/// The bucket of an object-store `location`, if it has one.
fn bucket_of(location: &str) -> Option<&str> {
    let rest = location.split_once("://")?.1;
    let bucket = rest.split('/').next()?;
    (!bucket.is_empty()).then_some(bucket)
}

/// Decide, apply the host-identity policy to, and (for a Google identity)
/// prove the storage identity a read of `location` uses, from the source's
/// catalog and `io` (its [`IcebergGsConfig::storage_io`]). No store is built;
/// no cache is consulted.
///
/// `location` is where the read starts (the table's metadata location, or a
/// Direct source's table location). The store built from the plan refuses any
/// other path its identity does not serve as it reads.
pub(crate) async fn plan_storage(
    catalog: &CatalogConfig,
    io: &IoConfig,
    location: &str,
    ctx: &IdentityContext,
) -> IcebergResult<StoragePlan> {
    let scheme = LocationScheme::of(location);
    let storage_auth = effective_storage_auth(catalog, io);
    let context = ctx.cache_scope();
    let plan = |identity, scope| StoragePlan {
        identity,
        scope,
        io: io.clone(),
    };
    let refuse = |identity: &str, served: &str| IcebergError::SchemeNotServed {
        path: location.to_string(),
        scheme: location
            .split_once("://")
            .map_or_else(|| "file".to_string(), |(s, _)| s.to_string()),
        identity: identity.to_string(),
        served: served.to_string(),
    };
    match storage_auth {
        StorageAuth::Ambient => {
            if scheme == LocationScheme::Local {
                // A catalog-less local table reads the filesystem: no identity,
                // and the operator's local-roots allowlist governs it. Only the
                // operator registers a Direct location; a catalog's answer is
                // not the operator's to vouch for, so a catalog that places a
                // table on this host's disk is refused, as before storage
                // identities (catalog sources always read through S3).
                return match catalog {
                    CatalogConfig::Direct { .. } => {
                        Ok(plan(Planned::Local, StorageScope::shared()))
                    }
                    CatalogConfig::Rest { .. }
                    | CatalogConfig::Glue { .. }
                    | CatalogConfig::S3Tables { .. } => {
                        Err(IcebergError::LocalLocationFromCatalog {
                            location: location.to_string(),
                        })
                    }
                };
            }
            ctx.policy.ensure(HostIdentityKind::Aws)?;
            let endpoint = io.s3_endpoint.as_deref();
            if !s3_serves(scheme, endpoint) {
                return Err(refuse(
                    "the host's ambient AWS identity",
                    "s3:// and s3a:// (gs:// only through the GCS interop endpoint)",
                ));
            }
            let buckets = ctx.policy.ambient_aws_buckets.clone();
            // An endpoint can address a bucket of its own (a virtual-hosted
            // host read path-style), so under a bucket allowlist the bucket a
            // path names would not be the bucket read.
            if let (Some(_), Some(endpoint)) = (&buckets, endpoint) {
                return Err(IcebergError::AmbientEndpointRefused {
                    endpoint: endpoint.to_string(),
                });
            }
            if let (Some(allowed), Some(bucket)) = (&buckets, bucket_of(location)) {
                if !allowed.iter().any(|b| b == bucket) {
                    return Err(IcebergError::AmbientBucketRefused {
                        bucket: bucket.to_string(),
                    });
                }
            }
            let scope = match (endpoint, context_discriminant(ctx)) {
                (None, None) => StorageScope::shared(),
                _ => StorageScope::of(&[
                    ("kind", "ambient"),
                    ("endpoint", endpoint.unwrap_or("-")),
                    ("context", &context),
                ]),
            };
            Ok(plan(Planned::Ambient { buckets }, scope))
        }
        StorageAuth::Vended => {
            let CatalogConfig::Rest { uri, warehouse, .. } = catalog else {
                return Err(IcebergError::Config(
                    "io.storage_auth `vended` needs a REST catalog: no other catalog mode vends \
                     storage credentials"
                        .to_string(),
                ));
            };
            if matches!(scheme, LocationScheme::Local | LocationScheme::Other) {
                return Err(refuse(
                    "the catalog's vended credentials",
                    "s3://, s3a:// and gs://",
                ));
            }
            let scope = StorageScope::of(&[
                ("kind", "vended"),
                ("catalog", uri),
                ("warehouse", warehouse.as_deref().unwrap_or("-")),
                ("auth", &auth_fingerprint(catalog)),
                ("endpoint", io.s3_endpoint.as_deref().unwrap_or("-")),
                ("context", &context),
            ]);
            Ok(plan(
                Planned::Vended {
                    catalog: uri.clone(),
                },
                scope,
            ))
        }
        StorageAuth::Catalog => {
            let CatalogConfig::Rest { auth, .. } = catalog else {
                return Err(IcebergError::Config(
                    "io.storage_auth `catalog` needs a REST catalog authenticated with a Google \
                     identity"
                        .to_string(),
                ));
            };
            if scheme != LocationScheme::Gs {
                return Err(refuse("the catalog's Google identity", "gs://"));
            }
            let key = identity_key("catalog", auth, ctx);
            let identity = cached_identity(key, || async {
                let hydrated = auth.hydrate(ctx.secret_resolver.as_ref()).await?;
                hydrated.google_identity(ctx)?.ok_or_else(|| {
                    IcebergError::Config(
                        "io.storage_auth `catalog` needs a REST catalog authenticated with a \
                         Google identity (auth type `google` or `google_metadata`)"
                            .to_string(),
                    )
                })
            })
            .await?;
            // The catalog's quota project bills its BigLake calls; storage
            // reads use the catalog's identity but not its quota project.
            // Sent to GCS, `x-goog-user-project` is a requester-pays billing
            // project, which needs serviceusage.services.use on it: a storage
            // read bills to a project only when one is configured for storage.
            google_plan(identity, None, true, io, &context, gcs_endpoint()).await
        }
        StorageAuth::Google {
            credentials,
            quota_project,
        } => {
            if scheme != LocationScheme::Gs {
                return Err(refuse("the source's Google identity", "gs://"));
            }
            let key = identity_key("google", &(&credentials, &quota_project), ctx);
            let identity = cached_identity(key, || async {
                let hydrated = credentials.hydrate(ctx.secret_resolver.as_ref()).await?;
                GoogleIdentity::new(&hydrated, quota_project.as_deref(), ctx)
            })
            .await?;
            // Only the quota project configured for storage bills its reads,
            // not one a credential file names for itself.
            let user_project = quota_project.clone();
            google_plan(identity, user_project, false, io, &context, gcs_endpoint()).await
        }
    }
}

/// The key a built storage identity is cached under: its kind, a digest of
/// the configuration as stored (references, not resolved secrets) and the
/// identity context, which names whose secrets the resolver resolves (see
/// [`fluree_db_iceberg::SecretResolver::scope_id`]), the policy and the AWS
/// subject. In memory only.
fn identity_key(kind: &str, stored: &impl serde::Serialize, ctx: &IdentityContext) -> String {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(serde_json::to_vec(stored).unwrap_or_default());
    let digest: String = digest[..16].iter().map(|b| format!("{b:02x}")).collect();
    format!("{kind}\u{1f}{digest}\u{1f}{}", ctx.cache_scope())
}

/// The storage identities planned so far, process-wide, so a warm query's
/// plan neither hydrates its secret (a secret-store call) nor rebuilds the
/// identity (a key parse and an HTTP client). Bounded by the catalog-client
/// TTL (`FLUREE_ICEBERG_REST_CLIENT_TTL_SECS`), so a rotated secret behind a
/// reference is picked up within it, as for catalog clients.
fn identities() -> &'static moka::sync::Cache<String, Arc<GoogleIdentity>> {
    static CACHE: std::sync::OnceLock<moka::sync::Cache<String, Arc<GoogleIdentity>>> =
        std::sync::OnceLock::new();
    CACHE.get_or_init(|| {
        moka::sync::Cache::builder()
            .max_capacity(256)
            .time_to_live(std::time::Duration::from_secs(
                super::cache::rest_client_ttl_secs(),
            ))
            .build()
    })
}

/// The identity cached under `key`, else the one `build` makes (cached when
/// caching is on). The token proof still runs on every plan
/// ([`google_plan`]); only the construction is shared.
async fn cached_identity<F, Fut>(key: String, build: F) -> IcebergResult<Arc<GoogleIdentity>>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = IcebergResult<GoogleIdentity>>,
{
    let caching =
        super::catalog_session::cache_enabled() && super::cache::rest_client_ttl_secs() > 0;
    if caching {
        if let Some(identity) = identities().get(&key) {
            return Ok(identity);
        }
    }
    let identity = Arc::new(build().await?);
    if caching {
        identities().insert(key, Arc::clone(&identity));
    }
    Ok(identity)
}

/// A Google-identity plan, proven: a storage token for the identity must
/// mint before anything it would read is served, cached or not. The scope
/// names the identity by its non-secret principal, which is safe to share
/// only because the caller just proved it holds the identity: a source that
/// names another tenant's service account without its key never gets this
/// far, so never reaches the other tenant's cached files. The token is cached
/// process-wide, so the proof costs a token exchange per identity per hour.
async fn google_plan(
    identity: Arc<GoogleIdentity>,
    user_project: Option<String>,
    from_catalog: bool,
    io: &IoConfig,
    context: &str,
    endpoint: &str,
) -> IcebergResult<StoragePlan> {
    // The store built from this plan sends the identity's token to `endpoint`
    // (the process's GCS endpoint): refuse a host identity whose token may not
    // go there before minting one. The store checks again when it is built.
    identity.ensure_token_destination(endpoint)?;
    identity.access_token(&[DEVSTORAGE_READ_ONLY_SCOPE]).await?;
    let scope = StorageScope::of(&[
        ("kind", "google"),
        ("principal", identity.principal()),
        ("endpoint", endpoint),
        ("context", context),
    ]);
    Ok(StoragePlan {
        identity: Planned::Google {
            identity,
            user_project,
            from_catalog,
        },
        scope,
        io: io.clone(),
    })
}

/// An onboarding connection's catalog, as the stored config models it.
pub(crate) fn connection_catalog(conn: &IcebergConnectionConfig) -> CatalogConfig {
    match &conn.catalog_mode {
        CatalogMode::Rest(rest) => CatalogConfig::Rest {
            catalog_type: "polaris".to_string(),
            uri: rest.catalog_uri.clone(),
            auth: rest.auth.clone(),
            warehouse: rest.warehouse.clone(),
        },
        CatalogMode::Direct { table_location } => CatalogConfig::direct(table_location),
        CatalogMode::Glue { region, catalog_id } => {
            CatalogConfig::glue(region.clone(), catalog_id.clone())
        }
        CatalogMode::S3Tables {
            region,
            table_bucket_arn,
        } => CatalogConfig::s3_tables(region.clone(), table_bucket_arn.clone()),
    }
}

#[cfg(test)]
mod tests;
