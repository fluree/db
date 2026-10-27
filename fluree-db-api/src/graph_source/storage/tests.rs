//! The storage decision, without network: which identity each configuration
//! reads as, what the host-identity policy refuses, which schemes each
//! identity serves, and the scope every cache is keyed by.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use fluree_db_iceberg::auth::{AuthConfig, ExternalAccountConfig, GoogleCredentials};
use fluree_db_iceberg::config::{CatalogConfig, IoConfig, StorageAuth};
use fluree_db_iceberg::credential::{GcsCredentials, S3Credentials};
use fluree_db_iceberg::{
    AwsSubjectCredentials, ConfigValue, HostIdentityKind, HostIdentityPolicy, IcebergError,
    IdentityContext, SendIcebergStorage,
};

use super::*;

fn open() -> IdentityContext {
    IdentityContext::default()
}

fn with_policy(policy: HostIdentityPolicy) -> IdentityContext {
    IdentityContext {
        policy,
        ..IdentityContext::default()
    }
}

fn ambient_io() -> IoConfig {
    IoConfig {
        vended_credentials: false,
        s3_region: Some("us-east-1".to_string()),
        ..IoConfig::default()
    }
}

fn rest_with(uri: &str, auth: AuthConfig) -> CatalogConfig {
    CatalogConfig::Rest {
        catalog_type: "rest".to_string(),
        uri: uri.to_string(),
        auth,
        warehouse: Some("wh".to_string()),
    }
}

fn bearer(token: &str) -> AuthConfig {
    AuthConfig::Bearer {
        token: ConfigValue::literal(token),
    }
}

fn s3_keys() -> VendedCredentials {
    VendedCredentials::S3(S3Credentials {
        access_key_id: "AKIA".to_string(),
        secret_access_key: "secret".to_string(),
        session_token: None,
        expires_at: Some(chrono::Utc::now() + chrono::Duration::hours(1)),
        endpoint: None,
        region: Some("us-east-1".to_string()),
        path_style: false,
    })
}

fn gcs_token() -> VendedCredentials {
    VendedCredentials::Gcs(GcsCredentials {
        token: "ya29.vended".to_string(),
        expires_at: chrono::Utc::now() + chrono::Duration::hours(1),
        user_project: None,
    })
}

fn load(location: &str, credentials: Option<VendedCredentials>) -> LoadTableResponse {
    LoadTableResponse {
        metadata_location: location.to_string(),
        config: HashMap::new(),
        credentials,
        metadata: None,
    }
}

const S3_META: &str = "s3://lake/wh/t/metadata/v1.metadata.json";
const GS_META: &str = "gs://lake/wh/t/metadata/v1.metadata.json";

async fn plan(
    catalog: &CatalogConfig,
    io: &IoConfig,
    location: &str,
    ctx: &IdentityContext,
) -> IcebergResult<StoragePlan> {
    plan_storage(catalog, io, location, ctx).await
}

#[tokio::test]
async fn the_default_ambient_configuration_keeps_the_shared_scope() {
    // What every install read as before storage identities: ambient AWS, no
    // endpoint override, the default identity context.
    let direct = CatalogConfig::direct("s3://lake/wh/t");
    let glue = CatalogConfig::glue(Some("us-east-1".to_string()), None);
    let rest_ambient = rest_with("https://cat.example", AuthConfig::None);
    for (catalog, location) in [
        (&direct, "s3://lake/wh/t"),
        (&glue, S3_META),
        (&rest_ambient, S3_META),
    ] {
        let plan = plan(catalog, &ambient_io(), location, &open())
            .await
            .unwrap();
        assert_eq!(plan.decision(), StorageDecision::Ambient, "{catalog:?}");
        assert!(plan.scope().is_shared(), "{catalog:?}");
    }
    // A catalog-less local table reads the filesystem, also unscoped.
    let local = plan(
        &CatalogConfig::direct("file:///data/wh/t"),
        &ambient_io(),
        "file:///data/wh/t",
        &open(),
    )
    .await
    .unwrap();
    assert_eq!(local.decision(), StorageDecision::LocalFile);
    assert!(local.scope().is_shared());
}

#[tokio::test]
async fn every_other_identity_gets_a_scope_of_its_own() {
    let direct = CatalogConfig::direct("s3://lake/wh/t");
    let shared = plan(&direct, &ambient_io(), "s3://lake/wh/t", &open())
        .await
        .unwrap();

    // An endpoint override is another store (MinIO, the GCS interop endpoint).
    let mut minio = ambient_io();
    minio.s3_endpoint = Some("http://minio.internal:9000".to_string());
    let minio = plan(&direct, &minio, "s3://lake/wh/t", &open())
        .await
        .unwrap();
    assert!(!minio.scope().is_shared());

    // A stricter policy, or an embedder's AWS subject, is another context.
    let strict = with_policy(HostIdentityPolicy::parse(Some("aws"), None).unwrap());
    let strict = plan(&direct, &ambient_io(), "s3://lake/wh/t", &strict)
        .await
        .unwrap();
    assert!(!strict.scope().is_shared());
    assert_ne!(strict.scope(), minio.scope());

    // Vended reads are scoped by the catalog, its warehouse and its auth.
    let vended = IoConfig::default();
    let a = plan(
        &rest_with("https://cat.example", bearer("a")),
        &vended,
        S3_META,
        &open(),
    )
    .await
    .unwrap();
    let a_again = plan(
        &rest_with("https://cat.example", bearer("a")),
        &vended,
        S3_META,
        &open(),
    )
    .await
    .unwrap();
    let b = plan(
        &rest_with("https://cat.example", bearer("b")),
        &vended,
        S3_META,
        &open(),
    )
    .await
    .unwrap();
    let other_catalog = plan(
        &rest_with("https://other.example", bearer("a")),
        &vended,
        S3_META,
        &open(),
    )
    .await
    .unwrap();
    assert_eq!(a.scope(), a_again.scope(), "one identity, one scope");
    for other in [&b, &other_catalog, &shared] {
        assert_ne!(a.scope(), other.scope());
    }
    assert_eq!(
        a.decision(),
        StorageDecision::Vended {
            catalog: "https://cat.example".to_string()
        }
    );
}

#[tokio::test]
async fn a_vended_source_reads_only_what_its_catalog_vended() {
    let catalog = rest_with("https://cat.example", bearer("a"));
    let plan = plan(&catalog, &IoConfig::default(), S3_META, &open())
        .await
        .unwrap();

    // §2: nothing vended is a refusal, never another identity.
    match plan
        .build(&CatalogGrant::from_load(&load(S3_META, None), None))
        .await
    {
        Err(IcebergError::CatalogCredentialsNotVended { catalog_uri }) => {
            assert_eq!(catalog_uri, "https://cat.example");
        }
        other => panic!("expected CatalogCredentialsNotVended, got {other:?}"),
    }
    let s3 = plan
        .build(&CatalogGrant::from_load(
            &load(S3_META, Some(s3_keys())),
            None,
        ))
        .await
        .unwrap();
    assert!(matches!(s3.backend, IcebergStorageBackend::S3(_)));
    let gcs_plan = super::plan_storage(&catalog, &IoConfig::default(), GS_META, &open())
        .await
        .unwrap();
    let gcs = gcs_plan
        .build(&CatalogGrant::from_load(
            &load(GS_META, Some(gcs_token())),
            None,
        ))
        .await
        .unwrap();
    assert!(matches!(gcs.backend, IcebergStorageBackend::Gcs(_)));
    // A vended GCS token reads gs:// only.
    let err = SendIcebergStorage::read(&gcs.backend, S3_META)
        .await
        .unwrap_err();
    assert!(
        matches!(err, IcebergError::SchemeNotServed { .. }),
        "{err:?}"
    );

    // Only a REST catalog vends; vended credentials never read local files.
    let direct_vended = IoConfig::default().with_storage_auth(StorageAuth::Vended);
    assert!(matches!(
        super::plan_storage(
            &CatalogConfig::direct("s3://lake/wh/t"),
            &direct_vended,
            "s3://lake/wh/t",
            &open()
        )
        .await,
        Err(IcebergError::Config(_))
    ));
    assert!(matches!(
        super::plan_storage(
            &catalog,
            &IoConfig::default(),
            "file:///wh/t/m.json",
            &open()
        )
        .await,
        Err(IcebergError::SchemeNotServed { .. })
    ));
}

#[tokio::test]
async fn an_explicitly_ambient_source_never_reads_with_vended_credentials() {
    let catalog = rest_with("https://cat.example", bearer("a"));
    let io = IoConfig::default().with_storage_auth(StorageAuth::Ambient);
    let plan = plan(&catalog, &io, S3_META, &open()).await.unwrap();
    let built = plan
        .build(&CatalogGrant::from_load(
            &load(S3_META, Some(s3_keys())),
            None,
        ))
        .await
        .unwrap();
    assert_eq!(built.decision, StorageDecision::Ambient);
}

#[tokio::test]
async fn the_host_identity_policy_governs_ambient_aws_and_its_buckets() {
    let direct = CatalogConfig::direct("s3://lake/wh/t");
    let no_aws = with_policy(HostIdentityPolicy::parse(Some("google-wif"), None).unwrap());
    match plan(&direct, &ambient_io(), "s3://lake/wh/t", &no_aws).await {
        Err(IcebergError::HostIdentityRefused { kind }) => assert_eq!(kind, HostIdentityKind::Aws),
        other => panic!("expected HostIdentityRefused(aws), got {other:?}"),
    }
    // A local table borrows no host identity.
    assert!(plan(
        &CatalogConfig::direct("file:///data/wh/t"),
        &ambient_io(),
        "file:///data/wh/t",
        &no_aws
    )
    .await
    .is_ok());

    let lake_only = with_policy(HostIdentityPolicy::parse(None, Some("lake")).unwrap());
    match plan(&direct, &ambient_io(), "s3://lakehouse/wh/t", &lake_only).await {
        Err(IcebergError::AmbientBucketRefused { bucket }) => assert_eq!(bucket, "lakehouse"),
        other => panic!("expected AmbientBucketRefused, got {other:?}"),
    }
    let allowed = plan(&direct, &ambient_io(), "s3://lake/wh/t", &lake_only)
        .await
        .unwrap()
        .build(&CatalogGrant::direct())
        .await
        .unwrap();
    // The store keeps the allowlist for every path it reads, not just the first.
    let err = SendIcebergStorage::read(&allowed.backend, "s3://other/data/f.parquet")
        .await
        .unwrap_err();
    assert!(
        matches!(err, IcebergError::AmbientBucketRefused { .. }),
        "{err:?}"
    );
}

#[tokio::test]
async fn an_identity_refuses_the_schemes_it_does_not_serve() {
    let direct_gs = CatalogConfig::direct("gs://lake/wh/t");
    // Without the GCS interop endpoint, an AWS client would read gs://b/k as
    // s3://b/k on AWS.
    assert!(matches!(
        plan(&direct_gs, &ambient_io(), "gs://lake/wh/t", &open()).await,
        Err(IcebergError::SchemeNotServed { .. })
    ));
    let mut interop = ambient_io();
    interop.s3_endpoint = Some("https://storage.googleapis.com".to_string());
    assert!(plan(&direct_gs, &interop, "gs://lake/wh/t", &open())
        .await
        .is_ok());

    // A Google identity reads gs:// only, refused before any identity is built.
    let google = ambient_io().with_storage_auth(StorageAuth::Google {
        credentials: GoogleCredentials::Metadata,
        quota_project: None,
    });
    assert!(matches!(
        plan(
            &CatalogConfig::direct("s3://lake/wh/t"),
            &google,
            "s3://lake/wh/t",
            &open()
        )
        .await,
        Err(IcebergError::SchemeNotServed { .. })
    ));
    let catalog = ambient_io().with_storage_auth(StorageAuth::Catalog);
    let google_catalog = rest_with(
        "https://biglake.googleapis.com/iceberg/v1/restcatalog",
        AuthConfig::GoogleMetadata {
            scopes: None,
            metadata_url: None,
        },
    );
    assert!(matches!(
        plan(&google_catalog, &catalog, S3_META, &open()).await,
        Err(IcebergError::SchemeNotServed { .. })
    ));
}

#[tokio::test]
async fn catalog_storage_needs_a_rest_catalog_with_a_google_identity() {
    let catalog = ambient_io().with_storage_auth(StorageAuth::Catalog);
    for config in [
        rest_with("https://cat.example", bearer("a")),
        CatalogConfig::glue(None, None),
    ] {
        assert!(
            matches!(
                plan(&config, &catalog, GS_META, &open()).await,
                Err(IcebergError::Config(_))
            ),
            "{config:?}"
        );
    }
}

#[tokio::test]
async fn a_google_host_identity_the_policy_refuses_is_refused_before_any_request() {
    let no_metadata = with_policy(HostIdentityPolicy::parse(Some("aws"), None).unwrap());
    let source = ambient_io().with_storage_auth(StorageAuth::Google {
        credentials: GoogleCredentials::Metadata,
        quota_project: None,
    });
    let catalog = ambient_io().with_storage_auth(StorageAuth::Catalog);
    let google_catalog = rest_with(
        "https://biglake.googleapis.com/iceberg/v1/restcatalog",
        AuthConfig::GoogleMetadata {
            scopes: None,
            metadata_url: None,
        },
    );
    for (config, io) in [
        (&CatalogConfig::direct("gs://lake/wh/t"), &source),
        (&google_catalog, &catalog),
    ] {
        match plan(config, io, GS_META, &no_metadata).await {
            Err(IcebergError::HostIdentityRefused { kind }) => {
                assert_eq!(kind, HostIdentityKind::GoogleMetadata);
            }
            other => panic!("expected HostIdentityRefused(google-metadata), got {other:?}"),
        }
    }
}

/// An embedder's AWS subject that cannot produce credentials: the WIF
/// exchange it would sign never starts.
#[derive(Debug)]
struct UnavailableSubject(&'static str);

#[async_trait]
impl AwsSubjectCredentials for UnavailableSubject {
    fn principal_key(&self) -> String {
        self.0.to_string()
    }

    async fn credentials(&self) -> fluree_db_iceberg::Result<fluree_db_iceberg::AwsCredentials> {
        Err(IcebergError::Credential("subject unavailable".to_string()))
    }

    fn region(&self) -> Option<String> {
        Some("us-east-1".to_string())
    }
}

/// What `gcloud iam workload-identity-pools create-cred-config … --aws`
/// writes (the fixture the identity module validates against).
fn gcloud_wif_config() -> serde_json::Map<String, serde_json::Value> {
    serde_json::json!({
        "type": "external_account",
        "audience": "//iam.googleapis.com/projects/123456789012/locations/global/workloadIdentityPools/aws-pool/providers/aws-provider",
        "subject_token_type": "urn:ietf:params:aws:token-type:aws4_request",
        "service_account_impersonation_url": "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/reader@lake-project.iam.gserviceaccount.com:generateAccessToken",
        "token_url": "https://sts.googleapis.com/v1/token",
        "credential_source": {
            "environment_id": "aws1",
            "region_url": "http://169.254.169.254/latest/meta-data/placement/availability-zone",
            "url": "http://169.254.169.254/latest/meta-data/iam/security-credentials",
            "regional_cred_verification_url": "https://sts.{region}.amazonaws.com?Action=GetCallerIdentity&Version=2011-06-15",
            "imdsv2_session_token_url": "http://169.254.169.254/latest/api/token"
        },
        "universe_domain": "googleapis.com"
    })
    .as_object()
    .unwrap()
    .clone()
}

/// A source naming a Google identity it cannot prove (here: a WIF config whose
/// AWS subject has no credentials) never gets a plan, so it can never be
/// served the files another source cached under that identity's principal.
#[tokio::test]
async fn a_google_identity_that_cannot_mint_a_token_gets_no_plan() {
    let ctx = IdentityContext {
        aws_subject: Some(Arc::new(UnavailableSubject("tenant-b:no-mint"))),
        ..IdentityContext::default()
    };
    let io = ambient_io().with_storage_auth(StorageAuth::Google {
        credentials: GoogleCredentials::ExternalAccount {
            config: ExternalAccountConfig::Inline(gcloud_wif_config()),
        },
        quota_project: None,
    });
    let err = plan(&CatalogConfig::direct("gs://lake/wh/t"), &io, GS_META, &ctx)
        .await
        .expect_err("an identity that mints no token is not proven");
    assert!(
        err.to_string().contains("subject unavailable"),
        "the mint was attempted and refused: {err}"
    );
}

#[tokio::test]
async fn two_sources_naming_one_gs_path_never_share_cached_metadata_or_manifests() {
    use crate::graph_source::cache::{CachedScanFiles, R2rmlCache};
    let io = IoConfig::default();
    let alice = plan(
        &rest_with("https://cat.example", bearer("alice")),
        &io,
        GS_META,
        &open(),
    )
    .await
    .unwrap();
    let bob = plan(
        &rest_with("https://cat.example", bearer("bob")),
        &io,
        GS_META,
        &open(),
    )
    .await
    .unwrap();
    let cache = R2rmlCache::new(4, 4);
    let metadata: fluree_db_iceberg::metadata::TableMetadata =
        serde_json::from_value(serde_json::json!({
            "format-version": 2, "table-uuid": "00000000-0000-0000-0000-000000000000",
            "location": "gs://lake/wh/t", "last-sequence-number": 0,
            "last-updated-ms": 0, "last-column-id": 0, "current-schema-id": 0,
            "schemas": [{"type": "struct", "schema-id": 0, "fields": []}],
            "default-spec-id": 0, "partition-specs": [], "last-partition-id": 0,
            "default-sort-order-id": 0, "sort-orders": [], "properties": {},
        }))
        .unwrap();
    cache
        .put_metadata(alice.scope(), GS_META, Arc::new(metadata))
        .await;
    cache
        .put_scan_files(
            alice.scope(),
            GS_META,
            Arc::new(CachedScanFiles {
                data_files: Arc::new(Vec::new()),
                estimated_row_count: 0,
                files_selected: 0,
                files_pruned: 0,
                has_delete_manifests: false,
            }),
        )
        .await;
    assert!(cache.get_metadata(alice.scope(), GS_META).await.is_some());
    assert!(cache.get_scan_files(alice.scope(), GS_META).await.is_some());
    assert!(cache.get_metadata(bob.scope(), GS_META).await.is_none());
    assert!(cache.get_scan_files(bob.scope(), GS_META).await.is_none());
    assert!(cache
        .get_metadata(&StorageScope::shared(), GS_META)
        .await
        .is_none());
}

#[tokio::test]
async fn graph_source_storage_applies_the_instances_identity_context() {
    let base = crate::FlureeBuilder::memory().build_memory();
    let config = IcebergGsConfig {
        catalog: CatalogConfig::direct("s3://lake/wh/t"),
        table: fluree_db_iceberg::config::TableConfig::Identifier(String::new()),
        io: ambient_io(),
        mapping: None,
        delete: None,
        order_by: None,
        model: None,
        default_allow: None,
    };
    let resolved = base
        .graph_source_storage(&config, &CatalogGrant::direct())
        .await
        .unwrap();
    assert_eq!(resolved.decision, StorageDecision::Ambient);
    assert!(resolved.scope.is_shared());

    let tenant = base.with_host_identity_policy(HostIdentityPolicy::deny_all());
    let err = tenant
        .graph_source_storage(&config, &CatalogGrant::direct())
        .await
        .unwrap_err();
    assert_eq!(err.status_code(), 403, "{err}");
    assert_eq!(
        err.error_type(),
        fluree_vocab::errors::HOST_IDENTITY_REFUSED
    );

    // A grant must match the source: Direct sources have no catalog, and a
    // catalog-backed one is read only with what its catalog granted.
    let rest = IcebergGsConfig {
        catalog: rest_with("https://cat.example", bearer("a")),
        table: fluree_db_iceberg::config::TableConfig::Identifier("ns.t".to_string()),
        io: IoConfig::default(),
        ..config.clone()
    };
    assert!(base
        .graph_source_storage(&rest, &CatalogGrant::direct())
        .await
        .is_err());
    assert!(base
        .graph_source_storage(&config, &CatalogGrant::unvended(S3_META))
        .await
        .is_err());
    let err = base
        .graph_source_storage(&rest, &CatalogGrant::unvended(S3_META))
        .await
        .unwrap_err();
    assert_eq!(
        err.error_type(),
        fluree_vocab::errors::CATALOG_CREDENTIALS_NOT_VENDED,
        "{err}"
    );
}

#[tokio::test]
async fn the_native_aws_catalogs_are_called_as_the_aws_host_identity() {
    let no_aws = with_policy(HostIdentityPolicy::parse(Some("google-wif"), None).unwrap());
    for conn in [
        IcebergConnectionConfig::glue(Some("us-east-1".to_string()), None),
        IcebergConnectionConfig::s3_tables(
            None,
            "arn:aws:s3tables:us-east-1:123456789012:bucket/lake",
        ),
    ] {
        let err = super::super::iceberg_catalog::catalog_client(&conn, "browse", &no_aws)
            .await
            .map(|_| ())
            .unwrap_err();
        assert_eq!(
            err.error_type(),
            fluree_vocab::errors::HOST_IDENTITY_REFUSED,
            "{err}"
        );
    }

    let tenant = crate::FlureeBuilder::memory()
        .build_memory()
        .with_host_identity_policy(HostIdentityPolicy::deny_all());
    let glue = r#"{"catalog":{"type":"glue","region":"us-east-1"},"table":"ns.t","io":{"vended_credentials":false}}"#;
    let config = IcebergGsConfig::from_json(glue).unwrap();
    match super::super::r2rml::shared_catalog_client(&tenant, "g:main", glue, &config).await {
        Err(fluree_db_query::QueryError::HostIdentityRefused { kind, .. }) => {
            assert_eq!(kind, "aws");
        }
        other => panic!("expected HostIdentityRefused, got {:?}", other.map(|_| ())),
    }
}

#[test]
fn a_grant_never_shows_the_credentials_it_carries() {
    let grant = CatalogGrant::from_load(&load(GS_META, Some(gcs_token())), None);
    let shown = format!("{grant:?}");
    assert!(!shown.contains("ya29.vended"), "{shown}");
    assert!(shown.contains("a GCS token"), "{shown}");
}
