//! A catalog's quota project bills the catalog, not storage reads: a source
//! reading `gs://` with its catalog's Google identity (`storage_auth:
//! catalog`) sends GCS no `x-goog-user-project`, while one with a quota
//! project configured for storage sends that one. Observed live: GCS treats
//! `x-goog-user-project` as a requester-pays billing project, so inheriting
//! the catalog's made every read need `serviceusage.services.use` on it.
//!
//! The whole path, against mocks of Google's metadata server, a BigLake-style
//! REST catalog and GCS, through the storage verification an onboarding
//! "Test" runs (the catalog's `loadTable`, the storage decision, the GCS read).
//!
//! A standalone `[[test]]` target with a single test: the mock endpoints are
//! the operator-only overrides (`FLUREE_GOOGLE_METADATA_URL`,
//! `FLUREE_GCS_ENDPOINT`, `FLUREE_GOOGLE_TOKEN_HOSTS`), which are read once
//! per process, so they are set before anything reads them and no other test
//! shares this process.

#![cfg(feature = "iceberg")]

use fluree_db_api::{
    FlureeBuilder, GoogleCredentials, IcebergAuthConfig, IcebergConnectionConfig, StorageAuth,
};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

const METADATA_PATH: &str = "/computeMetadata/v1/instance/service-accounts/default/token";

/// The verification ended in GCS's refusal of the read (the mock refuses
/// every read), reported or raised.
fn refused_by_gcs(outcome: fluree_db_api::Result<fluree_db_api::StorageAccessReport>) {
    match outcome {
        Ok(report) => assert!(!report.readable, "{report:?}"),
        Err(error) => assert_eq!(
            error.error_type(),
            fluree_vocab::errors::STORAGE_ACCESS_DENIED,
            "{error}"
        ),
    }
}

fn user_projects(requests: &[Request]) -> Vec<Option<String>> {
    requests
        .iter()
        .map(|r| {
            r.headers
                .get("x-goog-user-project")
                .map(|v| v.to_str().unwrap().to_string())
        })
        .collect()
}

#[tokio::test]
async fn storage_reads_bill_only_a_project_configured_for_storage() {
    let (google, catalog, gcs) = (
        MockServer::start().await,
        MockServer::start().await,
        MockServer::start().await,
    );
    std::env::set_var(
        "FLUREE_GOOGLE_METADATA_URL",
        format!("{}{METADATA_PATH}", google.uri()),
    );
    std::env::set_var("FLUREE_GCS_ENDPOINT", gcs.uri());
    // The mock catalog and GCS are on loopback; a host identity's token goes
    // only to Google's APIs or a host the operator lists.
    std::env::set_var("FLUREE_GOOGLE_TOKEN_HOSTS", "127.0.0.1");

    Mock::given(method("GET"))
        .and(path(METADATA_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "access_token": "ya29.host", "expires_in": 3600, "token_type": "Bearer"
        })))
        .mount(&google)
        .await;
    Mock::given(method("GET"))
        .and(path("/v1/config"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .mount(&catalog)
        .await;
    Mock::given(method("GET"))
        .and(path("/v1/namespaces/db/tables/t"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "metadata-location": "gs://lake/wh/t/metadata/v1.metadata.json"
        })))
        .mount(&catalog)
        .await;
    // Every GCS read is refused: the request's headers are what is checked.
    Mock::given(method("GET"))
        .respond_with(
            ResponseTemplate::new(403).set_body_string(
                "<Error><Code>AccessDenied</Code><Message>denied</Message></Error>",
            ),
        )
        .mount(&gcs)
        .await;

    let fluree = FlureeBuilder::memory().build_memory();
    let catalog_auth = IcebergAuthConfig::Google {
        credentials: GoogleCredentials::Metadata,
        scopes: None,
        quota_project: Some("catalog-billing".to_string()),
    };
    let connection = |storage_auth: StorageAuth| {
        IcebergConnectionConfig::rest(catalog.uri())
            .with_auth(catalog_auth.clone())
            .with_storage_auth(storage_auth)
    };

    // The catalog's identity reads storage: the catalog's calls bill to its
    // quota project, the storage reads to none.
    refused_by_gcs(
        fluree
            .verify_iceberg_storage_access(connection(StorageAuth::Catalog), "db.t")
            .await,
    );
    let to_catalog = catalog.received_requests().await.unwrap();
    assert!(!to_catalog.is_empty());
    assert!(
        user_projects(&to_catalog)
            .iter()
            .all(|p| p.as_deref() == Some("catalog-billing")),
        "{:?}",
        user_projects(&to_catalog)
    );
    let to_gcs = gcs.received_requests().await.unwrap();
    assert!(!to_gcs.is_empty(), "the metadata file was read from GCS");
    assert!(
        user_projects(&to_gcs).iter().all(Option::is_none),
        "{:?}",
        user_projects(&to_gcs)
    );

    // A quota project configured for storage bills the storage reads.
    let gcs_seen = to_gcs.len();
    refused_by_gcs(
        fluree
            .verify_iceberg_storage_access(
                connection(StorageAuth::Google {
                    credentials: GoogleCredentials::Metadata,
                    quota_project: Some("storage-billing".to_string()),
                }),
                "db.t",
            )
            .await,
    );
    let to_gcs = gcs.received_requests().await.unwrap();
    let billed = user_projects(&to_gcs[gcs_seen..]);
    assert!(!billed.is_empty(), "the metadata file was read from GCS");
    assert!(
        billed
            .iter()
            .all(|p| p.as_deref() == Some("storage-billing")),
        "{billed:?}"
    );
}
