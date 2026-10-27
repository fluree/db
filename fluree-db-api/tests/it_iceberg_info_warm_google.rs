//! A BigQuery table's row count, read from its manifest list, costs a warm
//! `/info` no remote call: no GCS read and no Google token mint, though the
//! cache is keyed by the storage identity's scope, which a Google identity
//! proves with a token. The table's snapshot summary is only
//! `{"operation": "overwrite"}`, as BigQuery writes it, so the count can only
//! come from the manifest list.
//!
//! The whole path, against mocks of Google's metadata server, a BigLake-style
//! REST catalog that returns the metadata inline, and GCS: a source reading
//! `gs://` with its catalog's Google identity (`storage_auth: catalog`), as a
//! BigLake federation (`bq://`) source does.
//!
//! A standalone `[[test]]` target with a single test: the mock endpoints are
//! the operator-only overrides (`FLUREE_GOOGLE_METADATA_URL`,
//! `FLUREE_GCS_ENDPOINT`, `FLUREE_GOOGLE_TOKEN_HOSTS`), which are read once
//! per process, so they are set before anything reads them and no other test
//! shares this process.

#![cfg(feature = "iceberg")]

use fluree_db_api::{
    FlureeBuilder, GoogleCredentials, IcebergAuthConfig, IcebergConnectionConfig,
    IcebergCreateConfig, StatsTier, StorageAuth, TableIdentifier,
};
use fluree_db_nameservice::{GraphSourceRecord, GraphSourceType};
use wiremock::matchers::{method, path};
use wiremock::{Match, Mock, MockServer, Request, ResponseTemplate};

const METADATA_PATH: &str = "/computeMetadata/v1/instance/service-accounts/default/token";
const LOAD_TABLE_PATH: &str = "/v1/namespaces/db/tables/big";
const MANIFEST_LIST_KEY: &str = "wh/big/metadata/snap-1.avro";
const DEVSTORAGE_READ_ONLY: &str = "https://www.googleapis.com/auth/devstorage.read_only";

/// The BigQuery table's metadata (summary `{"operation": "overwrite"}`) and
/// its manifest list, which states 2,000,000 rows.
const BQ_METADATA: &[u8] =
    include_bytes!("../../fluree-db-iceberg/tests/fixtures/bigquery/big.metadata.json");
const BQ_MANIFEST_LIST: &[u8] =
    include_bytes!("../../fluree-db-iceberg/tests/fixtures/bigquery/big.manifest-list.avro");

/// Matches a GET of `/{bucket}/{object}`. `object_store` percent-encodes the
/// whole object name (`/`, `-` and `.` included), which GCS decodes, so
/// compare the decoded path.
struct ObjectAt(String);

impl Match for ObjectAt {
    fn matches(&self, request: &Request) -> bool {
        percent_decode(request.url.path()).is_some_and(|p| p == self.0)
    }
}

fn percent_decode(s: &str) -> Option<String> {
    let mut out = Vec::with_capacity(s.len());
    let mut bytes = s.bytes();
    while let Some(b) = bytes.next() {
        if b == b'%' {
            let hex = [bytes.next()?, bytes.next()?];
            out.push(u8::from_str_radix(std::str::from_utf8(&hex).ok()?, 16).ok()?);
        } else {
            out.push(b);
        }
    }
    String::from_utf8(out).ok()
}

/// Requests made since `seen` of them were counted.
async fn since(server: &MockServer, seen: usize) -> Vec<Request> {
    server.received_requests().await.unwrap()[seen..].to_vec()
}

fn storage_token_mints(requests: &[Request]) -> usize {
    requests
        .iter()
        .filter(|r| {
            r.url
                .query_pairs()
                .any(|(k, v)| k == "scopes" && v.contains(DEVSTORAGE_READ_ONLY))
        })
        .count()
}

#[tokio::test]
async fn a_warm_info_reads_no_storage_and_mints_no_token() {
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
    // A BigLake REST `loadTable`: the metadata inline, its manifest list moved
    // to the mock bucket.
    let mut metadata: serde_json::Value = serde_json::from_slice(BQ_METADATA).unwrap();
    metadata["location"] = serde_json::json!("gs://lake/wh/big");
    metadata["snapshots"][0]["manifest-list"] =
        serde_json::json!(format!("gs://lake/{MANIFEST_LIST_KEY}"));
    assert_eq!(
        metadata["snapshots"][0]["summary"],
        serde_json::json!({"operation": "overwrite"}),
        "precondition: the summary carries no totals"
    );
    Mock::given(method("GET"))
        .and(path(LOAD_TABLE_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "metadata-location": "gs://lake/wh/big/metadata/v1.metadata.json",
            "metadata": metadata,
        })))
        .mount(&catalog)
        .await;
    Mock::given(method("GET"))
        .and(ObjectAt(format!("/lake/{MANIFEST_LIST_KEY}")))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("ETag", "\"etag-1\"")
                .insert_header("Last-Modified", "Wed, 21 Oct 2026 07:28:00 GMT")
                .set_body_bytes(BQ_MANIFEST_LIST.to_vec()),
        )
        .mount(&gcs)
        .await;

    let fluree = FlureeBuilder::memory().build_memory();
    let connection = IcebergConnectionConfig::rest(catalog.uri())
        .with_auth(IcebergAuthConfig::Google {
            credentials: GoogleCredentials::Metadata,
            scopes: None,
            quota_project: None,
        })
        .with_storage_auth(StorageAuth::Catalog);
    let config = IcebergCreateConfig::from_connection("bq-big", connection.clone(), "db.big")
        .to_iceberg_gs_config()
        .to_json()
        .unwrap();
    let record = GraphSourceRecord::new("bq-big", "main", GraphSourceType::Iceberg, config, vec![]);

    let row_count =
        |info: &serde_json::Value| info["source"]["table-row-counts"]["db.big"].as_i64();

    // Cold: the count is read from the manifest list, once.
    let info = fluree_db_api::ledger_info::build_graph_source_info(&fluree, &record)
        .await
        .unwrap();
    assert_eq!(row_count(&info), Some(2_000_000), "{info}");
    let gcs_reads = since(&gcs, 0).await;
    assert_eq!(gcs_reads.len(), 1, "one read: the manifest list");
    assert!(
        storage_token_mints(&since(&google, 0).await) >= 1,
        "the cold plan proved the storage identity with a token"
    );

    // Warm: the same count, and no remote call but the catalog's loadTable.
    let (google_seen, catalog_seen, gcs_seen) = (
        google.received_requests().await.unwrap().len(),
        catalog.received_requests().await.unwrap().len(),
        gcs.received_requests().await.unwrap().len(),
    );
    let info = fluree_db_api::ledger_info::build_graph_source_info(&fluree, &record)
        .await
        .unwrap();
    assert_eq!(row_count(&info), Some(2_000_000), "{info}");
    assert_eq!(since(&gcs, gcs_seen).await.len(), 0, "no GCS read");
    let tokens = since(&google, google_seen).await;
    assert!(
        tokens.is_empty(),
        "no token minted: {:?}",
        tokens.iter().map(|r| r.url.to_string()).collect::<Vec<_>>()
    );
    // `/info` asks the catalog for the table's current snapshot on every
    // call, as it did before manifest-list counts; that is the only call.
    let to_catalog: Vec<String> = since(&catalog, catalog_seen)
        .await
        .iter()
        .map(|r| r.url.path().to_string())
        .collect();
    assert_eq!(to_catalog, vec![LOAD_TABLE_PATH.to_string()]);

    // The Schema-tier preview of the same snapshot, under the same identity,
    // is served from the same cache: the totals with no GCS read and no
    // storage token.
    let (google_seen, gcs_seen) = (
        google.received_requests().await.unwrap().len(),
        gcs.received_requests().await.unwrap().len(),
    );
    let preview = fluree
        .preview_iceberg_table(
            connection,
            TableIdentifier::new("db", "big"),
            StatsTier::Schema,
        )
        .await
        .unwrap();
    assert_eq!(preview.schema.row_count, Some(2_000_000));
    assert_eq!(preview.schema.data_file_count, Some(4));
    assert_eq!(since(&gcs, gcs_seen).await.len(), 0, "no GCS read");
    assert_eq!(
        storage_token_mints(&since(&google, google_seen).await),
        0,
        "no storage token minted"
    );
}
