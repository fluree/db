//! Databricks Unity Catalog: where a named table lives, and the short-lived
//! credentials that read it.
//!
//! Unity issues credentials per table, scoped to that table's path and valid
//! for about an hour. They are handed to the object store as a
//! [`CredentialProvider`], which the store asks on every request — so a
//! table's store, and everything kept alongside it, outlives any one set.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use delta_kernel::object_store::aws::AwsCredential;
use delta_kernel::object_store::azure::AzureCredential;
use delta_kernel::object_store::{self as object_store, CredentialProvider};
use fluree_db_iceberg::auth::SendCatalogAuth;
use percent_encoding::{percent_decode_str, utf8_percent_encode, AsciiSet, NON_ALPHANUMERIC};
use serde::Deserialize;

use crate::config::UnityConfig;
use crate::error::{DeltaError, Result};

/// Credentials are replaced this long before they expire, so a request that
/// starts with them also finishes with them.
const REFRESH_MARGIN: Duration = Duration::from_secs(300);

const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);

/// Everything but the characters a table name is ordinarily made of.
const NAME: &AsciiSet = &NON_ALPHANUMERIC.remove(b'.').remove(b'_').remove(b'-');

#[derive(Debug)]
pub(crate) struct UnityClient {
    base: String,
    auth: Arc<dyn SendCatalogAuth>,
    http: reqwest::Client,
}

/// A table as Unity places it.
#[derive(Debug, Clone)]
pub(crate) struct UnityTable {
    pub(crate) full_name: String,
    pub(crate) id: String,
    pub(crate) location: String,
}

#[derive(Deserialize)]
struct TableRecord {
    table_id: String,
    #[serde(default)]
    storage_location: Option<String>,
    #[serde(default)]
    data_source_format: Option<String>,
    #[serde(default)]
    table_type: Option<String>,
    #[serde(default)]
    row_filter: Option<serde_json::Value>,
    #[serde(default)]
    columns: Vec<ColumnRecord>,
}

#[derive(Deserialize)]
struct ColumnRecord {
    #[serde(default)]
    mask: Option<serde_json::Value>,
}

#[derive(Deserialize)]
struct TemporaryCredentials {
    #[serde(default)]
    aws_temp_credentials: Option<AwsTemporary>,
    #[serde(default)]
    azure_user_delegation_sas: Option<AzureSas>,
    #[serde(default)]
    expiration_time: Option<u64>,
}

#[derive(Deserialize)]
struct AwsTemporary {
    access_key_id: String,
    secret_access_key: String,
    session_token: String,
}

#[derive(Deserialize)]
struct AzureSas {
    sas_token: String,
}

impl UnityClient {
    /// `config` must be hydrated: an unresolved secret reference fails here.
    pub(crate) fn new(config: &UnityConfig) -> Result<Self> {
        let http = fluree_db_iceberg::net::hardened_client_builder()
            .timeout(REQUEST_TIMEOUT)
            .build()
            .map_err(|e| DeltaError::Config(format!("Unity Catalog client: {e}")))?;
        Self::with_http(config, http)
    }

    fn with_http(config: &UnityConfig, http: reqwest::Client) -> Result<Self> {
        let auth = config
            .auth
            .create_provider_arc()
            .map_err(|e| DeltaError::Config(format!("Unity Catalog auth: {e}")))?;
        Ok(Self {
            base: config.uri.trim_end_matches('/').to_string(),
            auth,
            http,
        })
    }

    /// Where `full_name` lives. Only a Delta table with files of its own is
    /// placed: a view, or a table in another format, is refused by name.
    pub(crate) async fn table(&self, full_name: &str) -> Result<UnityTable> {
        let path = format!(
            "/api/2.1/unity-catalog/tables/{}",
            utf8_percent_encode(full_name, NAME)
        );
        let refuse = |message: String| DeltaError::Catalog {
            table: full_name.to_string(),
            message,
        };
        let record: TableRecord = serde_json::from_value(self.send(full_name, &path, None).await?)
            .map_err(|e| refuse(format!("unreadable table record: {e}")))?;
        let kind = record.table_type.as_deref().unwrap_or("table");
        match record.data_source_format.as_deref() {
            Some(format) if format.eq_ignore_ascii_case("delta") => {}
            Some(format) => {
                return Err(refuse(format!(
                    "is a {kind} in {format} format; only Delta tables are read"
                )))
            }
            None => return Err(refuse(format!("is a {kind}, not a Delta table"))),
        }
        // Unity applies these only in its own compute and issues no
        // credentials for such a table; say so before it is asked.
        let filtered = record.row_filter.is_some();
        let masked = record.columns.iter().any(|c| c.mask.is_some());
        if filtered || masked {
            return Err(refuse(format!(
                "has a {}, which Unity Catalog enforces only in its own compute; it issues no \
                 credentials to read such a table's files",
                if filtered {
                    "row filter"
                } else {
                    "column mask"
                }
            )));
        }
        let location = record
            .storage_location
            .filter(|l| !l.is_empty())
            .ok_or_else(|| refuse("has no storage location".to_string()))?;
        Ok(UnityTable {
            full_name: full_name.to_string(),
            id: record.table_id,
            location,
        })
    }

    async fn read_credentials(&self, table: &UnityTable) -> Result<Vended> {
        let body = serde_json::json!({ "table_id": table.id, "operation": "READ" });
        let refuse = |message: String| DeltaError::Catalog {
            table: table.full_name.clone(),
            message,
        };
        let issued: TemporaryCredentials = serde_json::from_value(
            self.send(
                &table.full_name,
                "/api/2.1/unity-catalog/temporary-table-credentials",
                Some(&body),
            )
            .await?,
        )
        .map_err(|e| refuse(format!("unreadable credentials: {e}")))?;
        let expires = issued
            .expiration_time
            .map(|ms| UNIX_EPOCH + Duration::from_millis(ms))
            .ok_or_else(|| refuse("credentials carry no expiry".to_string()))?;
        let secret = if let Some(aws) = issued.aws_temp_credentials {
            Secret::Aws(Arc::new(AwsCredential {
                key_id: aws.access_key_id,
                secret_key: aws.secret_access_key,
                token: Some(aws.session_token),
            }))
        } else if let Some(azure) = issued.azure_user_delegation_sas {
            Secret::Azure(Arc::new(AzureCredential::SASToken(sas_pairs(
                &azure.sas_token,
            ))))
        } else {
            return Err(refuse(
                "credentials are for a store this reader has none for (S3 and Azure are read)"
                    .to_string(),
            ));
        };
        Ok(Vended { secret, expires })
    }

    /// One authenticated request; a 401 refreshes the catalog token once.
    async fn send(
        &self,
        table: &str,
        path: &str,
        body: Option<&serde_json::Value>,
    ) -> Result<serde_json::Value> {
        let failed = |message: String| DeltaError::Catalog {
            table: table.to_string(),
            message,
        };
        let url = format!("{}{path}", self.base);
        for retried in [false, true] {
            let mut request = match body {
                Some(body) => self.http.post(&url).json(body),
                None => self.http.get(&url),
            };
            if let Some(header) = self
                .auth
                .authorization_header()
                .await
                .map_err(|e| failed(format!("catalog auth: {e}")))?
            {
                request = request.header("Authorization", header);
            }
            let response = request
                .send()
                .await
                .map_err(|e| failed(format!("request failed: {e}")))?;
            let status = response.status();
            if status == reqwest::StatusCode::UNAUTHORIZED && !retried {
                self.auth
                    .refresh()
                    .await
                    .map_err(|e| failed(format!("catalog auth: {e}")))?;
                continue;
            }
            let text = response
                .text()
                .await
                .map_err(|e| failed(format!("unreadable response: {e}")))?;
            if status.is_success() {
                return serde_json::from_str(&text)
                    .map_err(|e| failed(format!("unreadable response: {e}")));
            }
            // Unity's own message names the missing table or privilege.
            let said = serde_json::from_str::<serde_json::Value>(&text)
                .ok()
                .and_then(|v| v.get("message")?.as_str().map(str::to_string))
                .unwrap_or(text);
            return Err(failed(format!("{said} ({status})")));
        }
        unreachable!("the second pass returns")
    }
}

/// `k=v&…` as the decoded pairs the Azure store signs requests with.
fn sas_pairs(token: &str) -> Vec<(String, String)> {
    token
        .trim_start_matches('?')
        .split('&')
        .filter_map(|pair| pair.split_once('='))
        .map(|(k, v)| {
            let decode = |s: &str| percent_decode_str(s).decode_utf8_lossy().into_owned();
            (decode(k), decode(v))
        })
        .collect()
}

#[derive(Clone)]
enum Secret {
    Aws(Arc<AwsCredential>),
    Azure(Arc<AzureCredential>),
}

#[derive(Clone)]
struct Vended {
    secret: Secret,
    expires: SystemTime,
}

impl Vended {
    fn usable(&self) -> bool {
        SystemTime::now() + REFRESH_MARGIN < self.expires
    }
}

/// One table's credentials, asked of Unity again as they near expiry.
pub(crate) struct Vending<C> {
    unity: Arc<UnityClient>,
    table: UnityTable,
    /// Held across a refresh, so concurrent requests share one.
    held: tokio::sync::Mutex<Option<Vended>>,
    pick: fn(&Secret) -> Option<Arc<C>>,
}

impl<C> std::fmt::Debug for Vending<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "UnityCredentials({})", self.table.full_name)
    }
}

impl Vending<AwsCredential> {
    pub(crate) fn aws(unity: Arc<UnityClient>, table: UnityTable) -> Arc<Self> {
        Arc::new(Self {
            unity,
            table,
            held: tokio::sync::Mutex::default(),
            pick: |secret| match secret {
                Secret::Aws(credential) => Some(credential.clone()),
                Secret::Azure(_) => None,
            },
        })
    }
}

impl Vending<AzureCredential> {
    pub(crate) fn azure(unity: Arc<UnityClient>, table: UnityTable) -> Arc<Self> {
        Arc::new(Self {
            unity,
            table,
            held: tokio::sync::Mutex::default(),
            pick: |secret| match secret {
                Secret::Azure(credential) => Some(credential.clone()),
                Secret::Aws(_) => None,
            },
        })
    }
}

#[async_trait]
impl<C: Send + Sync + 'static> CredentialProvider for Vending<C> {
    type Credential = C;

    async fn get_credential(&self) -> object_store::Result<Arc<C>> {
        let refused = |e: DeltaError| object_store::Error::Generic {
            store: "UnityCatalog",
            source: Box::new(e),
        };
        let mut held = self.held.lock().await;
        let vended = match held.as_ref().filter(|v| v.usable()) {
            Some(vended) => vended.clone(),
            None => {
                let vended = self
                    .unity
                    .read_credentials(&self.table)
                    .await
                    .map_err(refused)?;
                tracing::info!(
                    table = %self.table.full_name,
                    renewed = held.is_some(),
                    valid_for_s = vended
                        .expires
                        .duration_since(SystemTime::now())
                        .map_or(0, |d| d.as_secs()),
                    "Unity Catalog issued table credentials"
                );
                *held = Some(vended.clone());
                vended
            }
        };
        (self.pick)(&vended.secret).ok_or_else(|| {
            refused(DeltaError::Catalog {
                table: self.table.full_name.clone(),
                message: format!(
                    "credentials are not for the store at {}",
                    self.table.location
                ),
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::DeltaIoConfig;
    use crate::store::Credentials;
    use delta_kernel::object_store::path::Path;
    use delta_kernel::object_store::ObjectStoreExt as _;
    use fluree_db_iceberg::auth::AuthConfig;
    use fluree_db_iceberg::ConfigValue;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use wiremock::matchers::{body_json, header, method, path, path_regex};
    use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

    const TABLES: &str = "/api/2.1/unity-catalog/tables/main.sales.orders";
    const CREDENTIALS: &str = "/api/2.1/unity-catalog/temporary-table-credentials";

    /// A plain HTTP client: the production one refuses the loopback mock.
    fn client(server: &MockServer) -> Arc<UnityClient> {
        let config = UnityConfig {
            uri: format!("{}/", server.uri()),
            auth: AuthConfig::Bearer {
                token: ConfigValue::literal("catalog-token"),
            },
            catalog: None,
            schema: None,
        };
        Arc::new(UnityClient::with_http(&config, reqwest::Client::new()).unwrap())
    }

    fn orders(location: &str) -> UnityTable {
        UnityTable {
            full_name: "main.sales.orders".to_string(),
            id: "t-1".to_string(),
            location: location.to_string(),
        }
    }

    fn ms_from_now(seconds: u64) -> u64 {
        (SystemTime::now() + Duration::from_secs(seconds))
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }

    /// Issues `KEY1`, `KEY2`, … each valid for `lifetime_s`.
    struct Issuer {
        issued: Arc<AtomicUsize>,
        lifetime_s: u64,
    }

    impl Respond for Issuer {
        fn respond(&self, _: &Request) -> ResponseTemplate {
            let n = self.issued.fetch_add(1, Ordering::SeqCst) + 1;
            ResponseTemplate::new(200)
                .set_delay(Duration::from_millis(50))
                .set_body_json(serde_json::json!({
                    "aws_temp_credentials": {
                        "access_key_id": format!("KEY{n}"),
                        "secret_access_key": "secret",
                        "session_token": format!("SESSION{n}"),
                    },
                    "expiration_time": ms_from_now(self.lifetime_s),
                }))
        }
    }

    async fn issue(server: &MockServer, lifetime_s: u64) -> Arc<AtomicUsize> {
        let issued = Arc::new(AtomicUsize::new(0));
        Mock::given(method("POST"))
            .and(path(CREDENTIALS))
            .and(body_json(
                serde_json::json!({"table_id": "t-1", "operation": "READ"}),
            ))
            .respond_with(Issuer {
                issued: issued.clone(),
                lifetime_s,
            })
            .mount(server)
            .await;
        issued
    }

    async fn serve_table(server: &MockServer, record: serde_json::Value) {
        Mock::given(method("GET"))
            .and(path(TABLES))
            .and(header("Authorization", "Bearer catalog-token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(record))
            .mount(server)
            .await;
    }

    fn message(error: DeltaError) -> String {
        match error {
            DeltaError::Catalog { table, message } => {
                assert_eq!(table, "main.sales.orders");
                message
            }
            other => panic!("expected a catalog error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_delta_table_is_placed_where_unity_says() {
        let server = MockServer::start().await;
        serve_table(
            &server,
            serde_json::json!({
                "table_id": "t-1", "table_type": "MANAGED", "data_source_format": "DELTA",
                "storage_location": "s3://bucket/tables/t-1",
                "row_filter": null, "columns": [{"name": "id"}, {"name": "label", "mask": null}],
            }),
        )
        .await;

        let table = client(&server).table("main.sales.orders").await.unwrap();
        assert_eq!(table.id, "t-1");
        assert_eq!(table.location, "s3://bucket/tables/t-1");
    }

    #[tokio::test]
    async fn what_is_not_a_delta_table_with_files_is_refused_by_name() {
        for (record, expected) in [
            (
                serde_json::json!({"table_id": "t-1", "table_type": "VIEW"}),
                "is a VIEW, not a Delta table",
            ),
            (
                serde_json::json!({
                    "table_id": "t-1", "table_type": "EXTERNAL", "data_source_format": "PARQUET",
                    "storage_location": "s3://bucket/t",
                }),
                "in PARQUET format",
            ),
            (
                serde_json::json!({"table_id": "t-1", "data_source_format": "DELTA"}),
                "no storage location",
            ),
            (
                serde_json::json!({
                    "table_id": "t-1", "data_source_format": "DELTA",
                    "storage_location": "s3://bucket/t",
                    "row_filter": {"function_name": "main.sales.only_mine"},
                }),
                "has a row filter",
            ),
            (
                serde_json::json!({
                    "table_id": "t-1", "data_source_format": "DELTA",
                    "storage_location": "s3://bucket/t",
                    "columns": [{"name": "id"}, {"name": "ssn", "mask": {"function_name": "f"}}],
                }),
                "has a column mask",
            ),
        ] {
            let server = MockServer::start().await;
            serve_table(&server, record).await;
            let said = message(
                client(&server)
                    .table("main.sales.orders")
                    .await
                    .unwrap_err(),
            );
            assert!(said.contains(expected), "{said}");
        }
    }

    #[tokio::test]
    async fn unitys_own_words_reach_the_error() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(TABLES))
            .respond_with(ResponseTemplate::new(404).set_body_json(serde_json::json!({
                "error_code": "TABLE_DOES_NOT_EXIST",
                "message": "Table 'main.sales.orders' does not exist.",
            })))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path(CREDENTIALS))
            .respond_with(ResponseTemplate::new(403).set_body_json(serde_json::json!({
                "message": "User does not have USE CATALOG on Catalog 'main'.",
            })))
            .mount(&server)
            .await;

        let unity = client(&server);
        let said = message(unity.table("main.sales.orders").await.unwrap_err());
        assert!(
            said.contains("does not exist") && said.contains("404"),
            "{said}"
        );

        let vending = Vending::aws(unity, orders("s3://bucket/t"));
        let said = vending.get_credential().await.unwrap_err().to_string();
        assert!(said.contains("USE CATALOG"), "{said}");
    }

    #[tokio::test]
    async fn a_rejected_catalog_token_is_refreshed_once() {
        struct Once(AtomicUsize);
        impl Respond for Once {
            fn respond(&self, _: &Request) -> ResponseTemplate {
                match self.0.fetch_add(1, Ordering::SeqCst) {
                    0 => ResponseTemplate::new(401),
                    _ => ResponseTemplate::new(200).set_body_json(serde_json::json!({
                        "table_id": "t-1", "data_source_format": "DELTA",
                        "storage_location": "s3://bucket/t",
                    })),
                }
            }
        }
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(TABLES))
            .respond_with(Once(AtomicUsize::new(0)))
            .mount(&server)
            .await;
        client(&server).table("main.sales.orders").await.unwrap();

        let always = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(401).set_body_string("no"))
            .mount(&always)
            .await;
        let said = message(
            client(&always)
                .table("main.sales.orders")
                .await
                .unwrap_err(),
        );
        assert!(said.contains("401"), "{said}");
        assert_eq!(always.received_requests().await.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn credentials_are_kept_until_they_near_expiry() {
        let server = MockServer::start().await;
        let issued = issue(&server, 3600).await;
        let vending = Vending::aws(client(&server), orders("s3://bucket/t"));
        for _ in 0..3 {
            assert_eq!(vending.get_credential().await.unwrap().key_id, "KEY1");
        }
        assert_eq!(issued.load(Ordering::SeqCst), 1);

        // Inside the refresh margin, every request asks again.
        let server = MockServer::start().await;
        let issued = issue(&server, REFRESH_MARGIN.as_secs() - 60).await;
        let vending = Vending::aws(client(&server), orders("s3://bucket/t"));
        assert_eq!(vending.get_credential().await.unwrap().key_id, "KEY1");
        let second = vending.get_credential().await.unwrap();
        assert_eq!(second.key_id, "KEY2");
        assert_eq!(second.token.as_deref(), Some("SESSION2"));
        assert_eq!(issued.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn concurrent_requests_share_one_refresh() {
        let server = MockServer::start().await;
        let issued = issue(&server, 3600).await;
        let vending = Vending::aws(client(&server), orders("s3://bucket/t"));
        let all = futures::future::join_all((0..8).map(|_| vending.get_credential())).await;
        assert!(all.iter().all(|c| c.as_ref().unwrap().key_id == "KEY1"));
        assert_eq!(issued.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn a_sas_token_becomes_its_decoded_pairs() {
        assert_eq!(
            sas_pairs("?sv=2022-11-02&sig=a%2Bb%2Fc%3D&sp=rl"),
            [("sv", "2022-11-02"), ("sig", "a+b/c="), ("sp", "rl")]
                .map(|(k, v)| (k.to_string(), v.to_string()))
        );
    }

    #[tokio::test]
    async fn azure_credentials_are_a_sas_and_do_not_pass_for_aws() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(CREDENTIALS))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "azure_user_delegation_sas": {"sas_token": "sv=1&sig=x%3D"},
                "expiration_time": ms_from_now(3600),
            })))
            .mount(&server)
            .await;
        let location = "abfss://c@acct.dfs.core.windows.net/t";

        let azure = Vending::azure(client(&server), orders(location));
        match &*azure.get_credential().await.unwrap() {
            AzureCredential::SASToken(pairs) => {
                assert_eq!(pairs[1], ("sig".to_string(), "x=".to_string()));
            }
            other => panic!("expected a SAS token, got {other:?}"),
        }

        let aws = Vending::aws(client(&server), orders("s3://bucket/t"));
        let said = aws.get_credential().await.unwrap_err().to_string();
        assert!(said.contains("not for the store"), "{said}");
    }

    #[tokio::test]
    async fn credentials_for_a_store_the_reader_lacks_are_refused() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(CREDENTIALS))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "gcp_oauth_token": {"oauth_token": "x"},
                "expiration_time": ms_from_now(3600),
            })))
            .mount(&server)
            .await;
        let vending = Vending::aws(client(&server), orders("s3://bucket/t"));
        let said = vending.get_credential().await.unwrap_err().to_string();
        assert!(said.contains("S3 and Azure are read"), "{said}");
    }

    #[test]
    fn a_catalog_cannot_place_a_table_on_local_disk() {
        let server_less = UnityConfig {
            uri: "https://workspace.example.com".to_string(),
            auth: AuthConfig::Bearer {
                token: ConfigValue::literal("t"),
            },
            catalog: None,
            schema: None,
        };
        let unity = Arc::new(UnityClient::with_http(&server_less, reqwest::Client::new()).unwrap());
        for location in ["/etc", "file:///etc"] {
            let refused = crate::store::open(
                location,
                &DeltaIoConfig::default(),
                Credentials::Unity(unity.clone(), orders(location)),
            )
            .err()
            .unwrap_or_else(|| panic!("{location} was opened"));
            assert!(
                refused.to_string().contains("local filesystem"),
                "{refused}"
            );
        }
    }

    /// The stamp: the requests the S3 store actually sends are signed with
    /// what Unity issued, and with the next set once the first nears expiry.
    #[tokio::test]
    async fn s3_requests_are_signed_with_the_issued_credentials() {
        let server = MockServer::start().await;
        issue(&server, REFRESH_MARGIN.as_secs() - 60).await;
        Mock::given(method("HEAD"))
            .and(path_regex("^/bucket/"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("Content-Length", "3")
                    .insert_header("Last-Modified", "Tue, 01 Sep 2026 00:00:00 GMT")
                    .insert_header("ETag", "\"e\""),
            )
            .mount(&server)
            .await;

        let io = DeltaIoConfig {
            s3_region: Some("us-east-1".to_string()),
            s3_endpoint: Some(server.uri()),
            s3_path_style: true,
            azure: None,
        };
        let location = "s3://bucket/tables/t-1";
        let (_, store) = crate::store::open(
            location,
            &io,
            Credentials::Unity(client(&server), orders(location)),
        )
        .unwrap();
        for _ in 0..2 {
            store
                .head(&Path::from("tables/t-1/_delta_log/0.json"))
                .await
                .unwrap();
        }

        let signed: Vec<(String, String)> = server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .filter(|r| r.method.as_str() == "HEAD")
            .map(|r| {
                let get = |name: &str| r.headers.get(name).unwrap().to_str().unwrap().to_string();
                (get("authorization"), get("x-amz-security-token"))
            })
            .collect();
        assert_eq!(signed.len(), 2);
        assert!(signed[0].0.contains("Credential=KEY1/"), "{}", signed[0].0);
        assert_eq!(signed[0].1, "SESSION1");
        assert!(signed[1].0.contains("Credential=KEY2/"), "{}", signed[1].0);
        assert_eq!(signed[1].1, "SESSION2");
    }
}
