//! The GCS reader against a wiremock GCS (XML API) and a wiremock Google
//! metadata server, so every behavior runs in CI with no network.
//!
//! `object_store`'s GCS backend reads objects at `{endpoint}/{bucket}/{key}`
//! and lists at `{endpoint}/{bucket}?list-type=2`; responses need `ETag` and
//! `Last-Modified`, which it requires of GCS.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use wiremock::matchers::{header, method, path, query_param};
use wiremock::{Match, Mock, MockServer, Request, Respond, ResponseTemplate};

use super::*;
use crate::auth::google::{parse_token_hosts, GoogleEndpoints};
use crate::auth::{GoogleCredentials, IdentityContext};
use crate::credential::{GcsCredentials, VendedCredentialSource};

const METADATA_PATH: &str = "/computeMetadata/v1/instance/service-accounts/default/token";
const LAST_MODIFIED: &str = "Wed, 21 Oct 2026 07:28:00 GMT";

/// Google's token endpoints at the mock `google`, with the mock GCS host
/// listed as a token destination, as an operator lists one in
/// `FLUREE_GOOGLE_TOKEN_HOSTS`: the metadata identity these tests read as is a
/// host identity, whose token goes nowhere else.
fn endpoints_at(google: &MockServer) -> GoogleEndpoints {
    GoogleEndpoints {
        token_hosts: parse_token_hosts(Some("127.0.0.1")),
        ..GoogleEndpoints::at(&google.uri())
    }
}

/// A metadata identity whose token server is `google`, minting `token`.
async fn identity(google: &MockServer, token: &str) -> Arc<GoogleIdentity> {
    Mock::given(method("GET"))
        .and(path(METADATA_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "access_token": token, "expires_in": 3600, "token_type": "Bearer"
        })))
        .mount(google)
        .await;
    Arc::new(
        GoogleIdentity::with_endpoints(
            &GoogleCredentials::Metadata,
            None,
            &IdentityContext::default(),
            &endpoints_at(google),
        )
        .unwrap(),
    )
}

fn store_as(
    identity: Arc<GoogleIdentity>,
    gcs: &MockServer,
    project: Option<&str>,
) -> GcsIcebergStorage {
    GcsIcebergStorage::at_endpoint(
        TokenSource::Identity(identity),
        project.map(str::to_string),
        &gcs.uri(),
    )
}

/// An object body as GCS sends it.
fn object(body: &[u8]) -> ResponseTemplate {
    ResponseTemplate::new(200)
        .insert_header("ETag", "\"etag-1\"")
        .insert_header("Last-Modified", LAST_MODIFIED)
        .set_body_bytes(body.to_vec())
}

/// `range` of `body` as a 206, the way GCS answers a ranged GET.
fn partial(body: &[u8], range: std::ops::Range<usize>) -> ResponseTemplate {
    ResponseTemplate::new(206)
        .insert_header("ETag", "\"etag-1\"")
        .insert_header("Last-Modified", LAST_MODIFIED)
        .insert_header(
            "Content-Range",
            format!("bytes {}-{}/{}", range.start, range.end - 1, body.len()).as_str(),
        )
        .set_body_bytes(body[range].to_vec())
}

fn bearer(request: &Request) -> Option<String> {
    request
        .headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string)
}

const BODY: &[u8] = b"0123456789";

/// Matches a request for `/{bucket}/{object}`. `object_store` percent-encodes
/// the whole object name (`/` as `%2F`), which GCS decodes, so compare the
/// decoded path.
struct ObjectAt(&'static str);

impl Match for ObjectAt {
    fn matches(&self, request: &Request) -> bool {
        urlencoding::decode(request.url.path()).is_ok_and(|p| p == self.0)
    }
}

fn object_at(route: &'static str) -> ObjectAt {
    ObjectAt(route)
}

#[tokio::test]
async fn a_ranged_read_returns_exactly_the_range_as_the_identity() {
    let (google, gcs) = (MockServer::start().await, MockServer::start().await);
    let store = store_as(
        identity(&google, "ya29.storage").await,
        &gcs,
        Some("lake-billing"),
    );
    Mock::given(method("GET"))
        .and(object_at("/lake/data/f.parquet"))
        .and(header("range", "bytes=2-5"))
        .and(header("authorization", "Bearer ya29.storage"))
        .and(header("x-goog-user-project", "lake-billing"))
        .respond_with(partial(BODY, 2..6))
        .expect(1)
        .mount(&gcs)
        .await;
    let bytes = SendIcebergStorage::read_range(&store, "gs://lake/data/f.parquet", 2..6)
        .await
        .unwrap();
    assert_eq!(&bytes[..], b"2345");

    // The storage token is minted for read-only storage, not cloud-platform.
    let minted = google.received_requests().await.unwrap();
    assert_eq!(minted.len(), 1);
    assert!(
        minted[0]
            .url
            .query_pairs()
            .any(|(k, v)| k == "scopes" && v == DEVSTORAGE_READ_ONLY_SCOPE),
        "{}",
        minted[0].url
    );
}

#[tokio::test]
async fn ranges_come_back_in_input_order_and_whole_reads_are_whole() {
    let (google, gcs) = (MockServer::start().await, MockServer::start().await);
    let store = store_as(identity(&google, "t").await, &gcs, None);
    /// Serve whatever single range was asked for.
    struct Ranged;
    impl Respond for Ranged {
        fn respond(&self, request: &Request) -> ResponseTemplate {
            let Some(range) = request.headers.get("range").and_then(|v| v.to_str().ok()) else {
                return object(BODY);
            };
            let (start, end) = range
                .trim_start_matches("bytes=")
                .split_once('-')
                .map(|(s, e)| (s.parse::<usize>().unwrap(), e.parse::<usize>().unwrap()))
                .unwrap();
            partial(BODY, start..end + 1)
        }
    }
    Mock::given(method("GET"))
        .and(object_at("/lake/f"))
        .respond_with(Ranged)
        .mount(&gcs)
        .await;
    let out = SendIcebergStorage::read_ranges(&store, "gs://lake/f", vec![7..9, 0..2, 4..5])
        .await
        .unwrap();
    let out: Vec<&[u8]> = out.iter().map(|b| &b[..]).collect();
    assert_eq!(out, vec![&b"78"[..], b"01", b"4"]);
    assert_eq!(
        &SendIcebergStorage::read(&store, "gs://lake/f")
            .await
            .unwrap()[..],
        BODY
    );
}

#[tokio::test]
async fn a_range_answered_with_anything_but_that_range_is_an_error_not_data() {
    let (google, gcs) = (MockServer::start().await, MockServer::start().await);
    let store = store_as(identity(&google, "t").await, &gcs, None);
    // A 200 with the whole object (a server or proxy that ignored Range).
    Mock::given(method("GET"))
        .and(object_at("/lake/whole"))
        .respond_with(object(BODY))
        .mount(&gcs)
        .await;
    // A 206 for some other range.
    Mock::given(method("GET"))
        .and(object_at("/lake/shifted"))
        .respond_with(partial(BODY, 3..7))
        .mount(&gcs)
        .await;
    for key in ["whole", "shifted"] {
        let err = SendIcebergStorage::read_range(&store, &format!("gs://lake/{key}"), 2..6)
            .await
            .expect_err("the wrong bytes must not be returned as the range");
        assert!(matches!(err, IcebergError::Storage(_)), "{key}: {err:?}");
    }
}

#[tokio::test]
async fn head_reports_the_objects_size() {
    let (google, gcs) = (MockServer::start().await, MockServer::start().await);
    let store = store_as(identity(&google, "t").await, &gcs, None);
    Mock::given(method("HEAD"))
        .and(object_at("/lake/data/f.parquet"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("ETag", "\"etag-1\"")
                .insert_header("Last-Modified", LAST_MODIFIED)
                .insert_header("Content-Length", "4096"),
        )
        .expect(1)
        .mount(&gcs)
        .await;
    assert_eq!(
        SendIcebergStorage::file_size(&store, "gs://lake/data/f.parquet")
            .await
            .unwrap(),
        4096
    );
}

const LISTING: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://doc.s3.amazonaws.com/2006-03-01">
  <Name>lake</Name>
  <Prefix>warehouse/</Prefix>
  <Contents>
    <Key>warehouse/version-hint.text</Key>
    <LastModified>2026-10-21T07:28:00.000Z</LastModified>
    <ETag>"etag-1"</ETag>
    <Size>1</Size>
  </Contents>
  <CommonPrefixes><Prefix>warehouse/orders/</Prefix></CommonPrefixes>
  <CommonPrefixes><Prefix>warehouse/customers/</Prefix></CommonPrefixes>
</ListBucketResult>"#;

#[tokio::test]
async fn a_listing_is_one_delimited_list_of_children() {
    let (google, gcs) = (MockServer::start().await, MockServer::start().await);
    let store = store_as(identity(&google, "t").await, &gcs, None);
    Mock::given(method("GET"))
        .and(path("/lake"))
        .and(query_param("list-type", "2"))
        .and(query_param("delimiter", "/"))
        .and(query_param("prefix", "warehouse/"))
        .respond_with(ResponseTemplate::new(200).set_body_string(LISTING))
        .expect(2)
        .mount(&gcs)
        .await;
    assert_eq!(
        SendIcebergStorage::list_dir(&store, "gs://lake/warehouse")
            .await
            .unwrap(),
        vec!["customers".to_string(), "orders".to_string()]
    );
    assert_eq!(
        SendIcebergStorage::list_files(&store, "gs://lake/warehouse/")
            .await
            .unwrap(),
        vec!["version-hint.text".to_string()]
    );
}

/// Mints `t1`, then `t2`, … one per request.
struct Minting(AtomicUsize);

impl Respond for Minting {
    fn respond(&self, _: &Request) -> ResponseTemplate {
        let n = self.0.fetch_add(1, Ordering::SeqCst) + 1;
        ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "access_token": format!("t{n}"), "expires_in": 3600, "token_type": "Bearer"
        }))
    }
}

#[tokio::test]
async fn a_401_renews_the_token_and_retries_once() {
    let (google, gcs) = (MockServer::start().await, MockServer::start().await);
    Mock::given(method("GET"))
        .and(path(METADATA_PATH))
        .respond_with(Minting(AtomicUsize::new(0)))
        .mount(&google)
        .await;
    let identity = Arc::new(
        GoogleIdentity::with_endpoints(
            &GoogleCredentials::Metadata,
            None,
            &IdentityContext::default(),
            &endpoints_at(&google),
        )
        .unwrap(),
    );
    let store = store_as(identity, &gcs, None);
    // GCS has revoked t1 and accepts t2.
    Mock::given(method("GET"))
        .and(object_at("/lake/f"))
        .and(header("authorization", "Bearer t2"))
        .respond_with(object(BODY))
        .mount(&gcs)
        .await;
    Mock::given(method("GET"))
        .and(object_at("/lake/f"))
        .respond_with(ResponseTemplate::new(401))
        .mount(&gcs)
        .await;
    assert_eq!(
        &SendIcebergStorage::read(&store, "gs://lake/f")
            .await
            .unwrap()[..],
        BODY
    );
    let tokens: Vec<_> = gcs
        .received_requests()
        .await
        .unwrap()
        .iter()
        .filter_map(bearer)
        .collect();
    assert_eq!(tokens, vec!["Bearer t1", "Bearer t2"]);

    // A refusal that survives the renewed token is a denial, after exactly one
    // retry.
    Mock::given(method("GET"))
        .and(object_at("/lake/never"))
        .respond_with(ResponseTemplate::new(401))
        .mount(&gcs)
        .await;
    let err = SendIcebergStorage::read(&store, "gs://lake/never")
        .await
        .unwrap_err();
    assert!(
        matches!(err, IcebergError::StorageAccessDenied { .. }),
        "{err:?}"
    );
    let attempts = gcs
        .received_requests()
        .await
        .unwrap()
        .iter()
        .filter(|r| ObjectAt("/lake/never").matches(r))
        .count();
    assert_eq!(attempts, 2);
}

#[tokio::test]
async fn a_403_is_storage_access_denied_on_reads_and_listings() {
    let (google, gcs) = (MockServer::start().await, MockServer::start().await);
    let store = store_as(identity(&google, "t").await, &gcs, None);
    Mock::given(method("GET"))
        .and(object_at("/lake/data/secret.parquet"))
        .respond_with(ResponseTemplate::new(403).set_body_string(
            "<Error><Code>AccessDenied</Code><Message>no storage.objects.get</Message></Error>",
        ))
        .mount(&gcs)
        .await;
    Mock::given(method("GET"))
        .and(path("/lake"))
        .respond_with(ResponseTemplate::new(403))
        .mount(&gcs)
        .await;
    match SendIcebergStorage::read(&store, "gs://lake/data/secret.parquet").await {
        Err(IcebergError::StorageAccessDenied {
            bucket,
            key,
            message,
            ..
        }) => {
            assert_eq!(bucket, "lake");
            assert_eq!(key, "data/secret.parquet");
            // The operation, the path, the status, and GCS's own code and
            // message, once: not the nested chain, not the XML.
            assert_eq!(
                message,
                "GCS refused the read of gs://lake/data/secret.parquet as Google (metadata): \
                 HTTP 403 AccessDenied: no storage.objects.get"
            );
        }
        other => panic!("expected StorageAccessDenied, got {other:?}"),
    }
    let err = SendIcebergStorage::list_dir(&store, "gs://lake/warehouse")
        .await
        .unwrap_err();
    match &err {
        IcebergError::StorageAccessDenied { message, .. } => assert_eq!(
            message,
            "GCS refused the listing of gs://lake/warehouse as Google (metadata): HTTP 403"
        ),
        other => panic!("expected StorageAccessDenied, got {other:?}"),
    }
    assert!(
        err.to_string().starts_with("Storage access denied"),
        "{err}"
    );
}

/// What GCS answered when it refused a request for the bill's project (a
/// requester-pays or `x-goog-user-project` read without
/// `serviceusage.services.use`), as observed live: the denial says it once,
/// in its own words.
#[tokio::test]
async fn a_user_project_refusal_says_once_what_gcs_said() {
    let (google, gcs) = (MockServer::start().await, MockServer::start().await);
    let store = store_as(identity(&google, "t").await, &gcs, Some("billing-project"));
    Mock::given(method("GET"))
        .and(object_at("/lake/wh/t/metadata/v1.metadata.json"))
        .respond_with(ResponseTemplate::new(403).set_body_string(
            "<?xml version='1.0' encoding='UTF-8'?><Error><Code>UserProjectAccessDenied</Code>\
             <Message>Requester does not have serviceusage.services.use permissions on user \
             project.</Message><Details>reader@lake-project.iam.gserviceaccount.com does not \
             have serviceusage.services.use access to the Google Cloud project. Permission \
             &apos;serviceusage.services.use&apos; denied on resource (or it may not \
             exist).</Details></Error>",
        ))
        .mount(&gcs)
        .await;
    let err = SendIcebergStorage::read(&store, "gs://lake/wh/t/metadata/v1.metadata.json")
        .await
        .unwrap_err();
    let shown = err.to_string();
    assert!(
        shown.starts_with("Storage access denied reading gs://lake/wh/t/metadata/v1.metadata.json"),
        "{shown}"
    );
    assert!(
        shown.contains(
            "HTTP 403 UserProjectAccessDenied: Requester does not have serviceusage.services.use \
             permissions on user project."
        ),
        "{shown}"
    );
    assert_eq!(
        shown.matches("Requester does not have").count(),
        1,
        "{shown}"
    );
    assert!(!shown.contains("<Error>"), "{shown}");
    assert!(!shown.contains("Details"), "{shown}");
}

#[tokio::test]
async fn a_path_this_identity_does_not_serve_is_refused_before_any_request() {
    let (google, gcs) = (MockServer::start().await, MockServer::start().await);
    let store = store_as(identity(&google, "t").await, &gcs, None);
    for path in ["s3://lake/f", "s3a://lake/f", "file:///tmp/f", "/tmp/f"] {
        let err = SendIcebergStorage::read(&store, path).await.unwrap_err();
        assert!(
            matches!(err, IcebergError::SchemeNotServed { .. }),
            "{path}: {err:?}"
        );
    }
    assert!(gcs.received_requests().await.unwrap().is_empty());
    assert!(google.received_requests().await.unwrap().is_empty());
}

#[test]
fn a_store_that_names_no_valid_project_is_refused() {
    let source = TokenSource::Vended(Arc::new(RenewingVendedCredentials::new(
        VendedCredentials::Gcs(GcsCredentials {
            token: "t".to_string(),
            expires_at: chrono::Utc::now() + chrono::Duration::hours(1),
            user_project: None,
        }),
        None,
    )));
    assert!(
        GcsIcebergStorage::build(source, Some("bad\r\nheader".to_string()), "http://x").is_err()
    );
}

/// A host identity's token goes only to Google's APIs or a host the operator
/// listed: a store whose endpoint is neither is refused when it is built,
/// typed, and no token is ever minted for it. At Google's own endpoint the
/// same identity builds; a vended token is the catalog's to scope (the vended
/// tests below read through an unlisted mock host).
#[tokio::test]
async fn a_host_identitys_token_never_goes_to_an_unlisted_endpoint() {
    let (google, gcs) = (MockServer::start().await, MockServer::start().await);
    Mock::given(method("GET"))
        .and(path(METADATA_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "access_token": "ya29.host", "expires_in": 3600, "token_type": "Bearer"
        })))
        .expect(0)
        .mount(&google)
        .await;
    let unlisted = Arc::new(
        GoogleIdentity::with_endpoints(
            &GoogleCredentials::Metadata,
            None,
            &IdentityContext::default(),
            &GoogleEndpoints::at(&google.uri()),
        )
        .unwrap(),
    );
    match GcsIcebergStorage::build(
        TokenSource::Identity(Arc::clone(&unlisted)),
        None,
        &gcs.uri(),
    ) {
        Err(IcebergError::GoogleTokenHostRefused { kind, host }) => {
            assert_eq!(kind, crate::host_identity::HostIdentityKind::GoogleMetadata);
            assert_eq!(host, "127.0.0.1");
        }
        Err(other) => panic!("expected GoogleTokenHostRefused, got {other:?}"),
        Ok(_) => panic!("a store sending a host token to an unlisted host was built"),
    }
    assert!(
        GcsIcebergStorage::build(TokenSource::Identity(unlisted), None, GCS_DEFAULT_ENDPOINT)
            .is_ok()
    );
    // `expect(0)` on the token server is verified when `google` drops.
}

/// A store keeps one client per bucket it reads, reused, and at most
/// [`MAX_BUCKET_CLIENTS`] of them: the least recently used is dropped (and
/// rebuilt if read again).
#[test]
fn a_store_keeps_a_bounded_set_of_bucket_clients() {
    let source = TokenSource::Vended(Arc::new(RenewingVendedCredentials::new(
        VendedCredentials::Gcs(GcsCredentials {
            token: "t".to_string(),
            expires_at: chrono::Utc::now() + chrono::Duration::hours(1),
            user_project: None,
        }),
        None,
    )));
    let storage = GcsIcebergStorage::build(source, None, "http://127.0.0.1:9").unwrap();
    let first = storage.store("bucket-0").unwrap();
    assert!(Arc::ptr_eq(&first, &storage.store("bucket-0").unwrap()));
    for i in 1..=MAX_BUCKET_CLIENTS {
        storage.store(&format!("bucket-{i}")).unwrap();
    }
    let held = storage
        .inner
        .stores
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .len();
    assert_eq!(held, MAX_BUCKET_CLIENTS);
    assert!(
        !Arc::ptr_eq(&first, &storage.store("bucket-0").unwrap()),
        "the least recently used client was dropped"
    );
}

// ── vended tokens renewed inside the store ──

/// A vended-token GCS: accepts `t1` until `t1` expires, `t2` always, and
/// counts every refusal.
struct ExpiringGcs {
    t1_expired: Arc<AtomicBool>,
    refusals: Arc<AtomicUsize>,
}

impl Respond for ExpiringGcs {
    fn respond(&self, request: &Request) -> ResponseTemplate {
        match bearer(request).as_deref() {
            Some("Bearer t2") => object(BODY),
            Some("Bearer t1") if !self.t1_expired.load(Ordering::SeqCst) => object(BODY),
            _ => {
                self.refusals.fetch_add(1, Ordering::SeqCst);
                ResponseTemplate::new(401)
            }
        }
    }
}

fn gcs_token(token: &str, expires_in: chrono::Duration) -> VendedCredentials {
    VendedCredentials::Gcs(GcsCredentials {
        token: token.to_string(),
        expires_at: chrono::Utc::now() + expires_in,
        user_project: None,
    })
}

#[tokio::test]
async fn a_long_scan_crossing_the_vended_tokens_expiry_keeps_reading() {
    // The catalog's per-table credentials endpoint re-vends t2, through the real
    // REST client and `CatalogVendedSource`.
    let (catalog, gcs) = (MockServer::start().await, MockServer::start().await);
    let warehouse = format!("wh-renew-{}", std::process::id());
    Mock::given(method("GET"))
        .and(path("/v1/config"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&catalog)
        .await;
    let expires = (chrono::Utc::now() + chrono::Duration::hours(1)).timestamp_millis();
    Mock::given(method("GET"))
        .and(path(format!("/v1/{warehouse}/namespaces/ns/tables/t/credentials")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "storage-credentials": [{
                "prefix": "gs://lake/ns/t",
                "config": {"gcs.oauth2.token": "t2", "gcs.oauth2.token-expires-at": expires.to_string()}
            }]
        })))
        .expect(1)
        .mount(&catalog)
        .await;
    let client = Arc::new(crate::catalog::RestCatalogClient::for_mock(
        &catalog.uri(),
        Some(&warehouse),
        Arc::new(crate::auth::NoAuth),
    ));
    let source = Arc::new(crate::credential::CatalogVendedSource::new(
        client,
        crate::catalog::TableIdentifier {
            namespace: "ns".to_string(),
            table: "t".to_string(),
        },
        "gs://lake/ns/t/metadata/v1.metadata.json",
    ));
    let renewing = Arc::new(
        RenewingVendedCredentials::new(
            gcs_token("t1", chrono::Duration::milliseconds(4_000)),
            Some(source),
        )
        .with_margin(std::time::Duration::from_millis(2_000)),
    );

    let t1_expired = Arc::new(AtomicBool::new(false));
    let refusals = Arc::new(AtomicUsize::new(0));
    Mock::given(method("GET"))
        .and(object_at("/lake/ns/t/data/f.parquet"))
        .respond_with(ExpiringGcs {
            t1_expired: Arc::clone(&t1_expired),
            refusals: Arc::clone(&refusals),
        })
        .mount(&gcs)
        .await;
    let store = GcsIcebergStorage::at_endpoint(TokenSource::Vended(renewing), None, &gcs.uri());
    let file = "gs://lake/ns/t/data/f.parquet";

    // Early in the scan: the vended t1 is used as held.
    SendIcebergStorage::read(&store, file).await.unwrap();
    // The scan runs on into t1's renewal margin; at its expiry GCS stops
    // accepting it.
    tokio::time::sleep(std::time::Duration::from_millis(2_300)).await;
    SendIcebergStorage::read(&store, file).await.unwrap();
    t1_expired.store(true, Ordering::SeqCst);
    SendIcebergStorage::read(&store, file).await.unwrap();

    let tokens: Vec<_> = gcs
        .received_requests()
        .await
        .unwrap()
        .iter()
        .filter_map(bearer)
        .collect();
    assert_eq!(tokens, vec!["Bearer t1", "Bearer t2", "Bearer t2"]);
    assert_eq!(
        refusals.load(Ordering::SeqCst),
        0,
        "renewed before expiry, not after a refusal"
    );
}

#[tokio::test]
async fn a_vended_token_gcs_refuses_is_renewed_from_the_catalog() {
    #[derive(Debug, Default)]
    struct Revend(AtomicUsize);
    #[async_trait]
    impl VendedCredentialSource for Revend {
        async fn revend(&self) -> Result<VendedCredentials> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(gcs_token("t2", chrono::Duration::hours(1)))
        }
    }
    let gcs = MockServer::start().await;
    let t1_expired = Arc::new(AtomicBool::new(true));
    Mock::given(method("GET"))
        .and(object_at("/lake/f"))
        .respond_with(ExpiringGcs {
            t1_expired,
            refusals: Arc::default(),
        })
        .mount(&gcs)
        .await;
    let revend = Arc::new(Revend::default());
    // t1 looks valid for an hour, but GCS has revoked it.
    let renewing = Arc::new(RenewingVendedCredentials::new(
        gcs_token("t1", chrono::Duration::hours(1)),
        Some(Arc::clone(&revend) as Arc<dyn VendedCredentialSource>),
    ));
    let store = GcsIcebergStorage::at_endpoint(TokenSource::Vended(renewing), None, &gcs.uri());
    assert_eq!(
        &SendIcebergStorage::read(&store, "gs://lake/f")
            .await
            .unwrap()[..],
        BODY
    );
    assert_eq!(revend.0.load(Ordering::SeqCst), 1);
}

#[test]
fn debug_shows_the_principal_not_the_token() {
    let renewing = Arc::new(RenewingVendedCredentials::new(
        gcs_token("ya29.secret", chrono::Duration::hours(1)),
        None,
    ));
    let store = GcsIcebergStorage::at_endpoint(TokenSource::Vended(renewing), None, "http://x");
    assert!(!format!("{store:?}").contains("ya29.secret"));
}
