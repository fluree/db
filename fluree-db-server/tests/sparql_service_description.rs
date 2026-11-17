//! SPARQL Service Description: a `GET` on a query endpoint with no query
//! returns an RDF description of the service in the negotiated graph format.

use axum::body::Body;
use fluree_db_server::{routes::build_router, AppState, ServerConfig, TelemetryConfig};
use http::{Request, StatusCode};
use http_body_util::BodyExt;
use std::sync::Arc;
use tempfile::TempDir;
use tower::ServiceExt;

const SD: &str = "http://www.w3.org/ns/sparql-service-description#";
const LEDGER: &str = "sd:main";

async fn app(auth_required: bool) -> (TempDir, axum::Router) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mut cfg = ServerConfig {
        cors_enabled: false,
        indexing_enabled: false,
        storage_path: Some(tmp.path().to_path_buf()),
        ..Default::default()
    };
    if auth_required {
        cfg.data_auth_mode = fluree_db_server::config::DataAuthMode::Required;
        cfg.data_auth_insecure_accept_any_issuer = true;
    }
    let telemetry = TelemetryConfig::with_server_config(&cfg);
    let state = Arc::new(AppState::new(cfg, telemetry).await.expect("AppState::new"));
    let app = build_router(state);
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/create")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({ "ledger": LEDGER }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    (tmp, app)
}

async fn get(
    app: &axum::Router,
    uri: &str,
    headers: &[(&str, &str)],
) -> (StatusCode, Option<String>, String) {
    let mut req = Request::builder().method("GET").uri(uri);
    for (name, value) in headers {
        req = req.header(*name, *value);
    }
    let resp = app
        .clone()
        .oneshot(req.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    let ct = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    (status, ct, String::from_utf8_lossy(&bytes).into_owned())
}

/// `(predicate, object)` of every N-Triples line about the service node.
fn service_triples(ntriples: &str) -> Vec<(String, String)> {
    ntriples
        .lines()
        .filter_map(|line| {
            let mut parts = line.trim().trim_end_matches('.').trim().splitn(3, ' ');
            let (_s, p, o) = (parts.next()?, parts.next()?, parts.next()?);
            Some((
                p.trim_matches(['<', '>']).to_string(),
                o.trim().trim_matches(['<', '>']).to_string(),
            ))
        })
        .collect()
}

fn has(triples: &[(String, String)], p: &str, o: &str) -> bool {
    triples.iter().any(|(tp, to)| tp == p && to == o)
}

#[tokio::test]
async fn get_without_a_query_describes_the_ledger_endpoint() {
    let (_tmp, app) = app(false).await;
    let path = format!("/v1/fluree/query/{LEDGER}");
    let (status, ct, body) = get(
        &app,
        &path,
        &[
            ("host", "sparql.example.test"),
            ("accept", "application/n-triples"),
        ],
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(ct.unwrap().starts_with("application/n-triples"));

    let triples = service_triples(&body);
    let sd = |local: &str| format!("{SD}{local}");
    assert!(has(
        &triples,
        "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
        &sd("Service")
    ));
    assert!(
        has(
            &triples,
            &sd("endpoint"),
            &format!("http://sparql.example.test{path}")
        ),
        "{body}"
    );
    assert!(has(&triples, &sd("supportedLanguage"), &sd("SPARQLQuery")));
    assert!(has(
        &triples,
        &sd("supportedLanguage"),
        &sd("SPARQL11Query")
    ));
    for version in ["version-1.2", "version-1.1"] {
        assert!(
            has(
                &triples,
                &sd("supportedVersion"),
                &format!("http://www.w3.org/ns/sparql#{version}")
            ),
            "{version}: {body}"
        );
    }
    assert!(has(
        &triples,
        &sd("resultFormat"),
        "http://www.w3.org/ns/formats/Turtle"
    ));
}

#[tokio::test]
async fn service_description_is_negotiated_in_every_graph_format() {
    let (_tmp, app) = app(false).await;
    let endpoint = "https://proxy.example.test/v1/fluree/query";
    let forwarded = [
        ("host", "internal:8090"),
        ("x-forwarded-proto", "https"),
        ("x-forwarded-host", "proxy.example.test"),
    ];
    for (accept, media) in [
        ("text/turtle", "text/turtle"),
        ("application/ld+json", "application/ld+json"),
        ("application/rdf+xml", "application/rdf+xml"),
        ("application/n-triples", "application/n-triples"),
    ] {
        let mut headers = forwarded.to_vec();
        headers.push(("accept", accept));
        let (status, ct, body) = get(&app, "/v1/fluree/query", &headers).await;
        assert_eq!(status, StatusCode::OK, "{accept}: {body}");
        assert!(ct.unwrap().starts_with(media), "{accept}");
        assert!(body.contains(endpoint), "{accept}: {body}");
        if accept == "text/turtle" {
            fluree_graph_turtle::parse_to_json(&body)
                .unwrap_or_else(|e| panic!("Turtle must parse: {e}\n{body}"));
        }
    }

    let (status, _, body) = get(&app, "/v1/fluree/query", &[("accept", "text/csv")]).await;
    assert_eq!(status, StatusCode::NOT_ACCEPTABLE, "{body}");
}

/// The description sits behind the endpoint's own authentication, and a GET
/// that carries a query still runs it.
#[tokio::test]
async fn service_description_follows_the_endpoint_auth() {
    let (_tmp, locked) = app(true).await;
    let path = format!("/v1/fluree/query/{LEDGER}");
    let (status, _, body) = get(&locked, &path, &[("accept", "text/turtle")]).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "{body}");

    let (_tmp2, open_app) = app(false).await;
    let query = urlencoding::encode("SELECT ?s WHERE { ?s ?p ?o }");
    let (status, _, body) = get(
        &open_app,
        &format!("{path}?query={query}"),
        &[("accept", "application/sparql-results+json")],
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let results: serde_json::Value = serde_json::from_str(&body).expect("SPARQL JSON results");
    assert_eq!(results["head"]["vars"], serde_json::json!(["s"]), "{body}");
}
